#!/usr/bin/env python3
"""
push_relay.py

Local RTMP source (jemon Node-Media-Server -- rtmp://localhost:1935/live/stream_01)
theke pull kore, **proti destination-er jonno completely alada ffmpeg process**
diye push kore YouTube/Facebook/etc e -- ekta shared `tee` process er poriborte
(ager version tee muxer diye ekta single process e shob destination pathato).

Ekhon destination list o (PUSH_DESTINATIONS env var er poriborte) stream_worker_stdin.py-er
playlist-load-er moto Supabase theke dynamically load hoy: env theke shudhu STREAM_ID
(streams.id) neওয়া hoy, tarpor:

    stream_destinations (stream_id = STREAM_ID diye) -> [{id, platform, rtmp_url,
    stream_key, ...}, ...]

query kore, proti row-er `rtmp_url` + `stream_key` jog kore final push URL banano hoy.

Keno per-destination alada process (tee-r poriborte)?
    - tee muxer-er `onfail=ignore` diyeo, shob destination ekই ffmpeg process-er
      moddhe thake -- tai ekta destination-er URL/auth problem (e.g. bad stream key)
      hole shei ekই process-er stderr/exit code shob destination-er jonno mix hoye
      jay, r kono destination ke individually restart kora jay na (restart mane
      shob-i restart, live thaka baki destination gulo-o shathe interrupt hoy).
    - Alada process hole: proti destination independently crash/restart/backoff/
      stall-detect hoy, r Supabase-e proti row-er `status`/`error_message`/
      `started_at`/`ended_at` shothik vabe alada-alada update kora jay (ekta
      platform down thakleo baki gulo unaffected thake -- exactly jei problem-er
      jonno ager version-e explicit `onfail=ignore` set kora hoyeche, eta shei
      shomossha-r arো shoktoshali shomadhan, karon pura process-i alada).

Destination list "start howar shomoy" (r reload signal ashle) Supabase theke
(re-)load hoy -- stream_worker_stdin.py-er playlist load-er moto DB-e change
korle (notun destination add/remove, stream_key update) SIGHUP pathale live
effective hoy, na hole next crash-restart-e (protyek destination thread nijer
Supabase row abar poRe na -- shudhu top-level reload/restart e notun list ashe).

Usage:
    python3 push_relay.py --log-file push_relay.log

.env e lagbe:
    RTMP_URL=rtmp://localhost:1935/live/   # BASE URL (trailing '/', kono stream key/naam chara) --
                                            # actual pull source = base + '/' + STREAM_ID, exactly
                                            # jekhane stream_worker_stdin.py push kore.
    STREAM_ID=<uuid>                       # streams.id -- destination list-er jonno, AR source
                                            # URL-er path/key hisebe-o (STREAM_NAME ar lagbe na)
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_SERVICE_ROLE_KEY=<service role key>

Supabase table (already created, reference):
    create table public.stream_destinations (
        id uuid primary key default gen_random_uuid(),
        stream_id uuid not null references streams(id) on delete cascade,
        platform text not null,
        rtmp_url text not null,
        stream_key text not null,
        status text not null default 'connecting',
        error_message text,
        started_at timestamptz,
        ended_at timestamptz,
        created_at timestamptz not null default now()
    );

Optional (stall watchdog -- proti destination-er jonno ALADA vabe track hoy,
karon ekta destination stuck thakleo baki gulo thik thakte pare):
    STALL_TIMEOUT_SECONDS=60
    STALL_WATCHDOG_ENABLED=true
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from logging_setup import setup_logging, set_stream_id
from supabase_client import (
    SUPABASE_RETRY_INTERVAL_SECONDS,
    SupabaseFetchError,
    is_configured as supabase_is_configured,
    supabase_get,
    supabase_patch,
    wait_for_supabase,
)

logger = setup_logging("push_relay")  # log_file/stream_id main() theke set hobe

RESTART_DELAY = 5          # seconds -- base delay before the first retry after a crash
RESTART_BACKOFF_MAX = 30   # seconds -- delay never grows past this, so recovery stays fast
STABLE_RUN_SECONDS = 20    # if ffmpeg ran at least this long, treat the next crash as fresh (reset backoff)
STDERR_TAIL_LINES = 20

# --- Stall watchdog (per-destination) ---
STALL_CHECK_INTERVAL = 5   # seconds -- how often the watchdog re-checks progress
STALL_TIMEOUT_SECONDS = float(os.environ.get("STALL_TIMEOUT_SECONDS", "60"))
STALL_WATCHDOG_ENABLED = os.environ.get("STALL_WATCHDOG_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")

_shutdown_requested = False
_reload_requested = False


def handle_signal(signum, frame):
    global _shutdown_requested
    logger.info("Received shutdown signal (%s), stopping...", signum)
    _shutdown_requested = True


def handle_reload(signum, frame):
    """
    SIGHUP (Linux/WSL) e destination list Supabase theke live re-fetch kore --
    notun destination add hoye thakle notun ffmpeg process start hobe, remove
    hoye thakle running process ta stop hobe, r existing/unchanged destination
    gulo (same id) uninterrupted thakbe (restart kora hobe na) -- eta chara
    normally shudhu next full-restart e notun list effective hoy.
    Windows-e SIGHUP nai, tai Windows-e eta kaje lagbe na.
    """
    global _reload_requested
    logger.info("Received reload signal, destination list will be refreshed from Supabase...")
    _reload_requested = True


def _strip_inline_comment(value: str) -> str:
    """
    Value quoted thakle (shuru ' ba " diye), quote-er por ja ache
    (trailing " # comment" shoho) ignore kore shudhu quoted part rakhe.
    Quoted na thakle, first unquoted ' #' (space+hash) ba tab+hash theke
    shuru kore baki shob truncate kore.
    """
    if not value:
        return value
    if value[0] in ("'", '"'):
        quote = value[0]
        end = value.find(quote, 1)
        if end != -1:
            return value[: end + 1]
        return value
    for marker in (" #", "\t#"):
        idx = value.find(marker)
        if idx != -1:
            value = value[:idx]
    return value.rstrip()


def load_env_file(env_file_path: str) -> None:
    path = Path(env_file_path)
    if not path.exists():
        logger.info(".env file not found (%s), using shell environment only.", env_file_path)
        return

    with path.open("r", encoding="utf-8") as f:
        for line_num, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                logger.warning(".env file line %d has no '=', skipping: %s", line_num, line)
                continue

            key, _, value = line.partition("=")
            key = key.strip()
            value = _strip_inline_comment(value.strip())

            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]

            os.environ[key] = value  # this script must re-read on reload, so overriding is intentional

    logger.info("(Re)loaded environment variables from: %s", env_file_path)


# ============================================================
# Supabase: destination list load + status reporting
# (SupabaseFetchError, supabase_get, supabase_patch, wait_for_supabase are
#  imported from the shared supabase_client.py module -- see top of file)
# ============================================================

def update_destination_status(dest_id: str, **fields) -> None:
    """
    stream_destinations row-er status/error_message/started_at/ended_at
    update kore. Only pass the fields you want changed, e.g.:
        update_destination_status(dest_id, status="live", started_at=_now_iso())
        update_destination_status(dest_id, status="error", error_message="...")
    """
    supabase_patch("stream_destinations", {"id": f"eq.{dest_id}"}, fields, logger=logger)


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _build_destination_url(rtmp_url: str, stream_key: str) -> str:
    rtmp_url = (rtmp_url or "").strip()
    stream_key = (stream_key or "").strip()
    if not stream_key:
        return rtmp_url
    return f"{rtmp_url.rstrip('/')}/{stream_key}"


def fetch_destinations_from_supabase(stream_id: str) -> list[dict]:
    """
    stream_destinations table theke, stream_id diye shokol row poRe. Proti
    row theke {"id", "platform", "url"} banano hoy (url = rtmp_url + stream_key).

    Raises:
        ValueError          -- config/data problem (missing env vars, no rows).
        SupabaseFetchError  -- transient network/DB problem.
    """
    if not supabase_is_configured():
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must both be set in the environment.")

    rows = supabase_get(
        "stream_destinations",
        {
            "stream_id": f"eq.{stream_id}",
            "select": "id,platform,rtmp_url,stream_key",
        },
    )
    if not rows:
        raise ValueError(f"No 'stream_destinations' rows found for stream_id={stream_id}.")

    destinations = []
    for i, row in enumerate(rows):
        rtmp_url = row.get("rtmp_url")
        stream_key = row.get("stream_key")
        if not rtmp_url or not stream_key:
            logger.warning(
                "stream_destinations row %d (id=%s) missing rtmp_url/stream_key, skipping.",
                i, row.get("id"),
            )
            continue
        destinations.append({
            "id": row["id"],
            "name": row.get("platform") or f"dest_{i + 1}",
            "url": _build_destination_url(rtmp_url, stream_key),
        })

    if not destinations:
        raise ValueError(f"stream_id={stream_id}: every stream_destinations row was missing rtmp_url/stream_key.")

    return destinations


def _load_destinations_or_wait(stream_id: str) -> "list[dict] | None":
    """
    fetch_destinations_from_supabase() wrapper -- infinite retry, fixed
    backoff, mirrors stream_worker_stdin.py's _load_playlist_or_wait()
    (both now delegate to the shared wait_for_supabase() helper).
    Returns None only if shutdown was requested while waiting.
    """
    return wait_for_supabase(
        lambda: fetch_destinations_from_supabase(stream_id),
        description=f"Could not load destinations from Supabase for stream_id={stream_id}",
        shutdown_check=lambda: _shutdown_requested,
        logger=logger,
        retry_interval=SUPABASE_RETRY_INTERVAL_SECONDS,
    )


# ============================================================
# Per-destination ffmpeg process (NO tee -- one destination, one process)
# ============================================================

def drain_stderr(proc: subprocess.Popen, tail: list) -> None:
    try:
        for raw_line in iter(proc.stderr.readline, b""):
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="ignore").rstrip()
            if line:
                logger.debug("ffmpeg stderr: %s", line)
                tail.append(line)
                del tail[:-STDERR_TAIL_LINES]
    except Exception:
        pass


def read_progress(proc: subprocess.Popen) -> None:
    """
    Reads ffmpeg's `-progress pipe:1` output. Whenever `total_size`
    (cumulative bytes written so far) actually increases,
    proc._last_progress_at is refreshed -- the stall watchdog's signal.
    """
    try:
        for raw_line in iter(proc.stdout.readline, b""):
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="ignore").strip()
            if not line or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key == "total_size":
                try:
                    size = int(value)
                except ValueError:
                    continue
                if size != getattr(proc, "_last_progress_bytes", -1):
                    proc._last_progress_bytes = size
                    proc._last_progress_at = time.time()
    except Exception:
        pass


def build_ffmpeg_process(source_url: str, dest_url: str) -> subprocess.Popen:
    """
    Ekta single destination-er jonno ffmpeg process -- tee NAI, shudhu
    ekta -c copy remux+push. Proti destination-er nijer alada process,
    tai ekta destination-er problem baki destination gulo-r process ke
    kono vabei touch kore na.
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-loglevel", "warning",
        "-nostats",
        "-progress", "pipe:1",            # structured progress info on stdout, for the stall watchdog
        "-analyzeduration", "10000000",   # 10s -- input stream detect korte beshi shomoy dey
        "-probesize", "10000000",         # 10MB -- beshi data dekhe stream detect kore
        "-rtmp_live", "live",             # live RTMP source (no seek), fresh connect e keyframe wait kore
        "-i", source_url,
        "-map", "0",
        "-c", "copy",
        "-f", "flv",
        dest_url,
    ]
    logger.info("ffmpeg command: %s", " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,   # -progress output only
        stderr=subprocess.PIPE,
    )
    proc._stderr_tail = []
    proc._started_at = time.time()
    proc._last_progress_bytes = -1
    proc._last_progress_at = time.time()  # grace period starts at spawn, not at the first progress line
    t_err = threading.Thread(target=drain_stderr, args=(proc, proc._stderr_tail), daemon=True)
    t_err.start()
    t_prog = threading.Thread(target=read_progress, args=(proc,), daemon=True)
    t_prog.start()
    return proc


def _build_ffmpeg_or_wait(source_url: str, dest_url: str, name: str, stop_flag: threading.Event) -> subprocess.Popen:
    """
    build_ffmpeg_process() wrapper that catches a missing ffmpeg binary
    (FileNotFoundError) so it can't crash this destination's whole thread --
    it just waits/retries in place at a fixed interval instead. Returns
    None only if shutdown/stop was requested while waiting.
    """
    while not _shutdown_requested and not stop_flag.is_set():
        try:
            return build_ffmpeg_process(source_url, dest_url)
        except FileNotFoundError:
            logger.error(
                "[%s] ffmpeg binary not found (not installed, or not on PATH). "
                "Install it (e.g. 'apt install ffmpeg' / 'brew install ffmpeg') -- "
                "retrying in %.0fs...",
                name, RESTART_BACKOFF_MAX,
            )
            waited = 0.0
            while waited < RESTART_BACKOFF_MAX and not _shutdown_requested and not stop_flag.is_set():
                time.sleep(0.5)
                waited += 0.5
    return None


def _terminate(p: subprocess.Popen) -> None:
    try:
        p.terminate()
        p.wait(timeout=5)
    except Exception:
        try:
            p.kill()
            p.wait(timeout=5)
        except Exception:
            pass
    for stream in (getattr(p, "stdout", None), getattr(p, "stderr", None)):
        try:
            if stream:
                stream.close()
        except Exception:
            pass


def run_destination(dest: dict, source_url: str, stop_flag: threading.Event) -> None:
    """
    Ekta single destination-er jonno independent supervise loop -- ei
    function ekta alada thread e run hoy, proti destination-er jonno
    alada instance. Nijer crash-restart backoff, nijer stall watchdog,
    r Supabase-e nijer status/error_message update -- baki destination
    gulor shathe kono shared state nai (source_url chhara), tai ekta
    destination-er kono shomossha baki destination gulo ke touch kore na.
    """
    name = dest["name"]
    dest_id = dest["id"]
    dest_url = dest["url"]

    update_destination_status(dest_id, status="connecting", error_message=None, started_at=_now_iso(), ended_at=None)

    proc = _build_ffmpeg_or_wait(source_url, dest_url, name, stop_flag)
    if proc is None:
        logger.info("[%s] Shutdown/stop requested before ffmpeg could start.", name)
        return
    logger.info("[%s] ffmpeg (pid=%s) push started -> %s", name, proc.pid, dest_url)
    update_destination_status(dest_id, status="live", error_message=None)

    consecutive_crashes = 0
    last_stall_check = time.time()

    if STALL_WATCHDOG_ENABLED:
        logger.info(
            "[%s] Stall watchdog enabled: will force a restart if ffmpeg reports zero "
            "forward progress for %.0fs straight (checked every %ds).",
            name, STALL_TIMEOUT_SECONDS, STALL_CHECK_INTERVAL,
        )

    def _restart(reason: str) -> "subprocess.Popen | None":
        nonlocal consecutive_crashes
        ran_for = time.time() - getattr(proc, "_started_at", 0)
        if ran_for < STABLE_RUN_SECONDS:
            consecutive_crashes += 1
        else:
            consecutive_crashes = 0
        delay = min(RESTART_DELAY * (2 ** consecutive_crashes), RESTART_BACKOFF_MAX)
        logger.warning(
            "[%s] Restarting ffmpeg in %.1fs (%s, consecutive quick failures: %d)...",
            name, delay, reason, consecutive_crashes,
        )
        update_destination_status(dest_id, status="connecting", error_message=reason)
        waited = 0.0
        while waited < delay and not _shutdown_requested and not stop_flag.is_set():
            time.sleep(0.5)
            waited += 0.5
        if _shutdown_requested or stop_flag.is_set():
            return None
        new_proc = _build_ffmpeg_or_wait(source_url, dest_url, name, stop_flag)
        if new_proc is not None:
            logger.info("[%s] ffmpeg restarted (pid=%s)", name, new_proc.pid)
            update_destination_status(dest_id, status="live", error_message=None)
        return new_proc

    try:
        while not _shutdown_requested and not stop_flag.is_set():
            ret = proc.poll()

            if ret is not None:
                tail = getattr(proc, "_stderr_tail", None)
                err_summary = "\n".join(tail) if tail else f"ffmpeg exited (code={ret})"
                if tail:
                    logger.warning("[%s] ffmpeg exited (code=%s), last error:\n%s", name, ret, err_summary)
                else:
                    logger.warning("[%s] ffmpeg exited (code=%s)", name, ret)

                proc = _restart(reason=f"exit_code={ret}: {err_summary}"[:500])
                if proc is None:
                    break
                last_stall_check = time.time()
                continue

            if STALL_WATCHDOG_ENABLED and time.time() - last_stall_check >= STALL_CHECK_INTERVAL:
                last_stall_check = time.time()
                stalled_for = time.time() - getattr(proc, "_last_progress_at", time.time())
                if stalled_for >= STALL_TIMEOUT_SECONDS:
                    logger.error(
                        "[%s] No forward progress from ffmpeg for %.0fs (last total_size=%s bytes) -- "
                        "treating this as a silent stall (process alive but stuck) and forcing a restart...",
                        name, stalled_for, getattr(proc, "_last_progress_bytes", "?"),
                    )
                    _terminate(proc)
                    proc = _restart(reason=f"stalled for {stalled_for:.0f}s (no forward progress)")
                    if proc is None:
                        break
                    last_stall_check = time.time()
                    continue

            time.sleep(1)
    finally:
        logger.info("[%s] Stopping, cleaning up ffmpeg...", name)
        if proc is not None:
            _terminate(proc)
        # Note: whether this destination stopped because of a full
        # shutdown or an individual reload-triggered stop, the resulting
        # row status is the same ("ended") -- there's no distinct status
        # for the two cases, so no conditional is needed here.
        update_destination_status(
            dest_id,
            status="ended",
            ended_at=_now_iso(),
        )
        logger.info("[%s] Destination relay stopped.", name)


# ============================================================
# Orchestration: one thread per destination, reload = re-fetch + diff
# ============================================================

class _DestinationHandle:
    __slots__ = ("dest", "stop_flag", "thread")

    def __init__(self, dest: dict, stop_flag: threading.Event, thread: threading.Thread):
        self.dest = dest
        self.stop_flag = stop_flag
        self.thread = thread


def _start_destination(dest: dict, source_url: str) -> _DestinationHandle:
    stop_flag = threading.Event()
    t = threading.Thread(target=run_destination, args=(dest, source_url, stop_flag), daemon=True)
    t.start()
    return _DestinationHandle(dest, stop_flag, t)


def _stop_destination(handle: _DestinationHandle, timeout: float = 15.0) -> None:
    handle.stop_flag.set()
    handle.thread.join(timeout=timeout)


def run_relay(env_file: str, source_env: str) -> None:
    global _reload_requested

    load_env_file(env_file)
    set_stream_id("push_relay", os.environ.get("STREAM_ID", "-"))
    source_base = os.environ.get(source_env)
    if not source_base:
        raise ValueError(f"'{source_env}' is not set (check the .env file).")

    stream_id = os.environ.get("STREAM_ID")
    if not stream_id:
        raise ValueError("STREAM_ID is not set (check the .env file) -- needed to load destinations from Supabase.")

    # RTMP_URL ekhon ekta BASE URL (e.g. "rtmp://localhost:1935/live/") --
    # STREAM_NAME ar lagbe na, karon STREAM_ID-i stream_worker_stdin.py-r
    # push target-er path/key hisebe use hocche. Tai eikhaneo (pull source)
    # shei same base + STREAM_ID diye actual local source URL banano hoy --
    # stream_worker_stdin.py jekhane push kore, exactly shei URL theke pull
    # hoy.
    source_url = f"{source_base.rstrip('/')}/{stream_id}"
    logger.info("Pull source: %s", source_url)

    destinations = _load_destinations_or_wait(stream_id)
    if destinations is None:
        logger.info("Shutdown requested before destinations could be loaded from Supabase.")
        return

    logger.info(
        "Starting relay: source=%s -> %d destination(s), each as its own ffmpeg process: %s",
        source_url, len(destinations), ", ".join(d["name"] for d in destinations),
    )

    # id -> _DestinationHandle
    handles: dict = {d["id"]: _start_destination(d, source_url) for d in destinations}

    try:
        while not _shutdown_requested:
            if _reload_requested:
                _reload_requested = False
                logger.info("Reload: re-reading .env and re-fetching destinations from Supabase...")
                load_env_file(env_file)
                set_stream_id("push_relay", os.environ.get("STREAM_ID", "-"))
                new_stream_id = os.environ.get("STREAM_ID", stream_id)

                try:
                    new_destinations = fetch_destinations_from_supabase(new_stream_id)
                except (ValueError, SupabaseFetchError) as e:
                    logger.error("Reload failed (destinations unchanged, keeping current ones running): %s", e)
                else:
                    new_by_id = {d["id"]: d for d in new_destinations}
                    old_ids = set(handles.keys())
                    new_ids = set(new_by_id.keys())

                    removed = old_ids - new_ids
                    added = new_ids - old_ids
                    kept = old_ids & new_ids

                    for dest_id in removed:
                        logger.info("Reload: destination removed (id=%s), stopping its ffmpeg process...", dest_id)
                        _stop_destination(handles.pop(dest_id))

                    # dest_id-er URL badle geche (e.g. stream_key updated) emon
                    # kotogula restart hocche eta ekhaneই count kora hocche --
                    # niche handles[dest_id] mutate hoye jaওয়ার AGE, karon mutate
                    # hoye gele old-vs-new URL comparison ar kaje lagbe na
                    # (handles[dest_id].dest already new_by_id[dest_id]-er shathe
                    # match korbe, tai "changed" detect e always miss hoto).
                    url_changed = 0
                    for dest_id in kept:
                        # url change hoye thakle (e.g. stream_key updated) restart lagbe,
                        # na hole running process ke uninterrupted rakha hocche.
                        if handles[dest_id].dest.get("url") != new_by_id[dest_id].get("url"):
                            logger.info(
                                "Reload: destination id=%s URL changed, restarting its ffmpeg process...", dest_id,
                            )
                            _stop_destination(handles.pop(dest_id))
                            handles[dest_id] = _start_destination(new_by_id[dest_id], source_url)
                            url_changed += 1

                    for dest_id in added:
                        logger.info(
                            "Reload: new destination detected (id=%s, platform=%s), starting ffmpeg process...",
                            dest_id, new_by_id[dest_id]["name"],
                        )
                        handles[dest_id] = _start_destination(new_by_id[dest_id], source_url)

                    logger.info(
                        "Reload complete: %d destination(s) running (%d added, %d removed, %d unchanged, %d restarted due to URL change).",
                        len(handles), len(added), len(removed), len(kept) - url_changed, url_changed,
                    )
                continue

            # Ekta destination thread nijer moddhe already restart/backoff kore
            # (crash-loop kore na), tai ekhane shudhu "thread mara geche kina"
            # (unexpected -- e.g. an unhandled exception inside run_destination)
            # check kori, jate emon truly-unexpected case-e-o silently stuck na
            # thaki -- restart kore diই.
            for dest_id, handle in list(handles.items()):
                if _shutdown_requested:
                    break
                if not handle.thread.is_alive():
                    logger.error(
                        "[%s] Destination thread died unexpectedly, restarting...", handle.dest["name"],
                    )
                    handles[dest_id] = _start_destination(handle.dest, source_url)

            time.sleep(0.5)
    finally:
        logger.info("Shutting down, stopping all destination relays...")
        for handle in handles.values():
            handle.stop_flag.set()
        deadline = time.time() + 15
        for handle in handles.values():
            remaining = max(0, deadline - time.time())
            handle.thread.join(timeout=remaining)
        logger.info("Relay stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="Per-destination RTMP push relay (local source -> YouTube/Facebook/etc, "
                     "one independent ffmpeg process per destination, destination list from Supabase)"
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env file (default: .env)")
    parser.add_argument(
        "--source-env", default="RTMP_URL",
        help="Env var naam jekhane local source RTMP URL thakbe (default: RTMP_URL -- stream_worker_stdin.py "
             "jekhane push kore, shei same URL eikhane source hisebe use hoy)",
    )
    parser.add_argument("--log-file", default=None, help="Log file path (na dile stdout e log hobe)")
    args = parser.parse_args()

    setup_logging("push_relay", log_file=args.log_file)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, handle_reload)  # Windows e available na, tai guarded

    try:
        run_relay(args.env_file, args.source_env)
    except ValueError as e:
        logger.error("Config error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()