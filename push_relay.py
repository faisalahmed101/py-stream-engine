#!/usr/bin/env python3
"""
push_relay.py

Local RTMP source (jemon Node-Media-Server -- rtmp://localhost:1935/live/stream_01)
theke pull kore, ekta shathe **dynamic list** of destination (YouTube, Facebook,
ba onno kono RTMP endpoint) e push kore -- ffmpeg-er `tee` muxer diye, ekta
single `-c copy` (no re-encode) process e.

"Dynamic" mane: destination gulo code-e hardcode na, ekta env var (JSON) theke
asche. Notun platform add/remove korte shudhu .env file edit korle hobe --
script/NMS kichu restart lagbe na (script nijei .env re-read kore proti
restart cycle-e, tai next crash-restart-e notun list effective hobe; live
running obosthay instant change chao hole "--reload" signal support-o ache,
niche dekho).

Usage:
    python3 push_relay.py --log-file push_relay.log

.env e lagbe (RTMP_URL already stream_worker_stdin.py e ache -- shei same
variable ekhane source hisebe reuse hoy, alada kono variable lagbe na):
    RTMP_URL=rtmp://localhost:1935/live/stream_01
    PUSH_DESTINATIONS=[{"name":"youtube","url":"rtmp://a.rtmp.youtube.com/live2/YOUR_KEY"},{"name":"facebook","url":"rtmps://live-api-s.facebook.com:443/rtmp/YOUR_KEY"}]

Optional (stall watchdog -- detects a "silently frozen" ffmpeg, i.e. the
process is still alive but has stopped actually pushing any bytes, which a
plain "did the process exit?" check can never catch):
    STALL_TIMEOUT_SECONDS=60   # kotokkhon kono forward progress na dekha gele
                               # "stalled" dhora hobe (default 60s -- deliberately
                               # generous, jate network jitter/video-transition
                               # e vul kore restart na hoy)
    STALL_WATCHDOG_ENABLED=true  # false dile watchdog puropuri off thakbe
"""

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from logging_setup import setup_logging, set_stream_id

logger = setup_logging("push_relay")  # log_file/stream_id main() theke set hobe

RESTART_DELAY = 5          # seconds -- base delay before the first retry after a crash
RESTART_BACKOFF_MAX = 30   # seconds -- delay never grows past this, so recovery stays fast
STABLE_RUN_SECONDS = 20    # if ffmpeg ran at least this long, treat the next crash as fresh (reset backoff)
STDERR_TAIL_LINES = 20

# --- Stall watchdog ---
# A crashed ffmpeg (non-zero exit) is already handled above by the normal
# restart loop. But ffmpeg can also get into a state where the *process is
# still alive* yet has stopped actually pushing any bytes (e.g. a wedged
# network socket that never errors out) -- "did the process exit?" can
# never catch that. The watchdog instead tracks ffmpeg's own `-progress`
# output (real byte throughput), and only acts if there has been *zero*
# forward progress for a long, deliberately generous window -- this keeps
# normal network jitter or brief hiccups from ever triggering a false
# restart of a perfectly healthy stream.
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
    SIGHUP (Linux/WSL) e destination list live re-read kore, running ffmpeg
    ke restart kore notun list diye -- eta chara normally shudhu next crash-e
    notun list effective hoy. Windows-e SIGHUP nai, tai Windows-e eta kaje
    lagbe na -- Windows-e change korte hole script restart korte hobe.
    """
    global _reload_requested
    logger.info("Received reload signal, destination list will be refreshed...")
    _reload_requested = True


def _strip_inline_comment(value: str) -> str:
    """
    Value quoted thakle (shuru ' ba " diye), quote-er por ja ache
    (trailing " # comment" shoho) ignore kore shudhu quoted part rakhe.
    Quoted na thakle, first unquoted ' #' (space+hash) ba tab+hash theke
    shuru kore baki shob truncate kore -- e.g.
    `RESTART_BACKOFF_BASE=2   # comment` theke shudhu `2` ber hoy.
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


def load_destinations(env_var: str) -> list[dict]:
    raw = os.environ.get(env_var)
    if not raw:
        raise ValueError(f"Environment variable '{env_var}' not found or empty.")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"'{env_var}' value is not valid JSON: {e}")

    if not isinstance(data, list) or not data:
        raise ValueError(f"'{env_var}' must be a non-empty JSON list.")

    destinations = []
    for i, item in enumerate(data):
        if isinstance(item, str):
            destinations.append({"name": f"dest_{i + 1}", "url": item})
        elif isinstance(item, dict):
            url = item.get("url")
            if not url:
                raise ValueError(f"Destination item {i} is missing the 'url' key: {item}")
            name = item.get("name") or f"dest_{i + 1}"
            destinations.append({"name": name, "url": url})
        else:
            raise ValueError(f"Destination item {i} has an invalid format: {item}")

    return destinations


def build_tee_output(destinations: list[dict]) -> str:
    """
    ffmpeg -f tee er jonno "[f=flv]url1|[f=flv]url2|..." format banay.

    IMPORTANT: tee muxer-er default "onfail" policy hocche "abort" --
    mane ekta destination (e.g. Facebook) fail/disconnect korle, ffmpeg
    default e PURO process abort kore fele, baki shob destination
    (e.g. YouTube) o shathe shathe bondho hoye jay. Eta ekta multi-destination
    relay-er jonno khub kharap behavior (ekta platform-er problem-e shob
    platform-e push bondho hoye jaoa uchit na).

    Tai proti destination e explicitly `onfail=ignore` set kora hocche,
    jate ekta destination fail korleo (connection drop, timeout, etc)
    baki destination gulo te push cholte thake unaffected vabe.
    """
    parts = [f"[f=flv:onfail=ignore]{d['url']}" for d in destinations]
    return "|".join(parts)


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
    Reads ffmpeg's `-progress pipe:1` output -- structured key=value lines
    (frame=, total_size=, out_time_ms=, speed=, progress=continue/end),
    completely separate from the human-readable warnings on stderr.

    Whenever `total_size` (cumulative bytes written so far) actually
    increases, proc._last_progress_at is refreshed. This is the one
    genuinely reliable "is real data still moving?" signal available --
    ffmpeg can be alive and non-erroring while still being stuck, but it
    cannot fake growing output size while stuck.
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


def build_ffmpeg_process(source_url: str, destinations: list[dict]) -> subprocess.Popen:
    tee_output = build_tee_output(destinations)
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
        "-map", "0",                      # tee muxer explicit map chara input stream auto-detect kore na
        "-c", "copy",
        "-f", "tee",
        tee_output,
    ]
    logger.info("ffmpeg command: %s", " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,   # -progress output (not video data -- video goes to the tee URLs)
        stderr=subprocess.PIPE,
    )
    proc._stderr_tail = []
    proc._started_at = time.time()  # used by run_relay() to decide backoff vs. reset
    proc._last_progress_bytes = -1
    proc._last_progress_at = time.time()  # grace period starts at spawn, not at the first progress line
    t_err = threading.Thread(target=drain_stderr, args=(proc, proc._stderr_tail), daemon=True)
    t_err.start()
    t_prog = threading.Thread(target=read_progress, args=(proc,), daemon=True)
    t_prog.start()
    return proc


def _build_ffmpeg_or_wait(source_url: str, destinations: list[dict]) -> subprocess.Popen:
    """
    build_ffmpeg_process() wrapper that specifically catches a missing
    ffmpeg binary (subprocess.Popen raises FileNotFoundError when the
    "ffmpeg" executable isn't installed / not on PATH).

    Without this, that FileNotFoundError is not caught anywhere else in
    this file -- it would propagate out of run_relay() and crash the
    entire Python process (not just "ffmpeg died"). main.py's supervisor
    would then keep respawning a brand-new Python process over and over
    with a full traceback each time -- a noisy, heavier "endless crash
    loop" than simply waiting for ffmpeg to become available.

    Instead, this waits/retries in place with a clear, low-noise log
    message, at a fixed RESTART_BACKOFF_MAX interval (no point hammering
    faster -- a missing binary won't fix itself in a second).

    Returns None only if shutdown was requested while waiting/retrying.
    """
    while not _shutdown_requested:
        try:
            return build_ffmpeg_process(source_url, destinations)
        except FileNotFoundError:
            logger.error(
                "ffmpeg binary not found (not installed, or not on PATH). "
                "Install it (e.g. 'apt install ffmpeg' / 'brew install ffmpeg') -- "
                "retrying in %.0fs...",
                RESTART_BACKOFF_MAX,
            )
            waited = 0.0
            while waited < RESTART_BACKOFF_MAX and not _shutdown_requested:
                time.sleep(0.5)
                waited += 0.5
    return None


def run_relay(env_file: str, source_env: str, dest_env: str) -> None:
    global _reload_requested

    load_env_file(env_file)
    set_stream_id("push_relay", os.environ.get("STREAM_ID", "-"))
    source_url = os.environ.get(source_env)
    if not source_url:
        raise ValueError(f"'{source_env}' is not set (check the .env file).")

    destinations = load_destinations(dest_env)
    logger.info(
        "Starting relay: source=%s -> %d destination(s): %s",
        source_url, len(destinations), ", ".join(d["name"] for d in destinations),
    )

    proc = _build_ffmpeg_or_wait(source_url, destinations)
    if proc is None:
        logger.info("Shutdown requested before ffmpeg could start.")
        return
    logger.info("ffmpeg (pid=%s) push started.", proc.pid)

    consecutive_crashes = 0  # tracks rapid repeat crashes to compute backoff; never stops retrying
    last_stall_check = time.time()

    if STALL_WATCHDOG_ENABLED:
        logger.info(
            "Stall watchdog enabled: will force a restart if ffmpeg reports zero "
            "forward progress for %.0fs straight (checked every %ds).",
            STALL_TIMEOUT_SECONDS, STALL_CHECK_INTERVAL,
        )

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

    try:
        while not _shutdown_requested:
            ret = proc.poll()

            if _reload_requested:
                _reload_requested = False
                logger.info("Reload: re-reading .env and restarting ffmpeg with the new destination list...")
                load_env_file(env_file)
                set_stream_id("push_relay", os.environ.get("STREAM_ID", "-"))
                try:
                    destinations = load_destinations(dest_env)
                except ValueError as e:
                    logger.error("New destination list is invalid, continuing with the previous list: %s", e)
                else:
                    _terminate(proc)
                    new_proc = _build_ffmpeg_or_wait(source_url, destinations)
                    if new_proc is None:
                        logger.info("Shutdown requested while restarting ffmpeg for reload.")
                        break
                    proc = new_proc
                    consecutive_crashes = 0  # manual reload isn't a crash, reset backoff
                    logger.info(
                        "ffmpeg (pid=%s) restarted with %d destination(s): %s",
                        proc.pid, len(destinations), ", ".join(d["name"] for d in destinations),
                    )
                continue

            if ret is not None:
                tail = getattr(proc, "_stderr_tail", None)
                if tail:
                    logger.warning("ffmpeg exited (code=%s), last error:\n%s", ret, "\n".join(tail))
                else:
                    logger.warning("ffmpeg exited (code=%s)", ret)

                # Stream never permanently stops: we always retry, just with a
                # progressively longer (capped) delay if ffmpeg keeps crashing
                # right away -- avoids hammering the source/destinations in a
                # tight loop, while a stable long-running process resets the
                # delay back to the base value.
                ran_for = time.time() - getattr(proc, "_started_at", 0)
                if ran_for < STABLE_RUN_SECONDS:
                    consecutive_crashes += 1
                else:
                    consecutive_crashes = 0
                delay = min(RESTART_DELAY * (2 ** consecutive_crashes), RESTART_BACKOFF_MAX)
                logger.warning(
                    "Restarting ffmpeg in %.1fs (consecutive quick failures: %d)...",
                    delay, consecutive_crashes,
                )
                time.sleep(delay)

                # .env is freshly re-read on every crash-restart, so an edited
                # destination list takes effect immediately at this point.
                #
                # This re-read is wrapped in try/except (unlike a plain
                # `destinations = load_destinations(dest_env)`) because a
                # crash-restart happens automatically, unattended, possibly
                # in the middle of the night a year from now -- if
                # PUSH_DESTINATIONS happens to be transiently malformed at
                # exactly this moment (e.g. a ConfigMap update caught
                # mid-write, a stray edit), an uncaught ValueError here would
                # propagate out of run_relay() and exit this whole Python
                # process. main.py's supervisor would then just keep
                # restarting a RELAY that immediately dies again on the same
                # bad config -- pushing to YouTube/Facebook would silently
                # stop (worker -> NMS keeps working fine, so nothing else
                # would look "down") until someone manually fixes the config.
                # Falling back to the last-known-good `destinations` list
                # avoids that failure mode entirely, consistent with how the
                # SIGHUP reload path above already behaves.
                load_env_file(env_file)
                set_stream_id("push_relay", os.environ.get("STREAM_ID", "-"))
                try:
                    destinations = load_destinations(dest_env)
                except ValueError as e:
                    logger.error(
                        "PUSH_DESTINATIONS is invalid after reload, keeping the previous "
                        "destination list and retrying with it: %s", e,
                    )
                proc = _build_ffmpeg_or_wait(source_url, destinations)
                if proc is None:
                    break  # shutdown requested while waiting for ffmpeg
                last_stall_check = time.time()
                logger.info("ffmpeg restarted (pid=%s)", proc.pid)
                continue

            # --- Stall watchdog: process is alive (ret is None) but may be
            # silently frozen. Checked only periodically, not every loop tick,
            # and only acts after STALL_TIMEOUT_SECONDS of *zero* progress --
            # both deliberately generous so a healthy stream is never touched.
            if STALL_WATCHDOG_ENABLED and time.time() - last_stall_check >= STALL_CHECK_INTERVAL:
                last_stall_check = time.time()
                stalled_for = time.time() - getattr(proc, "_last_progress_at", time.time())
                if stalled_for >= STALL_TIMEOUT_SECONDS:
                    logger.error(
                        "No forward progress from ffmpeg for %.0fs (last total_size=%s bytes) -- "
                        "treating this as a silent stall (process alive but stuck) and forcing a restart...",
                        stalled_for, getattr(proc, "_last_progress_bytes", "?"),
                    )
                    _terminate(proc)

                    ran_for = time.time() - getattr(proc, "_started_at", 0)
                    if ran_for < STABLE_RUN_SECONDS:
                        consecutive_crashes += 1
                    else:
                        consecutive_crashes = 0
                    delay = min(RESTART_DELAY * (2 ** consecutive_crashes), RESTART_BACKOFF_MAX)
                    logger.warning("Restarting ffmpeg in %.1fs after stall...", delay)
                    time.sleep(delay)

                    # Shei same reasoning hisebe (upore crash-restart branch e
                    # dekho) -- transient invalid PUSH_DESTINATIONS e process
                    # crash na kore, purono list diyei retry kora hocche.
                    load_env_file(env_file)
                    set_stream_id("push_relay", os.environ.get("STREAM_ID", "-"))
                    try:
                        destinations = load_destinations(dest_env)
                    except ValueError as e:
                        logger.error(
                            "PUSH_DESTINATIONS is invalid after reload, keeping the previous "
                            "destination list and retrying with it: %s", e,
                        )
                    proc = _build_ffmpeg_or_wait(source_url, destinations)
                    if proc is None:
                        break
                    last_stall_check = time.time()
                    logger.info("ffmpeg restarted after stall (pid=%s)", proc.pid)
                    continue

            time.sleep(1)
    finally:
        logger.info("Shutting down, cleaning up ffmpeg...")
        if proc is not None:
            _terminate(proc)
        logger.info("Relay stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="Dynamic multi-destination RTMP push relay (local source -> YouTube/Facebook/etc via ffmpeg tee)"
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env file (default: .env)")
    parser.add_argument(
        "--source-env", default="RTMP_URL",
        help="Env var naam jekhane local source RTMP URL thakbe (default: RTMP_URL -- stream_worker_stdin.py "
             "jekhane push kore, shei same URL eikhane source hisebe use hoy)",
    )
    parser.add_argument(
        "--dest-env", default="PUSH_DESTINATIONS",
        help="Env var naam jekhane JSON destination list thakbe (default: PUSH_DESTINATIONS)",
    )
    parser.add_argument("--log-file", default=None, help="Log file path (na dile stdout e log hobe)")
    args = parser.parse_args()

    setup_logging("push_relay", log_file=args.log_file)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, handle_reload)  # Windows e available na, tai guarded

    try:
        run_relay(args.env_file, args.source_env, args.dest_env)
    except ValueError as e:
        logger.error("Config error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()