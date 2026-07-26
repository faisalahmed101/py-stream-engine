#!/usr/bin/env python3
"""
stream_worker_stdin.py

Ekta persistent ffmpeg process chalu thake, jetar stdin e Python script
HLS video (.ts segment) er raw bytes por por push kore. Ffmpeg oi data
-c copy diye SRS e RTMP push kore, tai video-to-video transition e
kono reconnect/gap hoy na (continuous single RTMP connection).

Playlist local JSON file er poriborte, ekhon Supabase theke dynamically load
kora hoy: script start howar shomoy (ba worker restart howar shomoy) env
theke shudhu STREAM_ID (streams.id row) neওয়া hoy, tarpor Supabase theke:
    streams (id -> playlist_id)  ->  playlist_items (playlist_id diye, sort_order
    onujayi sorted)  ->  [{"title": video_title, "url": video_url}, ...]
build kore RAM e rakha hoy (kono disk file lagbe na, kono live-reload-o na --
DB shudhu "start/restart howar shomoy check kora hoy" er jonno, ei
requirement onujayi).

RTMP_URL ekhon ekta BASE URL (STREAM_NAME r lagbe na) -- ei script actual
push target nijei banay: base RTMP_URL + '/' + STREAM_ID. E.g.:
    RTMP_URL=rtmp://localhost:1935/live/   (trailing '/', kono stream key/naam chara)
    STREAM_ID=9cfd9e88-ccdf-46a9-a0c7-77c849cd5730
    -> actual push target: rtmp://localhost:1935/live/9cfd9e88-ccdf-46a9-a0c7-77c849cd5730

Usage (.env file diye):
    # .env file:
    #   RTMP_URL=rtmp://localhost:1935/live/
    #   STREAM_ID=<uuid -- streams.id row, jar playlist stream hobe -- ei-i push URL-er path/key-o>
    #   SUPABASE_URL=https://xxxx.supabase.co
    #   SUPABASE_SERVICE_ROLE_KEY=<service role key -- RLS bypass kore, tai
    #                              eta shudhu server-side/backend e rakho,
    #                              kokhono client/browser e na>

    python3 stream_worker_stdin.py --log-file stream_01.log

Usage (CLI diye override, .env chara o):
    python3 stream_worker_stdin.py \
        --rtmp-url rtmp://localhost:1935/live/ \
        --stream-id <uuid> \
        --log-file stream_01.log

.env file er path change korte chaile --env-file flag use koro.

Supabase unreachable/misconfigured hole (network down, playlist_id null,
playlist_items khali, etc): stream shuru-i hobe na, infinite retry with a
fixed backoff cholte thakbe (exactly ffmpeg-binary-not-found er moto
behavior) jotokkhon na Supabase ekta valid playlist dey ba shutdown signal
ashe. Kono local cached-copy fallback nei -- eta intentional (requirement
onujayi), jate purono/stale playlist diye kokhono bhul video push na hoy.

Optional (stall watchdog -- detects a "silently frozen" ffmpeg, i.e. the
process is still alive and accepting stdin writes, but has stopped actually
pushing any bytes to SRS -- something the existing stuck-pipe write-timeout
mostly, but not perfectly, covers):
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
import queue
import select
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urljoin

from logging_setup import setup_logging, set_stream_id
from supabase_client import (
    SUPABASE_RETRY_INTERVAL_SECONDS,
    SupabaseFetchError,
    is_configured as supabase_is_configured,
    supabase_get,
    wait_for_supabase,
)

logger = setup_logging("stream_worker")  # log_file/stream_id are set from main()

_IS_POSIX = os.name == "posix"
if _IS_POSIX:
    import fcntl



# ---------- Config ----------
SEGMENT_FETCH_RETRIES = 3
SEGMENT_RETRY_BACKOFF_BASE = 2      # seconds
M3U8_FETCH_RETRIES = 3
CHUNK_SIZE = 64 * 1024              # stream in 64KB chunks
FFMPEG_LOGLEVEL = "warning"
FFMPEG_RESTART_DELAY = 3            # seconds -- base delay before the first retry after a crash
FFMPEG_RESTART_BACKOFF_MAX = 30     # seconds -- delay never grows past this, so recovery stays fast
FFMPEG_STABLE_RUN_SECONDS = 20      # if ffmpeg ran at least this long, treat the next crash as fresh (reset backoff)
STDIN_WRITE_TIMEOUT = 10            # seconds before a blocked ffmpeg stdin write is considered "stuck"

# --- Stall watchdog ---
# The stdin write-timeout above (StuckPipeError) already catches most
# "ffmpeg stopped consuming stdin" cases, since output-side backpressure
# (SRS/network stalled) typically fills ffmpeg's internal buffers and
# blocks the next stdin write within seconds. This watchdog is a second,
# independent layer for the narrower edge case where ffmpeg keeps
# consuming stdin normally (no write-timeout) but has silently stopped
# actually pushing bytes out over RTMP. It tracks ffmpeg's own `-progress`
# output (real byte throughput) and only acts after a long, deliberately
# generous window of *zero* forward progress -- so normal jitter or a
# slow segment fetch never triggers a false restart of a healthy stream.
STALL_CHECK_INTERVAL = 5    # seconds -- how often the watchdog re-checks progress
STALL_TIMEOUT_SECONDS = float(os.environ.get("STALL_TIMEOUT_SECONDS", "60"))
STALL_WATCHDOG_ENABLED = os.environ.get("STALL_WATCHDOG_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")

_shutdown_requested = False


class StuckPipeError(Exception):
    """ffmpeg stdin write timed out -- the process is stuck/stalled (e.g. HTTP
    -listen mode with no client pulling on the output side)."""
    pass


def drain_stderr(proc: subprocess.Popen, stream_id: str) -> None:
    """
    ffmpeg er stderr pipe ke continuously read kore fela, background thread e.
    Ei thread na thakle stderr pipe (~64KB) bhore gele ffmpeg stderr e write
    korte block hoye jay, r shetar fole stdin read kora o bondho kore dey --
    jeta silent freeze er ekta karon hote pare.

    Recent line gulo proc._stderr_tail e rakha hoy, jate crash er por
    "ffmpeg last error" log kora jay (pipe to ekhon thread e drain hocche,
    tai proc.stderr.read() ar kaje debe na).
    """
    try:
        for raw_line in iter(proc.stderr.readline, b""):
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="ignore").rstrip()
            if line:
                logger.debug("[%s] ffmpeg stderr: %s", stream_id, line)
                tail = getattr(proc, "_stderr_tail", None)
                if tail is not None:
                    tail.append(line)
                    del tail[:-20]  # last 20 line rakhle jothesto
    except Exception:
        pass


def read_progress(proc: subprocess.Popen) -> None:
    """
    Reads ffmpeg's `-progress pipe:1` output -- structured key=value lines
    (frame=, total_size=, out_time_ms=, speed=, progress=continue/end),
    completely separate from the human-readable warnings on stderr.

    Whenever `total_size` (cumulative bytes pushed out over RTMP so far)
    actually increases, proc._last_progress_at is refreshed. This is the
    stall watchdog's real "is data still moving?" signal -- ffmpeg can be
    alive and accepting stdin without erroring while still being stuck
    downstream, but it cannot fake a growing output size while stuck.
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


def handle_signal(signum, frame):
    global _shutdown_requested
    logger.info("Received shutdown signal (%s), will stop after the current segment finishes...", signum)
    _shutdown_requested = True


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
    """
    .env file theke KEY=VALUE line gulo poRe os.environ e set kore.
    Already-set environment variable ke override kore na (setdefault use kore),
    tai shell theke export kora value uc chaile priority pabe.
    Kono external dependency (python-dotenv) lagbe na.
    """
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

            # Purota quote diye wrap kora thakle (single ba double), quote gulo shore fela
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]

            os.environ.setdefault(key, value)

    logger.info("Loaded environment variables from: %s", env_file_path)


def load_playlist_from_env(env_var: str) -> list[dict]:
    """
    LEGACY / not used by default anymore -- kept only as a reference/manual
    fallback for local dev without a Supabase project set up (e.g. you
    could call this instead of fetch_playlist_from_supabase() in run_stream()
    for a quick local test). Production flow now uses Supabase (see
    fetch_playlist_from_supabase() below) driven by STREAM_ID.

    Environment variable theke JSON playlist load kore RAM e list[dict]
    hisebe rakhe. Proti item e 'title' r 'url' thakbe.

    Format:
        ["https://.../v1/playlist.m3u8", "https://.../v2/playlist.m3u8"]
        -- ba --
        [{"title": "Video 1", "url": "https://.../v1/playlist.m3u8"}, ...]

    Dutoi support kora hoy: shudhu string dile auto title (video_1, video_2...)
    generate kore.
    """
    raw = os.environ.get(env_var)
    if not raw:
        raise ValueError(f"Environment variable '{env_var}' not found or empty.")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"'{env_var}' value is not valid JSON: {e}")

    if not isinstance(data, list) or not data:
        raise ValueError(f"'{env_var}' must be a non-empty JSON list.")

    playlist = []
    for i, item in enumerate(data):
        if isinstance(item, str):
            playlist.append({"title": f"video_{i + 1}", "url": item})
        elif isinstance(item, dict):
            url = item.get("url")
            if not url:
                raise ValueError(f"Playlist item {i} is missing the 'url' key: {item}")
            title = item.get("title") or f"video_{i + 1}"
            playlist.append({"title": title, "url": url})
        else:
            raise ValueError(f"Playlist item {i} has an invalid format (must be string or object): {item}")

    return playlist


def fetch_playlist_from_supabase(stream_id: str) -> list[dict]:
    """
    streams.id (stream_id) -> streams.playlist_id -> playlist_items
    (ordered by sort_order) -- builds the same [{"title","url"}, ...]
    shape load_playlist_from_env() used to produce, so the rest of the
    pipeline (run_stream()'s playback loop) doesn't need to change at all.

    Raises:
        ValueError            -- config/data problem (missing env vars,
                                  stream row not found, no playlist_id set,
                                  playlist has no usable items). Not a
                                  network issue, but the caller
                                  (_load_playlist_or_wait) retries these
                                  too, since a human may fix the DB row
                                  while this process keeps waiting.
        SupabaseFetchError     -- transient problem (network blip,
                                  Supabase momentarily down, bad response).
    """
    if not supabase_is_configured():
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must both be set in the environment.")

    streams = supabase_get("streams", {"id": f"eq.{stream_id}", "select": "playlist_id"})
    if not streams:
        raise ValueError(f"No 'streams' row found for id={stream_id}.")

    playlist_id = streams[0].get("playlist_id")
    if not playlist_id:
        raise ValueError(f"streams.id={stream_id} has no playlist_id set.")

    items = supabase_get(
        "playlist_items",
        {
            "playlist_id": f"eq.{playlist_id}",
            "select": "video_title,video_url,sort_order",
            "order": "sort_order.asc",
        },
    )
    if not items:
        raise ValueError(f"playlist_id={playlist_id} has no playlist_items.")

    playlist = []
    for i, item in enumerate(items):
        url = item.get("video_url")
        if not url:
            logger.warning("playlist_items row %d (playlist_id=%s) missing video_url, skipping.", i, playlist_id)
            continue
        title = item.get("video_title") or f"video_{i + 1}"
        playlist.append({"title": title, "url": url})

    if not playlist:
        raise ValueError(f"playlist_id={playlist_id}: every item was missing video_url.")

    return playlist


def _load_playlist_or_wait(stream_id: str) -> "list[dict] | None":
    """
    fetch_playlist_from_supabase() wrapper with infinite retry+fixed-backoff
    -- mirrors _build_ffmpeg_or_wait()'s "ffmpeg binary not found" handling
    below. The stream simply does not start until Supabase returns a usable
    playlist for this stream_id; there is intentionally no local-cache
    fallback (per requirement), so a stale/wrong playlist is never pushed.

    Both ValueError (bad/missing data) and SupabaseFetchError (transient
    network/DB issue) are retried identically here -- from this worker's
    point of view "DB is down" and "DB says nothing is ready yet" both
    just mean "not ready, keep waiting".

    Returns None only if shutdown was requested while waiting.
    """
    return wait_for_supabase(
        lambda: fetch_playlist_from_supabase(stream_id),
        description=f"Could not load playlist from Supabase for stream_id={stream_id}",
        shutdown_check=lambda: _shutdown_requested,
        logger=logger,
        retry_interval=SUPABASE_RETRY_INTERVAL_SECONDS,
    )


def fetch_bytes(url: str, timeout: int = 15) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "stream-worker/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def get_segment_urls(m3u8_url: str) -> list[str]:
    """Fetch the m3u8 playlist and return the segment (.ts) URLs."""
    data = None
    for attempt in range(M3U8_FETCH_RETRIES):
        try:
            data = fetch_bytes(m3u8_url).decode("utf-8", errors="ignore")
            break
        except Exception as e:
            logger.warning(
                "m3u8 fetch failed (%s) attempt %d/%d: %s",
                m3u8_url, attempt + 1, M3U8_FETCH_RETRIES, e,
            )
            time.sleep(2 * (attempt + 1))

    if data is None:
        return []

    segments = []
    for line in data.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        segments.append(urljoin(m3u8_url, line))

    return segments


def build_ffmpeg_process(output_url: str) -> subprocess.Popen:
    """
    Persistent ffmpeg process banay, jetar stdin diye raw mpegts bytes
    feed kora hobe. -c copy dile transcode lagbe na, shudhu remux+push.

    output_url: rtmp://...  -> SRS (ba onno RTMP media server) e sorasori
    push kore. Ei URL e ffmpeg nijei ekta client hisebe connect kore
    continuous data pathay -- kono "listen"/server mode nai.
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-loglevel", FFMPEG_LOGLEVEL,
        "-nostats",
        "-progress", "pipe:1",            # structured progress info on stdout, for the stall watchdog
        "-f", "mpegts",
        "-i", "pipe:0",
        "-c", "copy",
        "-f", "flv",
        output_url,
    ]

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,   # -progress output (not video data -- video goes out over RTMP)
        stderr=subprocess.PIPE,
    )

    # stderr pipe buffer full hoye deadlock jeno na hoy, tai continuously drain
    proc._stderr_tail = []
    proc._started_at = time.time()  # used by run_stream() to decide backoff vs. reset
    proc._last_progress_bytes = -1
    proc._last_progress_at = time.time()  # grace period starts at spawn, not at the first progress line
    t_err = threading.Thread(target=drain_stderr, args=(proc, "ffmpeg"), daemon=True)
    t_err.start()
    t_prog = threading.Thread(target=read_progress, args=(proc,), daemon=True)
    t_prog.start()

    return proc


def _build_ffmpeg_or_wait(rtmp_url: str, stream_id: str) -> subprocess.Popen:
    """
    build_ffmpeg_process() wrapper that specifically catches a missing
    ffmpeg binary (subprocess.Popen raises FileNotFoundError when the
    "ffmpeg" executable isn't installed / not on PATH).

    Without this, that FileNotFoundError is not caught anywhere else in
    this file -- it would propagate all the way out of run_stream() and
    crash the entire Python process (not just "ffmpeg died"). main.py's
    supervisor would then keep respawning a brand-new Python process
    over and over with a full traceback each time -- a noisy, heavier
    "endless crash loop" than simply waiting for ffmpeg to become
    available.

    Instead, this waits/retries in place with a clear, low-noise log
    message, at a fixed FFMPEG_RESTART_BACKOFF_MAX interval (no point
    hammering faster -- a missing binary won't fix itself in a second).

    Returns None only if shutdown was requested while waiting/retrying.
    """
    while not _shutdown_requested:
        try:
            return build_ffmpeg_process(rtmp_url)
        except FileNotFoundError:
            logger.error(
                "[%s] ffmpeg binary not found (not installed, or not on PATH). "
                "Install it (e.g. 'apt install ffmpeg' / 'brew install ffmpeg') -- "
                "retrying in %.0fs...",
                stream_id, FFMPEG_RESTART_BACKOFF_MAX,
            )
            waited = 0.0
            while waited < FFMPEG_RESTART_BACKOFF_MAX and not _shutdown_requested:
                time.sleep(0.5)
                waited += 0.5
    return None


def _ensure_nonblocking(proc: subprocess.Popen) -> None:
    """
    POSIX only. Puts the ffmpeg stdin pipe into non-blocking mode once,
    so writes can be paired with select() for a real OS-level timeout
    instead of a background thread.
    """
    if getattr(proc, "_stdin_nonblocking", False):
        return
    fd = proc.stdin.fileno()
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    proc._stdin_nonblocking = True


def _write_posix(proc: subprocess.Popen, data: bytes, timeout: float) -> None:
    """
    Production path (Linux/Ubuntu). No background thread is used here,
    so there is nothing to leak: select() waits (with a real timeout)
    for the stdin pipe to become writable, then a non-blocking os.write()
    pushes the bytes. If the pipe never becomes writable within `timeout`
    (ffmpeg stuck/stalled downstream), StuckPipeError is raised so the
    caller can restart ffmpeg -- exactly like the old thread-based
    behavior, but without ever leaving a stuck thread behind.
    """
    _ensure_nonblocking(proc)
    fd = proc.stdin.fileno()
    view = memoryview(data)
    deadline = time.time() + timeout

    while view:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise StuckPipeError(
                f"ffmpeg stdin write did not complete within {timeout}s -- process appears stuck"
            )
        try:
            _, writable, _ = select.select([], [fd], [], remaining)
        except (OSError, ValueError):
            raise BrokenPipeError("ffmpeg stdin fd is no longer valid (process has exited)")

        if not writable:
            raise StuckPipeError(
                f"ffmpeg stdin write did not complete within {timeout}s -- process appears stuck"
            )

        try:
            n = os.write(fd, view.tobytes())
        except BlockingIOError:
            continue
        except BrokenPipeError:
            raise
        view = view[n:]


def _get_writer_thread(proc: subprocess.Popen) -> "_StdinWriter":
    """
    Windows-only dev fallback: lazily creates a persistent background
    writer thread per ffmpeg proc (cached on proc._writer).
    """
    writer = getattr(proc, "_writer", None)
    if writer is None:
        writer = _StdinWriter(proc)
        proc._writer = writer
    return writer


class _StdinWriter:
    """
    Windows-only dev fallback for stuck-pipe detection.

    fcntl/select-based non-blocking writes are POSIX-only, so on Windows
    a dedicated background thread performs the actual blocking
    proc.stdin.write() call, and the main thread waits on the result with
    a timeout.

    If ffmpeg gets stuck downstream (no client pulling, network/SRS
    stalled), the write() call can block in the background thread
    forever -- but the main thread times out and raises StuckPipeError,
    which the caller treats like a BrokenPipeError to restart ffmpeg.
    (The background thread leaks in that case; harmless in practice
    since the proc itself is being killed/restarted, but on production
    -- Linux -- this class is not used at all, so no leak is possible there.)
    """

    def __init__(self, proc: subprocess.Popen):
        self._proc = proc
        self._in_q: "queue.Queue[bytes]" = queue.Queue()
        self._out_q: "queue.Queue[object]" = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while True:
            data = self._in_q.get()
            try:
                self._proc.stdin.write(data)
                self._proc.stdin.flush()
                self._out_q.put(None)  # success
            except Exception as e:
                self._out_q.put(e)
                return  # thread ends -- proc.stdin already broken/closed

    def write(self, data: bytes, timeout: float) -> None:
        self._in_q.put(data)
        try:
            result = self._out_q.get(timeout=timeout)
        except queue.Empty:
            raise StuckPipeError(
                f"ffmpeg stdin write did not complete within {timeout}s -- process appears stuck"
            )
        if isinstance(result, Exception):
            raise result


def write_stdin_with_timeout(proc: subprocess.Popen, data: bytes, timeout: float = STDIN_WRITE_TIMEOUT) -> None:
    """
    A direct proc.stdin.write() call blocks -- if ffmpeg is stuck on the
    output side (e.g. HTTP -listen mode with no client pulling, or RTMP
    push mode with a stalled network/SRS), ffmpeg stops reading stdin and
    this call would hang forever with no exception or log (a silent freeze).

    On production (Linux), this is handled with select()+non-blocking
    os.write() and a real timeout -- no background thread involved, so
    there is nothing to leak. On Windows (local dev only), a background
    writer thread is used as a fallback since fcntl/select don't support
    pipes there. Either way, StuckPipeError is raised on timeout so the
    caller can treat it like a BrokenPipeError and restart ffmpeg.
    """
    if _IS_POSIX:
        _write_posix(proc, data, timeout)
    else:
        writer = _get_writer_thread(proc)
        writer.write(data, timeout)


def stream_segment_to_ffmpeg(seg_url: str, proc: subprocess.Popen, stream_id: str) -> bool:
    """
    Ekta segment fetch kore, chunk kore ffmpeg er stdin e write kore.
    BrokenPipeError uthle mane ffmpeg mara geche -- caller ke propagate kori.
    Onno kono network error hole retry kori, sob retry fail korle False.
    """
    for attempt in range(SEGMENT_FETCH_RETRIES):
        try:
            req = urllib.request.Request(seg_url, headers={"User-Agent": "stream-worker/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                while True:
                    chunk = resp.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    write_stdin_with_timeout(proc, chunk)
            return True
        except (BrokenPipeError, StuckPipeError):
            raise
        except Exception as e:
            logger.warning(
                "[%s] Segment fetch fail (%s) attempt %d/%d: %s",
                stream_id, seg_url, attempt + 1, SEGMENT_FETCH_RETRIES, e,
            )
            time.sleep(SEGMENT_RETRY_BACKOFF_BASE * (attempt + 1))

    return False


def run_stream(rtmp_url: str, stream_id: str) -> None:
    # rtmp_url ekhane already-built FULL push target (base RTMP_URL + '/' +
    # stream_id) -- eta main() e build hoy, ei function-e shudhu use hoy.
    # DB shudhu ekhane -- process start/restart howar shomoy -- check kora
    # hoy (requirement onujayi). Ekbar load hoye gele playlist shudhu RAM e
    # thake; live mid-stream e DB theke re-check/re-sync kora hoy na. Worker
    # process pura crash kore main.py dara restart hole, run_stream() abar
    # notun kore call hoy -- tokhon DB theke fresh data ashbe (e.g. keu
    # playlist update kore thakle seta pore next restart e effective hobe).
    playlist = _load_playlist_or_wait(stream_id)
    if playlist is None:
        logger.info("[%s] Shutdown requested before the playlist could be loaded from Supabase.", stream_id)
        return
    logger.info(
        "[%s] Playlist loaded from Supabase, %d video(s).",
        stream_id, len(playlist),
    )

    proc = _build_ffmpeg_or_wait(rtmp_url, stream_id)
    if proc is None:
        logger.info("[%s] Shutdown requested before ffmpeg could start.", stream_id)
        return
    logger.info("[%s] ffmpeg (pid=%s) started, connecting to SRS...", stream_id, proc.pid)

    consecutive_crashes = 0  # tracks rapid repeat crashes to compute backoff; never stops retrying
    last_stall_check = time.time()

    if STALL_WATCHDOG_ENABLED:
        logger.info(
            "[%s] Stall watchdog enabled: will force a restart if ffmpeg reports zero "
            "forward progress for %.0fs straight (checked every %ds).",
            stream_id, STALL_TIMEOUT_SECONDS, STALL_CHECK_INTERVAL,
        )

    def _terminate(p: subprocess.Popen) -> None:
        try:
            p.stdin.close()
        except Exception:
            pass
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

    def _handle_stall_if_needed() -> None:
        """
        Checked once per segment (roughly every few seconds in practice --
        gated further by STALL_CHECK_INTERVAL). Only restarts ffmpeg if there
        has been *zero* forward progress for STALL_TIMEOUT_SECONDS straight --
        both deliberately generous so a healthy stream is never touched.
        """
        nonlocal proc, last_stall_check, consecutive_crashes
        if not STALL_WATCHDOG_ENABLED:
            return
        if time.time() - last_stall_check < STALL_CHECK_INTERVAL:
            return
        last_stall_check = time.time()

        stalled_for = time.time() - getattr(proc, "_last_progress_at", time.time())
        if stalled_for < STALL_TIMEOUT_SECONDS:
            return

        logger.error(
            "[%s] No forward progress from ffmpeg for %.0fs (last total_size=%s bytes) -- "
            "treating this as a silent stall (process alive but stuck) and forcing a restart...",
            stream_id, stalled_for, getattr(proc, "_last_progress_bytes", "?"),
        )
        _terminate(proc)

        ran_for = time.time() - getattr(proc, "_started_at", 0)
        if ran_for < FFMPEG_STABLE_RUN_SECONDS:
            consecutive_crashes += 1
        else:
            consecutive_crashes = 0
        delay = min(FFMPEG_RESTART_DELAY * (2 ** consecutive_crashes), FFMPEG_RESTART_BACKOFF_MAX)
        logger.warning("[%s] Restarting ffmpeg in %.1fs after stall...", stream_id, delay)
        time.sleep(delay)

        new_proc = _build_ffmpeg_or_wait(rtmp_url, stream_id)
        proc = new_proc  # None if shutdown was requested while waiting -- caller checks _shutdown_requested
        if proc is not None:
            last_stall_check = time.time()
            logger.info("[%s] ffmpeg restarted after stall (pid=%s)", stream_id, proc.pid)

    try:
        while not _shutdown_requested:
            for entry in playlist:
                if _shutdown_requested:
                    break

                video_url = entry["url"]
                video_title = entry["title"]

                segments = get_segment_urls(video_url)
                if not segments:
                    logger.error(
                        "[%s] Segment list empty/failed, skipping: %s (%s)",
                        stream_id, video_title, video_url,
                    )
                    continue

                logger.info(
                    "[%s] Pushing: %s (%d segments) -- %s",
                    stream_id, video_title, len(segments), video_url,
                )

                for seg_url in segments:
                    if _shutdown_requested:
                        break

                    _handle_stall_if_needed()
                    if proc is None or _shutdown_requested:
                        break

                    try:
                        ok = stream_segment_to_ffmpeg(seg_url, proc, stream_id)
                        if not ok:
                            logger.warning("[%s] Segment permanently skipped: %s", stream_id, seg_url)
                    except (BrokenPipeError, StuckPipeError) as e:
                        reason = "broken pipe" if isinstance(e, BrokenPipeError) else "stuck/stalled (write timeout)"
                        logger.error(
                            "[%s] ffmpeg process died/stuck (%s), restarting...",
                            stream_id, reason,
                        )
                        tail = getattr(proc, "_stderr_tail", None)
                        _terminate(proc)
                        if tail:
                            logger.warning("[%s] ffmpeg last error:\n%s", stream_id, "\n".join(tail))

                        # Stream never permanently stops: we always retry, just with
                        # a progressively longer (capped) delay if ffmpeg keeps
                        # crashing right away -- avoids hammering SRS/network in a
                        # tight loop, while a stable long-running process resets
                        # the delay back to the base value.
                        ran_for = time.time() - getattr(proc, "_started_at", 0)
                        if ran_for < FFMPEG_STABLE_RUN_SECONDS:
                            consecutive_crashes += 1
                        else:
                            consecutive_crashes = 0
                        delay = min(
                            FFMPEG_RESTART_DELAY * (2 ** consecutive_crashes),
                            FFMPEG_RESTART_BACKOFF_MAX,
                        )
                        logger.warning(
                            "[%s] Restarting ffmpeg in %.1fs (consecutive quick failures: %d)...",
                            stream_id, delay, consecutive_crashes,
                        )
                        time.sleep(delay)
                        proc = _build_ffmpeg_or_wait(rtmp_url, stream_id)
                        if proc is None:
                            break  # shutdown requested while waiting for ffmpeg
                        last_stall_check = time.time()
                        logger.info("[%s] ffmpeg restarted (pid=%s)", stream_id, proc.pid)

                if proc is None:
                    break

            if proc is None:
                break

            logger.info("[%s] Finished one full playlist round, looping back to the start.", stream_id)

    finally:
        logger.info("[%s] Shutting down, cleaning up ffmpeg...", stream_id)
        if proc is not None:
            _terminate(proc)
        logger.info("[%s] Worker stopped.", stream_id)


def main():
    parser = argparse.ArgumentParser(
        description="Single persistent-ffmpeg stream worker: HLS segments -> stdin pipe -> ffmpeg -> SRS"
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env file (default: .env)")
    parser.add_argument(
        "--rtmp-url", default=None,
        help="SRS RTMP ingest BASE URL (e.g. rtmp://localhost:1935/live/ -- no stream key/name at the "
             "end, shudhu trailing '/' porjonto). STREAM_ID ei base URL-er shathe jog kore actual push "
             "target banano hoy. Na dile .env/environment er RTMP_URL use hobe.",
    )
    parser.add_argument(
        "--stream-id", default=None,
        help="streams.id (uuid) -- Supabase theke playlist fetch korar jonno, AR push target URL "
             "(base RTMP_URL + '/' + stream_id) banano r log identification-er jonno-o lagbe. "
             "Na dile .env/environment er STREAM_ID use hobe.",
    )
    parser.add_argument("--log-file", default=None, help="Log file path (na dile stdout e log hobe)")
    args = parser.parse_args()

    # .env file age load kori, jate RTMP_URL/STREAM_ID/SUPABASE_URL/
    # SUPABASE_SERVICE_ROLE_KEY shob environment e chole ashe -- CLI diye dile
    # shetai priority pabe.
    #
    # NOTE: supabase_client.py SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY LAZILY
    # poRe (proti API call-er shomoy fresh os.environ.get(), import-er
    # shomoy cache kore rakhe na) -- tai eikhane load_env_file() ei script
    # er nijer `from supabase_client import ...` er AGE na PORE call hocche,
    # shetatao kono somossha na. Ei script eka eka (standalone, main.py
    # chara) direct run korleo .env file e SUPABASE_URL/
    # SUPABASE_SERVICE_ROLE_KEY thakleই hobe, alada kore shell e export
    # korte hobe na.
    #
    # STALL_TIMEOUT_SECONDS / STALL_WATCHDOG_ENABLED (upore, module-level e)
    # holo alada case -- oigulo genuinely import-time e ekbar poRa hoy (env
    # var value time e change hoy na, restart chara update hoy na), tai
    # shegulo standalone run e shell export lagbei -- .env-e likhleo,
    # module import-er AGE load_env_file() call na hole dhorbe na.
    load_env_file(args.env_file)

    setup_logging("stream_worker", log_file=args.log_file)

    stream_id = args.stream_id or os.environ.get("STREAM_ID")
    set_stream_id("stream_worker", stream_id or "-")

    rtmp_base = args.rtmp_url or os.environ.get("RTMP_URL")

    if not rtmp_base:
        parser.error("Provide --rtmp-url or set RTMP_URL in the .env file.")
    if not stream_id:
        parser.error(
            "Provide --stream-id or set STREAM_ID in the .env file "
            "(needed to look up the playlist for this stream in Supabase, AND to build the push URL)."
        )

    # RTMP_URL ekhon ekta BASE URL (e.g. "rtmp://localhost:1935/live/") --
    # STREAM_NAME ar lagbe na, karon STREAM_ID-i ekhon stream-key/path
    # hisebe use hocche: actual push target = base + '/' + STREAM_ID.
    # (e.g. RTMP_URL=rtmp://localhost:1935/live/ + STREAM_ID=abc123
    #  -> rtmp://localhost:1935/live/abc123)
    rtmp_url = f"{rtmp_base.rstrip('/')}/{stream_id}"
    logger.info("[%s] Push target: %s", stream_id, rtmp_url)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    run_stream(rtmp_url, stream_id)


if __name__ == "__main__":
    main()