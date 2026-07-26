#!/usr/bin/env python3
"""
stream_worker_stdin.py

Ekta persistent ffmpeg process chalu thake, jetar stdin e Python script
HLS video (.ts segment) er raw bytes por por push kore. Ffmpeg oi data
-c copy diye SRS e RTMP push kore, tai video-to-video transition e
kono reconnect/gap hoy na (continuous single RTMP connection).

Playlist file er poriborte, environment variable theke JSON hisebe
playlist load kora hoy (title + url shoho), RAM e ekta list[dict] hisebe
thake -- kono disk file lagbe na. Shob config .env file diye dewa jay,
jate baar baar CLI e lomba flag likhte na hoy.

Usage (.env file diye):
    # .env file:
    #   RTMP_URL=rtmp://localhost:1935/live/stream_01
    #   STREAM_NAME=stream_01
    #   STREAM_PLAYLIST=[{"title":"Video 1","url":"https://.../v1/playlist.m3u8"}]

    python3 stream_worker_stdin.py --log-file stream_01.log

Usage (CLI diye override, .env chara o):
    python3 stream_worker_stdin.py \
        --rtmp-url rtmp://localhost:1935/live/stream_01 \
        --stream-name stream_01 \
        --log-file stream_01.log

.env file er path change korte chaile --env-file flag, r playlist er jonno
onno environment variable naam chaile --playlist-env flag use koro.
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
import urllib.request
from pathlib import Path
from urllib.parse import urljoin

from logging_setup import setup_logging, set_stream_id

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

_shutdown_requested = False


class StuckPipeError(Exception):
    """ffmpeg stdin write timed out -- the process is stuck/stalled (e.g. HTTP
    -listen mode with no client pulling on the output side)."""
    pass


def drain_stderr(proc: subprocess.Popen, stream_name: str) -> None:
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
                logger.debug("[%s] ffmpeg stderr: %s", stream_name, line)
                tail = getattr(proc, "_stderr_tail", None)
                if tail is not None:
                    tail.append(line)
                    del tail[:-20]  # last 20 line rakhle jothesto
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
        "-f", "mpegts",
        "-i", "pipe:0",
        "-c", "copy",
        "-f", "flv",
        output_url,
    ]

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    # stderr pipe buffer full hoye deadlock jeno na hoy, tai continuously drain
    proc._stderr_tail = []
    proc._started_at = time.time()  # used by run_stream() to decide backoff vs. reset
    t = threading.Thread(target=drain_stderr, args=(proc, "ffmpeg"), daemon=True)
    t.start()

    return proc


def _build_ffmpeg_or_wait(rtmp_url: str, stream_name: str) -> subprocess.Popen:
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
                stream_name, FFMPEG_RESTART_BACKOFF_MAX,
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


def stream_segment_to_ffmpeg(seg_url: str, proc: subprocess.Popen, stream_name: str) -> bool:
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
                stream_name, seg_url, attempt + 1, SEGMENT_FETCH_RETRIES, e,
            )
            time.sleep(SEGMENT_RETRY_BACKOFF_BASE * (attempt + 1))

    return False


def run_stream(playlist_env: str, rtmp_url: str, stream_name: str) -> None:
    playlist = load_playlist_from_env(playlist_env)
    logger.info(
        "[%s] Playlist loaded into memory (from '%s'), %d video(s).",
        stream_name, playlist_env, len(playlist),
    )

    proc = _build_ffmpeg_or_wait(rtmp_url, stream_name)
    if proc is None:
        logger.info("[%s] Shutdown requested before ffmpeg could start.", stream_name)
        return
    logger.info("[%s] ffmpeg (pid=%s) started, connecting to SRS...", stream_name, proc.pid)

    consecutive_crashes = 0  # tracks rapid repeat crashes to compute backoff; never stops retrying

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
                        stream_name, video_title, video_url,
                    )
                    continue

                logger.info(
                    "[%s] Pushing: %s (%d segments) -- %s",
                    stream_name, video_title, len(segments), video_url,
                )

                for seg_url in segments:
                    if _shutdown_requested:
                        break
                    try:
                        ok = stream_segment_to_ffmpeg(seg_url, proc, stream_name)
                        if not ok:
                            logger.warning("[%s] Segment permanently skipped: %s", stream_name, seg_url)
                    except (BrokenPipeError, StuckPipeError) as e:
                        reason = "broken pipe" if isinstance(e, BrokenPipeError) else "stuck/stalled (write timeout)"
                        logger.error(
                            "[%s] ffmpeg process died/stuck (%s), restarting...",
                            stream_name, reason,
                        )
                        try:
                            proc.stdin.close()
                        except Exception:
                            pass
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)

                        tail = getattr(proc, "_stderr_tail", None)
                        if tail:
                            logger.warning("[%s] ffmpeg last error:\n%s", stream_name, "\n".join(tail))

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
                            stream_name, delay, consecutive_crashes,
                        )
                        time.sleep(delay)
                        proc = _build_ffmpeg_or_wait(rtmp_url, stream_name)
                        if proc is None:
                            break  # shutdown requested while waiting for ffmpeg
                        logger.info("[%s] ffmpeg restarted (pid=%s)", stream_name, proc.pid)

                if proc is None:
                    break

            if proc is None:
                break

            logger.info("[%s] Finished one full playlist round, looping back to the start.", stream_name)

    finally:
        logger.info("[%s] Shutting down, cleaning up ffmpeg...", stream_name)
        if proc is not None:
            try:
                proc.stdin.close()
            except Exception:
                pass
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        logger.info("[%s] Worker stopped.", stream_name)


def main():
    parser = argparse.ArgumentParser(
        description="Single persistent-ffmpeg stream worker: HLS segments -> stdin pipe -> ffmpeg -> SRS"
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env file (default: .env)")
    parser.add_argument(
        "--playlist-env", default="STREAM_PLAYLIST",
        help="Environment variable naam jekhane JSON playlist thakbe (default: STREAM_PLAYLIST)",
    )
    parser.add_argument(
        "--rtmp-url", default=None,
        help="SRS RTMP ingest URL. Na dile .env/environment er RTMP_URL use hobe.",
    )
    parser.add_argument(
        "--stream-name", default=None,
        help="Log identification er jonno naam. Na dile .env/environment er STREAM_NAME use hobe.",
    )
    parser.add_argument("--log-file", default=None, help="Log file path (na dile stdout e log hobe)")
    args = parser.parse_args()

    # .env file age load kori, jate STREAM_PLAYLIST/RTMP_URL/STREAM_NAME/STREAM_ID
    # shob environment e chole ashe -- CLI diye dile shetai priority pabe.
    load_env_file(args.env_file)

    setup_logging("stream_worker", log_file=args.log_file)
    set_stream_id("stream_worker", os.environ.get("STREAM_ID", "-"))

    rtmp_url = args.rtmp_url or os.environ.get("RTMP_URL")
    stream_name = args.stream_name or os.environ.get("STREAM_NAME")

    if not rtmp_url:
        parser.error("Provide --rtmp-url or set RTMP_URL in the .env file.")
    if not stream_name:
        parser.error("Provide --stream-name or set STREAM_NAME in the .env file.")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    run_stream(args.playlist_env, rtmp_url, stream_name)


if __name__ == "__main__":
    main()