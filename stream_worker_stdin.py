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
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urljoin

# ---------- Config ----------
SEGMENT_FETCH_RETRIES = 3
SEGMENT_RETRY_BACKOFF_BASE = 2      # second
M3U8_FETCH_RETRIES = 3
CHUNK_SIZE = 64 * 1024              # 64KB kore stream kora hobe
FFMPEG_LOGLEVEL = "warning"
FFMPEG_RESTART_DELAY = 3            # ffmpeg crash korle koto sec por restart hobe

_shutdown_requested = False


def handle_signal(signum, frame):
    global _shutdown_requested
    logging.info("Shutdown signal (%s) peyechi, current segment shesh hole বন্ধ হবে...", signum)
    _shutdown_requested = True


def load_env_file(env_file_path: str) -> None:
    """
    .env file theke KEY=VALUE line gulo poRe os.environ e set kore.
    Already-set environment variable ke override kore na (setdefault use kore),
    tai shell theke export kora value uc chaile priority pabe.
    Kono external dependency (python-dotenv) lagbe na.
    """
    path = Path(env_file_path)
    if not path.exists():
        logging.info(".env file paoya jayni (%s), শুধু shell environment use hobe.", env_file_path)
        return

    with path.open("r", encoding="utf-8") as f:
        for line_num, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                logging.warning(".env file er %d nong line e '=' nai, skip kora hocche: %s", line_num, line)
                continue

            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()

            # Purota quote diye wrap kora thakle (single ba double), quote gulo shore fela
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]

            os.environ.setdefault(key, value)

    logging.info(".env file theke variables load kora hoyeche: %s", env_file_path)


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
        raise ValueError(f"Environment variable '{env_var}' pawa jayni ba khali.")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"'{env_var}' er value valid JSON na: {e}")

    if not isinstance(data, list) or not data:
        raise ValueError(f"'{env_var}' ekta non-empty JSON list hote hobe.")

    playlist = []
    for i, item in enumerate(data):
        if isinstance(item, str):
            playlist.append({"title": f"video_{i + 1}", "url": item})
        elif isinstance(item, dict):
            url = item.get("url")
            if not url:
                raise ValueError(f"Playlist item {i} e 'url' key nai: {item}")
            title = item.get("title") or f"video_{i + 1}"
            playlist.append({"title": title, "url": url})
        else:
            raise ValueError(f"Playlist item {i} er format thik na (string ba object hote hobe): {item}")

    return playlist


def fetch_bytes(url: str, timeout: int = 15) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "stream-worker/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def get_segment_urls(m3u8_url: str) -> list[str]:
    """m3u8 playlist fetch kore segment (.ts) URL gulo return kore."""
    data = None
    for attempt in range(M3U8_FETCH_RETRIES):
        try:
            data = fetch_bytes(m3u8_url).decode("utf-8", errors="ignore")
            break
        except Exception as e:
            logging.warning(
                "m3u8 fetch fail (%s) attempt %d/%d: %s",
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

    output_url:
        - rtmp://...   -> SRS/normal RTMP server e push kore (production)
        - http://...   -> ffmpeg nijei ekta HTTP server hisebe serve kore
                          (SRS setup na thakle test korar jonno). Ei URL
                          VLC/ffplay diye open korle stream check kora jay.
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-loglevel", FFMPEG_LOGLEVEL,
        "-f", "mpegts",
        "-i", "pipe:0",
        "-c", "copy",
        "-f", "flv",
    ]

    if output_url.startswith("http://") or output_url.startswith("https://"):
        # ffmpeg http server hisebe act korbe, tai -listen 1 lagbe
        cmd += ["-listen", "1"]

    cmd.append(output_url)

    return subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


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
                    proc.stdin.write(chunk)
            return True
        except BrokenPipeError:
            raise
        except Exception as e:
            logging.warning(
                "[%s] Segment fetch fail (%s) attempt %d/%d: %s",
                stream_name, seg_url, attempt + 1, SEGMENT_FETCH_RETRIES, e,
            )
            time.sleep(SEGMENT_RETRY_BACKOFF_BASE * (attempt + 1))

    return False


def run_stream(playlist_env: str, rtmp_url: str, stream_name: str) -> None:
    playlist = load_playlist_from_env(playlist_env)
    logging.info(
        "[%s] Playlist RAM e load hoyeche ('%s' theke), %d ta video ache.",
        stream_name, playlist_env, len(playlist),
    )

    proc = build_ffmpeg_process(rtmp_url)
    logging.info("[%s] ffmpeg (pid=%s) start hoyeche, SRS e connect hocche...", stream_name, proc.pid)

    try:
        while not _shutdown_requested:
            for entry in playlist:
                if _shutdown_requested:
                    break

                video_url = entry["url"]
                video_title = entry["title"]

                segments = get_segment_urls(video_url)
                if not segments:
                    logging.error(
                        "[%s] Segment list khali/fail hoyeche, skip kora hocche: %s (%s)",
                        stream_name, video_title, video_url,
                    )
                    continue

                logging.info(
                    "[%s] Push hocche: %s (%d segments) -- %s",
                    stream_name, video_title, len(segments), video_url,
                )

                for seg_url in segments:
                    if _shutdown_requested:
                        break
                    try:
                        ok = stream_segment_to_ffmpeg(seg_url, proc, stream_name)
                        if not ok:
                            logging.warning("[%s] Segment permanently skip: %s", stream_name, seg_url)
                    except BrokenPipeError:
                        logging.error(
                            "[%s] ffmpeg process mara geche (broken pipe), restart hocche...",
                            stream_name,
                        )
                        try:
                            proc.stdin.close()
                        except Exception:
                            pass
                        proc.wait(timeout=5)
                        stderr_out = proc.stderr.read() if proc.stderr else b""
                        if stderr_out:
                            logging.warning("[%s] ffmpeg last error:\n%s", stream_name, stderr_out[-500:])

                        time.sleep(FFMPEG_RESTART_DELAY)
                        proc = build_ffmpeg_process(rtmp_url)
                        logging.info("[%s] ffmpeg restart hoyeche (pid=%s)", stream_name, proc.pid)

            logging.info("[%s] Puro playlist ek round shesh, abar shuru hocche (loop).", stream_name)

    finally:
        logging.info("[%s] Bondho hocche, ffmpeg cleanup hocche...", stream_name)
        try:
            proc.stdin.close()
        except Exception:
            pass
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        logging.info("[%s] Worker shesh.", stream_name)


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

    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )

    # .env file age load kori, jate STREAM_PLAYLIST/RTMP_URL/STREAM_NAME shob
    # environment e chole ashe -- CLI diye dile shetai priority pabe.
    load_env_file(args.env_file)

    rtmp_url = args.rtmp_url or os.environ.get("RTMP_URL")
    stream_name = args.stream_name or os.environ.get("STREAM_NAME")

    if not rtmp_url:
        parser.error("--rtmp-url dao othoba .env file e RTMP_URL set koro.")
    if not stream_name:
        parser.error("--stream-name dao othoba .env file e STREAM_NAME set koro.")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    run_stream(args.playlist_env, rtmp_url, stream_name)


if __name__ == "__main__":
    main()