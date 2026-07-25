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

RESTART_DELAY = 5  # seconds, ffmpeg crash korle koto sec por retry hobe
STDERR_TAIL_LINES = 20

_shutdown_requested = False
_reload_requested = False


def handle_signal(signum, frame):
    global _shutdown_requested
    logging.info("Shutdown signal (%s) peyechi, bondho hocche...", signum)
    _shutdown_requested = True


def handle_reload(signum, frame):
    """
    SIGHUP (Linux/WSL) e destination list live re-read kore, running ffmpeg
    ke restart kore notun list diye -- eta chara normally shudhu next crash-e
    notun list effective hoy. Windows-e SIGHUP nai, tai Windows-e eta kaje
    lagbe na -- Windows-e change korte hole script restart korte hobe.
    """
    global _reload_requested
    logging.info("Reload signal peyechi, destination list refresh hobe...")
    _reload_requested = True


def load_env_file(env_file_path: str) -> None:
    path = Path(env_file_path)
    if not path.exists():
        logging.info(".env file paoya jayni (%s), shudhu shell environment use hobe.", env_file_path)
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

            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]

            os.environ[key] = value  # relay script e re-read korte hobe, tai override kori

    logging.info(".env file theke variables (re)load kora hoyeche: %s", env_file_path)


def load_destinations(env_var: str) -> list[dict]:
    raw = os.environ.get(env_var)
    if not raw:
        raise ValueError(f"Environment variable '{env_var}' pawa jayni ba khali.")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"'{env_var}' er value valid JSON na: {e}")

    if not isinstance(data, list) or not data:
        raise ValueError(f"'{env_var}' ekta non-empty JSON list hote hobe.")

    destinations = []
    for i, item in enumerate(data):
        if isinstance(item, str):
            destinations.append({"name": f"dest_{i + 1}", "url": item})
        elif isinstance(item, dict):
            url = item.get("url")
            if not url:
                raise ValueError(f"Destination item {i} e 'url' key nai: {item}")
            name = item.get("name") or f"dest_{i + 1}"
            destinations.append({"name": name, "url": url})
        else:
            raise ValueError(f"Destination item {i} er format thik na: {item}")

    return destinations


def build_tee_output(destinations: list[dict]) -> str:
    """
    ffmpeg -f tee er jonno "[f=flv]url1|[f=flv]url2|..." format banay.
    Prottekta destination-e alada-alada [f=flv] wrap thake, jate ekta
    destination fail korleo (tee-er default behavior) onnogulo cholte thake.
    """
    parts = [f"[f=flv]{d['url']}" for d in destinations]
    return "|".join(parts)


def drain_stderr(proc: subprocess.Popen, tail: list) -> None:
    try:
        for raw_line in iter(proc.stderr.readline, b""):
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="ignore").rstrip()
            if line:
                logging.debug("ffmpeg stderr: %s", line)
                tail.append(line)
                del tail[:-STDERR_TAIL_LINES]
    except Exception:
        pass


def build_ffmpeg_process(source_url: str, destinations: list[dict]) -> subprocess.Popen:
    tee_output = build_tee_output(destinations)
    cmd = [
        "ffmpeg",
        "-y",
        "-loglevel", "warning",
        "-analyzeduration", "10000000",   # 10s -- input stream detect korte beshi shomoy dey
        "-probesize", "10000000",         # 10MB -- beshi data dekhe stream detect kore
        "-rtmp_live", "live",             # live RTMP source (no seek), fresh connect e keyframe wait kore
        "-i", source_url,
        "-map", "0",                      # tee muxer explicit map chara input stream auto-detect kore na
        "-c", "copy",
        "-f", "tee",
        tee_output,
    ]
    logging.info("ffmpeg command: %s", " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    proc._stderr_tail = []
    t = threading.Thread(target=drain_stderr, args=(proc, proc._stderr_tail), daemon=True)
    t.start()
    return proc


def run_relay(env_file: str, source_env: str, dest_env: str) -> None:
    global _reload_requested

    load_env_file(env_file)
    source_url = os.environ.get(source_env)
    if not source_url:
        raise ValueError(f"'{source_env}' set kora nai (.env file check koro).")

    destinations = load_destinations(dest_env)
    logging.info(
        "Relay shuru hocche: source=%s -> %d destination(s): %s",
        source_url, len(destinations), ", ".join(d["name"] for d in destinations),
    )

    proc = build_ffmpeg_process(source_url, destinations)
    logging.info("ffmpeg (pid=%s) push shuru hoyeche.", proc.pid)

    try:
        while not _shutdown_requested:
            ret = proc.poll()

            if _reload_requested:
                _reload_requested = False
                logging.info("Reload: .env abar load kore, ffmpeg restart hocche notun destination list diye...")
                load_env_file(env_file)
                try:
                    destinations = load_destinations(dest_env)
                except ValueError as e:
                    logging.error("Notun destination list invalid, purono list-e continue kora hocche: %s", e)
                else:
                    try:
                        proc.terminate()
                        proc.wait(timeout=5)
                    except Exception:
                        proc.kill()
                    proc = build_ffmpeg_process(source_url, destinations)
                    logging.info(
                        "ffmpeg (pid=%s) restart hoyeche, %d destination(s): %s",
                        proc.pid, len(destinations), ", ".join(d["name"] for d in destinations),
                    )
                continue

            if ret is not None:
                tail = getattr(proc, "_stderr_tail", None)
                if tail:
                    logging.warning("ffmpeg exited (code=%s), last error:\n%s", ret, "\n".join(tail))
                else:
                    logging.warning("ffmpeg exited (code=%s)", ret)

                time.sleep(RESTART_DELAY)
                # crash-restart e .env freshly re-read kori, tai edit kore
                # rakha notun destination list ei muhurte-e apply hoye jabe
                load_env_file(env_file)
                destinations = load_destinations(dest_env)
                proc = build_ffmpeg_process(source_url, destinations)
                logging.info("ffmpeg restart hoyeche (pid=%s)", proc.pid)
                continue

            time.sleep(1)
    finally:
        logging.info("Bondho hocche, ffmpeg cleanup hocche...")
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        logging.info("Relay shesh.")


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

    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, handle_reload)  # Windows e available na, tai guarded

    try:
        run_relay(args.env_file, args.source_env, args.dest_env)
    except ValueError as e:
        logging.error("Config error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()