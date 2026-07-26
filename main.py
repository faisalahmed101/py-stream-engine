#!/usr/bin/env python3
"""
main.py

Node-Media-Server, stream_worker_stdin.py, r push_relay.py -- tin-ta
alada terminal e chalanor poriborte, ekta single command diye ekshathe
chalanor jonno launcher/supervisor script. Shob process child process
hisebe start hoy, output prefix shoho (ekta terminal e-i) merge kore
dekhano hoy, r Ctrl+C dile shob-i clean-e bondho hoye jay.

Order:
    1. Node-Media-Server start hoy (node node-media-server.js)
    2. Kichu shomoy wait kora hoy (NMS port bind hote)
    3. stream_worker_stdin.py start hoy (video source -> NMS e publish)
    4. Kichu shomoy wait kora hoy (worker-er first segment publish hote)
    5. push_relay.py start hoy (NMS theke pull kore YouTube/Facebook e push)

Usage:
    python3 main.py

Config (.env file theke, ba environment variable diye override):
    NMS_DIR=node-media-server              # node-media-server.js jei subfolder e ache (relative, main.py er tulonay)
    NMS_SCRIPT=node-media-server.js        # Node-Media-Server entry file naam
    NMS_STARTUP_WAIT=8                     # NMS start howar por koto sec wait korbe
    WORKER_STARTUP_WAIT=5                  # worker start howar por koto sec wait korbe push_relay chalur age
    WORKER_LOG=stream_01.log               # stream_worker_stdin.py er log file naam
    PUSH_RELAY_LOG=push_relay.log          # push_relay er log file naam
    RUN_ALL_LOG=run_all.log                # main.py nijer log file naam
    STREAM_ID=stream_01                    # proti log line e "[stream=...]" hisebe bosbe
    DEBUG=false                            # true dile verbose (DEBUG level) log dekhabe

Ctrl+C (SIGINT) dile shob process (NMS + worker + push_relay) gracefully terminate hobe.
"""

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from logging_setup import setup_logging, set_stream_id

logger = setup_logging("run_all")  # log_file/stream_id main() theke set hobe

_shutdown_requested = False
_children: list[subprocess.Popen] = []


def load_env_file(env_file_path: str) -> None:
    """
    .env file theke KEY=VALUE line gulo poRe os.environ e set kore.
    Already-set environment variable (shell theke export kora) ke
    override kore na.
    """
    path = Path(env_file_path)
    if not path.exists():
        logger.info(f"[run_all] .env file paoya jayni ({env_file_path}), shudhu shell environment use hobe.")
        return

    with path.open("r", encoding="utf-8") as f:
        for line_num, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue

            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()

            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]

            os.environ.setdefault(key, value)

    logger.info(f"[run_all] .env file theke variables load kora hoyeche: {env_file_path}")


def handle_signal(signum, frame):
    global _shutdown_requested
    logger.info(f"[run_all] Shutdown signal ({signum}) peyechi, shob process bondho hocche...")
    _shutdown_requested = True


def stream_output(proc: subprocess.Popen, prefix: str) -> None:
    """Child process er stdout/stderr ke ekta prefix shoho merge kore print kore."""
    try:
        for raw_line in iter(proc.stdout.readline, b""):
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="ignore").rstrip()
            if line:
                logger.info(f"[{prefix}] {line}")
    except Exception:
        pass


def start_process(cmd: list[str], prefix: str, cwd: str = None) -> subprocess.Popen:
    logger.info(f"[run_all] Starting {prefix}: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    _children.append(proc)
    t = threading.Thread(target=stream_output, args=(proc, prefix), daemon=True)
    t.start()
    return proc


def terminate_all() -> None:
    for proc in _children:
        if proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass
    deadline = time.time() + 10
    for proc in _children:
        remaining = max(0, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except Exception:
                pass


def main():
    here = Path(__file__).resolve().parent

    load_env_file(str(here / ".env"))

    setup_logging("run_all", log_file=os.environ.get("RUN_ALL_LOG", "run_all.log"))
    set_stream_id("run_all", os.environ.get("STREAM_ID", "-"))

    nms_dir_rel = os.environ.get("NMS_DIR", ".")   # node-media-server.js jei subfolder e ache
    nms_script = os.environ.get("NMS_SCRIPT", "node-media-server.js")
    nms_dir = (here / nms_dir_rel).resolve()
    startup_wait = float(os.environ.get("NMS_STARTUP_WAIT", "8"))
    worker_startup_wait = float(os.environ.get("WORKER_STARTUP_WAIT", "5"))
    worker_log = os.environ.get("WORKER_LOG", "stream_01.log")
    push_relay_log = os.environ.get("PUSH_RELAY_LOG", "push_relay.log")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # 1) Node-Media-Server -- nijer subfolder e run kora hoy (node_modules
    #    shei subfolder e thake bole, cwd oikhanei set kora joruri)
    nms_proc = start_process(["node", nms_script], "NMS", cwd=str(nms_dir))

    # 2) NMS port bind howar jonno wait (fixed delay -- simple r reliable,
    #    kono health-check API depend kora lagbe na)
    logger.info(f"[run_all] NMS start hocche, {startup_wait}s wait kora hocche...")
    waited = 0.0
    while waited < startup_wait and not _shutdown_requested:
        if nms_proc.poll() is not None:
            logger.info("[run_all] NMS process nijei exit kore geche startup wait er modhye -- thamano hocche.")
            terminate_all()
            sys.exit(1)
        time.sleep(0.5)
        waited += 0.5

    if _shutdown_requested:
        terminate_all()
        sys.exit(0)

    # 3) stream_worker_stdin.py -- actual video source, jeta stream_01 e publish kore
    worker_proc = start_process(
        [sys.executable, "stream_worker_stdin.py", "--log-file", worker_log],
        "WORKER",
        cwd=str(here),
    )

    # 4) Worker-ke kichu shomoy dei jate first segment(s) publish hoye NMS e
    #    pouche jay, noyle push_relay-er ffmpeg khali stream e connect kore
    #    kono data na peye bar bar disconnect/reconnect korte thakbe.
    logger.info(f"[run_all] Worker start hocche, {worker_startup_wait}s wait kora hocche...")
    waited = 0.0
    while waited < worker_startup_wait and not _shutdown_requested:
        if worker_proc.poll() is not None:
            logger.info("[run_all] Worker process nijei exit kore geche startup wait er modhye -- thamano hocche.")
            terminate_all()
            sys.exit(1)
        time.sleep(0.5)
        waited += 0.5

    if _shutdown_requested:
        terminate_all()
        sys.exit(0)

    # 5) push_relay.py
    push_relay_proc = start_process(
        [sys.executable, "push_relay.py", "--log-file", push_relay_log],
        "RELAY",
        cwd=str(here),
    )

    # Jekono ekta process nijei exit kore gele, ba Ctrl+C ashle -- shob bondho kore dei.
    try:
        while not _shutdown_requested:
            if nms_proc.poll() is not None:
                logger.info("[run_all] NMS process exit kore geche, shob bondho kora hocche...")
                break
            if worker_proc.poll() is not None:
                logger.info("[run_all] Worker process exit kore geche, shob bondho kora hocche...")
                break
            if push_relay_proc.poll() is not None:
                logger.info("[run_all] push_relay process exit kore geche, shob bondho kora hocche...")
                break
            time.sleep(1)
    finally:
        terminate_all()
        logger.info("[run_all] Shesh.")


if __name__ == "__main__":
    main()