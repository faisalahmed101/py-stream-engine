#!/usr/bin/env python3
"""
main.py

stream_worker_stdin.py r push_relay.py -- duita alada terminal e chalanor
poriborte, ekta single command diye ekshathe chalanor jonno launcher/
supervisor script.

NOTE: Node-Media-Server (NMS) ekhon r ei script manage kore na. RTMP
server hisebe ekhon SRS (ossrs/srs) use kora hocche, jeta ALADA-vabe
Docker-e chalano hoy (e.g. `docker run -d --name srs -p 1935:1935 -p
1985:1985 -p 8080:8080 ossrs/srs:5`) -- ei script shudhu dhore neয় SRS
already up ache (RTMP_URL-e), tai NMS start/poll kora shob code bad
deওয়া hoyeche.

Order:
    0. SRS-er RTMP port (RTMP_URL theke) khola ache kina check kora hoy
       (blocking -- SRS up na thakle proti SRS_CHECK_INTERVAL sec por por
       retry kora hoy, jotokkhon na SRS up hoy ba Ctrl+C ashe).
    1. stream_worker_stdin.py start hoy (video source -> SRS e publish,
       RTMP_URL e already-running SRS dhore niye)
    2. Worker-er log e "Push hocche:" (first segment publish shuru
       howar marker) dekha porjonto active wait kora hoy.
    3. push_relay.py start hoy (SRS theke pull kore YouTube/Facebook e push)

Production-grade reliability:
    - Proti child process (WORKER / RELAY) ke ALADA-ALADA vabe supervise
      kora hoy. Jekono ekta process jekono karone (crash, unhandled
      exception, kill -9, OOM) exit/theme gele, shudhu shetake
      exponential backoff shoho automatic restart kora hoy -- baki
      process ta unaffected thake (proti process nijer moddhe already
      resilient: e.g. push_relay/stream_worker nijeder ffmpeg subprocess
      crash nijera i handle kore, main.py shudhu porar Python process
      nijei crash korle top-level e restart kore).
    - Startup-eo guarantee deওয়া hoy -- kono step (WORKER/RELAY) shuru
      hotei fail korle main.py exit kore na, backoff shoho retry korte
      thake jotokkhon na successful hoy ba Ctrl+C আসে.

Usage:
    python3 main.py

Prerequisite: SRS Docker container already running, e.g.:
    docker run -d --name srs -p 1935:1935 -p 1985:1985 -p 8080:8080 \\
        -p 8000:8000/udp -p 10080:10080/udp ossrs/srs:5

Config (.env file theke, ba environment variable diye override):
    WORKER_STARTUP_WAIT=5                  # worker "ready" howar jonno max wait (safety-net, blind sleep na)
    RTMP_URL=rtmp://localhost:1935/live/   # BASE URL (STREAM_NAME ar lagbe na -- STREAM_ID-i ekhon
                                            # push/pull path e boshe: base + '/' + STREAM_ID). Ei
                                            # URL-er host:port e SRS up ache kina, WORKER start korar
                                            # age eka script nijei check kore neয়.
    SRS_CHECK_TIMEOUT=1                    # SRS port-check-er proti attempt-er socket timeout (sec)
    STREAM_ID=<uuid>                       # proti log line e "[stream=...]" hisebe bosbe, r
                                            # worker/relay-er actual push/pull URL-er path/key-o eta-i
    DEBUG=false                            # true dile verbose (DEBUG level) log dekhabe
    RESTART_BACKOFF_BASE=2                 # process crash korle koto sec theke restart backoff shuru hobe
    RESTART_BACKOFF_MAX=60                 # backoff max koto second porjonto barbe

Ctrl+C (SIGINT) dile shob process (worker + push_relay) gracefully terminate hobe.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from logging_setup import setup_logging, set_stream_id

logger = setup_logging("run_all")  # stream_id main() theke set hobe

_shutdown_requested = False


def _strip_inline_comment(value: str) -> str:
    """
    Value quoted thakle (shuru ' ba " diye), quote-er por ja ache
    (trailing " # comment" shoho) ignore kore shudhu quoted part rakhe.
    Quoted na thakle, first unquoted ' #' (space+hash) ba tab+hash theke
    shuru kore baki shob truncate kore -- e.g.
    `RESTART_BACKOFF_BASE=2   # comment` theke shudhu `2` ber hoy.
    Ei fix chara emon inline comment thakle value-er modhye comment text
    shoho dhuke jay, r porer int()/float() cast e crash kore.
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
    Already-set environment variable (shell theke export kora) ke
    override kore na.
    """
    path = Path(env_file_path)
    if not path.exists():
        logger.info(f".env file not found ({env_file_path}), using shell environment only.")
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
            value = _strip_inline_comment(value.strip())

            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]

            os.environ.setdefault(key, value)

    logger.info(f"Loaded environment variables from: {env_file_path}")


def handle_signal(signum, frame):
    global _shutdown_requested
    logger.info(f"Received shutdown signal ({signum}), stopping all processes...")
    _shutdown_requested = True


def _parse_host_port(rtmp_url: str, default_port: int = 1935) -> tuple[str, int]:
    parsed = urlparse(rtmp_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or default_port
    return host, port


def _tcp_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ---------- SRS readiness check (SRS is external -- NOT a child process) ----------
# Ei script SRS ke start/manage kore na (SRS ALADA-vabe, e.g. Docker-e,
# chalano hoy) -- kintu WORKER/RELAY start korar AGE ekbar check kore neওয়া
# hoy SRS-er RTMP port (RTMP_URL theke) actually khola ache kina. Na thakle
# WORKER/RELAY start hoyeo shathe shathe connection-refused e crash-loop e
# porto (SRS up howa porjonto), tai eikhaneই blocking-vabe wait kora hoy --
# jate log-e ekbar-i porishkar bola hoy "SRS up na" (bar bar crash-restart
# log spam er poriborte), r SRS up hওয়া matro shathe shathe egiye jai.
SRS_CHECK_INTERVAL = 2.0    # seconds -- koto ghono ghono port check kora hobe
SRS_CHECK_TIMEOUT = float(os.environ.get("SRS_CHECK_TIMEOUT", "1"))  # per-attempt socket timeout


def wait_for_srs(host: str, port: int) -> bool:
    """
    SRS-er RTMP port khola howa porjonto (ba shutdown request ashar age
    porjonto) blocking-vabe wait kore, proti SRS_CHECK_INTERVAL sec por por
    retry kore. Returns False shudhu shutdown-er karone thamle.
    """
    logged_waiting = False
    while not _shutdown_requested:
        if _tcp_port_open(host, port, timeout=SRS_CHECK_TIMEOUT):
            if logged_waiting:
                logger.info(f"[run_all] SRS is up ({host}:{port}), proceeding.")
            else:
                logger.info(f"[run_all] SRS check passed ({host}:{port}).")
            return True
        if not logged_waiting:
            logger.warning(
                f"[run_all] SRS is not reachable at {host}:{port}. "
                f"Make sure the SRS Docker container is running and its port is "
                f"published (e.g. 'docker run -d --name srs -p 1935:1935 ... ossrs/srs:5'). "
                f"Will keep checking every {SRS_CHECK_INTERVAL:.0f}s..."
            )
            logged_waiting = True
        waited = 0.0
        while waited < SRS_CHECK_INTERVAL and not _shutdown_requested:
            time.sleep(0.2)
            waited += 0.2
    return False


# ---------- Kubernetes health/readiness endpoints ----------
# GET /healthz -> main.py (ei Python process) nijei live ache kina.
# GET /readyz  -> WORKER + RELAY -- duita child process i live/running
#                 kina check kore JSON e prottekta separate status shoho.
#                 Kono ekta down thakle (restart/backoff cholche) 503 dey.
#
# NOTE: SRS (RTMP server) ekhon ei process-er child na (alada Docker
# container e chole), tai /readyz e SRS-er kono status thake na -- shudhu
# WORKER/RELAY. SRS nijer container-er health/liveness Docker/K8s-e
# alada-vabe check korte hobe.
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8081"))


def _start_health_server(processes: dict) -> None:
    class _HealthHandler(BaseHTTPRequestHandler):
        def _write_json(self, code: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/healthz":
                self._write_json(200, {"status": "ok"})
                return
            if self.path == "/readyz":
                statuses = {name: p.is_alive() for name, p in processes.items()}
                ready = all(statuses.values())
                self._write_json(200 if ready else 503, {"ready": ready, "processes": statuses})
                return
            self._write_json(404, {"error": "not found"})

        def log_message(self, format, *args):
            # BaseHTTPRequestHandler default e proti request stderr e log
            # kore -- k8s probe proti few second e ekbar hit korbe, tai eta
            # off kora hocche jate log spam na hoy. Health server nijer
            # kono error (bind fail, etc) eta suppress kore na, shudhu
            # per-request access log.
            pass

    server = ThreadingHTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"[run_all] Health check server listening on :{HEALTH_PORT} (/healthz, /readyz)")


class ManagedProcess:
    """
    Ekta child process (WORKER / RELAY) ke supervise kore -- crash/
    unexpected-exit hole automatic restart kore (exponential backoff
    shoho), jate stream kono karone hothat theme gele manual intervention
    chara nijei abar cholte shuru kore -- eituku i ei supervisor-er
    "production grade reliability" guarantee.
    """

    def __init__(
        self,
        name: str,
        build_cmd,
        cwd: str = None,
        ready_pattern: str = None,
        restart_backoff_base: float = 2.0,
        restart_backoff_max: float = 60.0,
    ):
        self.name = name
        self.build_cmd = build_cmd  # callable -> list[str] (restart e o fresh env/cmd build hoy)
        self.cwd = cwd
        self.ready_pattern = ready_pattern
        self.restart_backoff_base = restart_backoff_base
        self.restart_backoff_max = restart_backoff_max

        self.proc: subprocess.Popen = None
        self.ready_event = threading.Event()
        self.consecutive_failures = 0
        self._thread: threading.Thread = None

    def start(self) -> None:
        cmd = self.build_cmd()
        logger.info(f"[{self.name}] Starting: {' '.join(cmd)}")
        self.ready_event.clear()
        # PYTHONUNBUFFERED=1 -- belt-and-suspenders alongside the "-u" flag
        # on the WORKER/RELAY build_cmd (see those comments): guarantees
        # unbuffered stdout even if a future build_cmd change forgets "-u".
        child_env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        try:
            self.proc = subprocess.Popen(
                cmd,
                cwd=self.cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=child_env,
            )
        except OSError as e:
            # e.g. FileNotFoundError -- the command binary isn't installed/
            # on PATH, or `cwd` doesn't exist. Without this catch, this
            # exception would propagate all the way out of main() and crash
            # the whole launcher -- and nothing supervises main.py itself,
            # so that would be a hard stop, not just a restart. Instead we
            # log clearly and leave self.proc as None, so is_alive() reports
            # False and the normal backoff/retry loop in main() handles it
            # the same as any other startup failure.
            logger.error(f"[{self.name}] Failed to start '{' '.join(cmd)}': {e}")
            self.proc = None
            return
        self._thread = threading.Thread(target=self._stream_output, daemon=True)
        self._thread.start()

        if not self.ready_pattern:
            # Kono readiness marker deওয়া na thakle, process start hওয়াটাকেই
            # "ready" dhore newa hoy -- kono fixed sleep lagbe na.
            self.ready_event.set()

    def _stream_output(self) -> None:
        try:
            for raw_line in iter(self.proc.stdout.readline, b""):
                if not raw_line:
                    break
                line = raw_line.decode("utf-8", errors="ignore").rstrip()
                if line:
                    logger.info(f"[{self.name}] {line}")
                    if self.ready_pattern and self.ready_pattern in line:
                        self.ready_event.set()
        except Exception:
            pass

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def wait_ready(self, timeout: float, poll_interval: float = 0.2) -> bool:
        """
        Process ready howar jonno wait kore -- ready marker (log line)
        dekha gele shathe shathe True (1-2 sec e o hote pare). Process
        nijei tar age crash kore gele, r baki shomoy uselessly wait na
        kore shathe shathe False return kore. Timeout hoye gele-o
        process beche thakle fallback hisebe True dhore continue kora
        hoy (log-e warning shoho), jate slow-startup e pura pipeline
        permanently আটকে na thake.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if _shutdown_requested:
                return False
            if not self.is_alive():
                return False
            if self.ready_event.wait(poll_interval):
                return True

        if not self.is_alive():
            return False

        logger.warning(
            f"[{self.name}] No readiness marker seen within {timeout}s, "
            f"but process is still alive -- proceeding anyway (fallback)."
        )
        return True

    # 2**n er exponent-er upor ekta hard ceiling -- delay eto age-i
    # restart_backoff_max e cap hoye jay (e.g. base=2 hole 2**10*2 already
    # 2048, kono normal restart_backoff_max-er cheye onek beshi) je exponent
    # ke arো barano shudhu-i wasted computation. Eta chara, kono ekta
    # process (e.g. bad config, permanently-down endpoint) mash-er por
    # mash fail korte thakle self.consecutive_failures unboundedly barte
    # thake (restart cycle proti ekbar increment -- ei plain-int increment
    # nijei cheap, kono somossha na), kintu 2**self.consecutive_failures
    # ekta astronomically boro integer hoye jay -- shudhu min() diye felar
    # jonno proti restart e ei bignum compute kora CPU/memory-r niredik
    # opocoy. Tai raw counter (log-e accurate count dekhanor jonno) rekhe,
    # SHUDHU exponent hisebe use howar age take cap kora hocche.
    _MAX_BACKOFF_EXPONENT = 12

    def note_failure_and_backoff(self) -> None:
        self.consecutive_failures += 1
        capped_exponent = min(self.consecutive_failures - 1, self._MAX_BACKOFF_EXPONENT)
        delay = min(
            self.restart_backoff_base * (2 ** capped_exponent),
            self.restart_backoff_max,
        )
        logger.warning(
            f"[{self.name}] consecutive failure #{self.consecutive_failures}, "
            f"restarting in {delay:.1f}s..."
        )
        waited = 0.0
        while waited < delay and not _shutdown_requested:
            time.sleep(0.5)
            waited += 0.5

    def note_success(self) -> None:
        self.consecutive_failures = 0

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
            except Exception:
                pass

    def wait_exit(self, timeout: float) -> None:
        if self.proc:
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                try:
                    self.proc.kill()
                except Exception:
                    pass


def main():
    here = Path(__file__).resolve().parent

    load_env_file(str(here / ".env"))

    # Console/stdout-only logging -- a container's own log collector
    # (Fluent Bit / Loki / CloudWatch etc, or `docker logs` locally) is
    # expected to capture stdout instead of this process writing files.
    setup_logging("run_all")
    set_stream_id("run_all", os.environ.get("STREAM_ID", "-"))

    worker_ready_timeout = float(os.environ.get("WORKER_STARTUP_WAIT", "5"))

    restart_backoff_base = float(os.environ.get("RESTART_BACKOFF_BASE", "2"))
    restart_backoff_max = float(os.environ.get("RESTART_BACKOFF_MAX", "60"))

    # RTMP_URL ekhon worker/relay nijeder push/pull URL banate use hoy, AR
    # eikhaneo (SRS readiness check-er jonno) shudhu host:port ber kora hoy --
    # path/stream-id ta matter kore na ei check-er jonno.
    rtmp_url = os.environ.get("RTMP_URL", "rtmp://localhost:1935/live/")
    srs_host, srs_port = _parse_host_port(rtmp_url)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # WORKER start korar age SRS up ache kina check kore neওয়া hoy (blocking,
    # SIGINT/SIGTERM e break-out kore) -- dekho wait_for_srs()-er comment.
    logger.info(f"[run_all] Checking SRS at {srs_host}:{srs_port} before starting WORKER...")
    if not wait_for_srs(srs_host, srs_port):
        logger.info("[run_all] Shutdown requested before SRS became reachable, exiting.")
        return

    worker = ManagedProcess(
        "WORKER",
        # "-u": stdout ekhon subprocess.PIPE e redirect hocche (TTY na), tai
        # Python default e block-buffered hoye jay -- "Pushing:" log line
        # print hoyeo onek shomoy dhore parent (main.py) er kache pouchay na
        # (buffer bhorা na porjonto), fole wait_ready() proti bar timeout
        # hoye "fallback" warning dey r merged console log delayed batches e
        # ashe. "-u" diye Python ke unbuffered stdout/stderr e force kora
        # hocche, jate readiness marker r shob log line sathe sathe (near
        # real-time) main.py porjonto pouchay.
        build_cmd=lambda: [sys.executable, "-u", "stream_worker_stdin.py"],
        cwd=str(here),
        ready_pattern="Pushing:",  # first segment publish shuru howar log line
        restart_backoff_base=restart_backoff_base,
        restart_backoff_max=restart_backoff_max,
    )
    relay = ManagedProcess(
        "RELAY",
        # "-u" -- see WORKER comment above; same buffering issue applies
        # here (no ready_pattern for RELAY, but its regular log lines would
        # otherwise arrive in delayed chunks in the merged console output).
        build_cmd=lambda: [sys.executable, "-u", "push_relay.py"],
        cwd=str(here),
        restart_backoff_base=restart_backoff_base,
        restart_backoff_max=restart_backoff_max,
    )

    order = ["WORKER", "RELAY"]
    processes = {"WORKER": worker, "RELAY": relay}

    # Health server ke shob-er age start kori (proti child process start
    # howar age-i) -- tai k8s startupProbe/livenessProbe /healthz e 200
    # pabe process start howar shathe shathei, r /readyz thakbe 503 jotokkhon
    # na WORKER+RELAY duitai actually up hoy (accurate readiness signal).
    _start_health_server(processes)

    def start_and_wait_worker() -> bool:
        worker.start()
        ready = worker.wait_ready(worker_ready_timeout)
        return worker.is_alive() and ready is not False

    def start_and_wait_relay() -> bool:
        relay.start()
        return relay.is_alive()

    starters = {"WORKER": start_and_wait_worker, "RELAY": start_and_wait_relay}

    # ---------- Guaranteed startup ----------
    # Proti step retry hote thake jotokkhon na successful hoy (ba Ctrl+C
    # ashe) -- tai kono transient error/misconfiguration e main.py
    # nijei permanently exit kore na, nijei abar try kore.
    for name in order:
        while not _shutdown_requested:
            if starters[name]():
                logger.info(f"[run_all] {name} started successfully.")
                processes[name].note_success()
                break
            processes[name].note_failure_and_backoff()
        if _shutdown_requested:
            break

    # ---------- Ongoing supervision (production reliability guarantee) ----------
    # Proti process ke independently monitor kori -- jekono ekta crash
    # korle shudhu shetake restart kora hoy (backoff shoho), baki gulo
    # unaffected thake. Loop 0.5s e ekbar check kore, tai crash howar
    # sathe sathe (near-instant) dhora pore.
    try:
        while not _shutdown_requested:
            for name in order:
                if _shutdown_requested:
                    break
                p = processes[name]
                if not p.is_alive():
                    ret = p.proc.poll() if p.proc else None
                    logger.warning(f"[run_all] {name} process exited (code={ret}), restarting...")
                    p.note_failure_and_backoff()
                    if _shutdown_requested:
                        break
                    if starters[name]():
                        p.note_success()
                        logger.info(f"[run_all] {name} restarted successfully.")
                    else:
                        logger.warning(f"[run_all] {name} restart attempt also crashed, will retry again.")
            time.sleep(0.5)
    finally:
        logger.info("[run_all] Shutting down, terminating all processes...")
        for p in processes.values():
            p.stop()
        deadline = time.time() + 10
        for p in processes.values():
            remaining = max(0, deadline - time.time())
            p.wait_exit(remaining)
        logger.info("[run_all] Done.")


if __name__ == "__main__":
    main()