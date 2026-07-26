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
    2. NMS-er RTMP port (RTMP_URL theke) bind hওয়া porjonto active
       poll kora hoy (fixed sleep na -- 1-2 sec e ready hoye gele
       shathe shathe egiye jay).
    3. stream_worker_stdin.py start hoy (video source -> NMS e publish)
    4. Worker-er log e "Push hocche:" (first segment publish shuru
       howar marker) dekha porjonto active wait kora hoy.
    5. push_relay.py start hoy (NMS theke pull kore YouTube/Facebook e push)

Production-grade reliability:
    - Proti child process (NMS / WORKER / RELAY) ke ALADA-ALADA vabe
      supervise kora hoy. Jekono ekta process jekono karone (crash,
      unhandled exception, kill -9, OOM) exit/theme gele, shudhu
      shetake exponential backoff shoho automatic restart kora hoy --
      baki process gulo unaffected thake (proti process nijer moddhe
      already resilient: e.g. push_relay/stream_worker nijeder ffmpeg
      subprocess crash nijera i handle kore, main.py shudhu porar
      Python process nijei crash korle top-level e restart kore).
    - Startup-eo guarantee deওয়া hoy -- kono step (NMS/WORKER/RELAY)
      shuru hotei fail korle main.py exit kore na, backoff shoho retry
      korte thake jotokkhon na successful hoy ba Ctrl+C আসে.

Usage:
    python3 main.py

Config (.env file theke, ba environment variable diye override):
    NMS_DIR=node-media-server              # node-media-server.js jei subfolder e ache (relative, main.py er tulonay)
    NMS_SCRIPT=node-media-server.js        # Node-Media-Server entry file naam
    NMS_STARTUP_WAIT=8                     # NMS port bind howar jonno max wait (safety-net, blind sleep na)
    WORKER_STARTUP_WAIT=5                  # worker "ready" howar jonno max wait (safety-net, blind sleep na)
    RTMP_URL=rtmp://localhost:1935/live/   # BASE URL (STREAM_NAME ar lagbe na -- STREAM_ID-i ekhon
                                            # push/pull path e boshe: base + '/' + STREAM_ID). Ei
                                            # variable theke shudhu host:port ber kora hoy (NMS
                                            # readiness check-er jonno) -- path/stream-id ta matter
                                            # kore na ei check-er jonno.
    STREAM_ID=<uuid>                       # proti log line e "[stream=...]" hisebe bosbe, r
                                            # worker/relay-er actual push/pull URL-er path/key-o eta-i
    DEBUG=false                            # true dile verbose (DEBUG level) log dekhabe
    RESTART_BACKOFF_BASE=2                 # process crash korle koto sec theke restart backoff shuru hobe
    RESTART_BACKOFF_MAX=60                 # backoff max koto second porjonto barbe

Ctrl+C (SIGINT) dile shob process (NMS + worker + push_relay) gracefully terminate hobe.
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


def wait_for_port(host: str, port: int, timeout: float, proc: subprocess.Popen = None, poll_interval: float = 0.2) -> bool:
    """
    Fixed sleep er poriborte, host:port e actual socket connect kore
    dekhe -- port taratari (1-2 sec e o) bind hoye gele shathe shathe
    True return kore, r na hole max `timeout` second porjonto try kore
    (eta ekta upper-bound safety net, blind wait na).

    `proc` deওয়া thakle, proti iteration e process beche ache kina
    check kora hoy -- process nijei crash kore gele r baki shomoy
    uselessly wait na kore shathe shathe False return kore, jate
    caller taratari retry/restart korte pare.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _shutdown_requested:
            return False
        if proc is not None and proc.poll() is not None:
            return False
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(poll_interval)
    return False


def _parse_host_port(rtmp_url: str, default_port: int = 1935) -> tuple[str, int]:
    parsed = urlparse(rtmp_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or default_port
    return host, port


# ---------- Kubernetes health/readiness endpoints ----------
# Ekta lightweight HTTP server (stdlib http.server -- kono extra pip/npm
# dependency lagbe na, tai Docker image e kichu add korte hobe na) jate
# k8s Pod-er liveness/readiness probe check korte pare. main.py e-i thake
# (NMS/WORKER/RELAY er moto alada process na) karon eituku i "single
# container" design e sob theke shohoj jayga -- ei ekta process i shob
# child process ke track kore.
#
#   GET /healthz -> shudhu eituku bole je main.py (ei Python process) nijei
#                   live ache (event loop hang/deadlock na hole always 200).
#                   Eta livenessProbe e use koro:
#
#                       livenessProbe:
#                         httpGet: {path: /healthz, port: 8081}
#                         initialDelaySeconds: 15
#                         periodSeconds: 10
#
#   GET /readyz  -> NMS + WORKER + RELAY -- tinta child process i live/
#                   running kina check kore JSON e prottekta separate
#                   status shoho. Kono ekta down thakle (restart/backoff
#                   cholche) 503 dey. Eta readinessProbe e use koro:
#
#                       readinessProbe:
#                         httpGet: {path: /readyz, port: 8081}
#                         initialDelaySeconds: 15
#                         periodSeconds: 5
#                         failureThreshold: 3
#
# NOTE (important, Deployment manifest e mathay rekho):
#   - Eta horizontally scale kora jaবে na -- ekta Pod = ekta stream
#     pipeline (ekta ffmpeg push). Deployment e `replicas: 1` rakho,
#     naile duita Pod same RTMP destination e duibar push korbe.
#   - RESTART_BACKOFF_MAX (default 60s) porjonto delay lagte pare kono
#     child process restart korte, r SIGTERM ashar por ffmpeg/node ke
#     clean-e bondho hote (5-10s) shomoy lage. Tai Pod spec e
#     `terminationGracePeriodSeconds: 30` (ba beshi) rakho, noile
#     graceful shutdown shesh howar age SIGKILL chole ashte pare.
#   - HEALTH_PORT env var diye port change kora jay (default 8081) --
#     Deployment/Service e containerPort/probe port eর shathe match
#     korte hobe.
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
    Ekta child process (NMS / WORKER / RELAY) ke supervise kore --
    crash/unexpected-exit hole automatic restart kore (exponential
    backoff shoho), jate stream kono karone hothat theme gele manual
    intervention chara nijei abar cholte shuru kore -- eituku i ei
    supervisor-er "production grade reliability" guarantee.
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
        # Harmless/no-op for NMS (node), since node doesn't read this var.
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
            # e.g. FileNotFoundError -- the command binary (like "node")
            # isn't installed/on PATH, or `cwd` doesn't exist. Without this
            # catch, this exception would propagate all the way out of
            # main() and crash the whole launcher -- and nothing supervises
            # main.py itself, so that would be a hard stop, not just a
            # restart. Instead we log clearly and leave self.proc as None,
            # so is_alive() reports False and the normal backoff/retry loop
            # in main() handles it the same as any other startup failure.
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

    nms_dir_rel = os.environ.get("NMS_DIR", ".")   # node-media-server.js jei subfolder e ache
    nms_script = os.environ.get("NMS_SCRIPT", "node-media-server.js")
    nms_dir = (here / nms_dir_rel).resolve()

    # Eta ekhon blind sleep na, upper-bound safety net -- actual wait
    # onek kom shomoy-eo (1-2 sec) shesh hote pare.
    nms_ready_timeout = float(os.environ.get("NMS_STARTUP_WAIT", "8"))
    worker_ready_timeout = float(os.environ.get("WORKER_STARTUP_WAIT", "5"))

    restart_backoff_base = float(os.environ.get("RESTART_BACKOFF_BASE", "2"))
    restart_backoff_max = float(os.environ.get("RESTART_BACKOFF_MAX", "60"))

    rtmp_url = os.environ.get("RTMP_URL", "rtmp://localhost:1935/live/")
    nms_host, nms_port = _parse_host_port(rtmp_url)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    nms = ManagedProcess(
        "NMS",
        build_cmd=lambda: ["node", nms_script],
        cwd=str(nms_dir),
        restart_backoff_base=restart_backoff_base,
        restart_backoff_max=restart_backoff_max,
    )
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

    order = ["NMS", "WORKER", "RELAY"]
    processes = {"NMS": nms, "WORKER": worker, "RELAY": relay}

    # Health server ke shob-er age start kori (proti child process start
    # howar age-i) -- tai k8s startupProbe/livenessProbe /healthz e 200
    # pabe process start howar shathe shathei, r /readyz thakbe 503 jotokkhon
    # na shob child process actually up hoy (accurate readiness signal).
    _start_health_server(processes)

    def start_and_wait_nms() -> bool:
        nms.start()
        if not nms.is_alive():
            return False
        logger.info(f"[run_all] Waiting for NMS port {nms_host}:{nms_port} to bind (max {nms_ready_timeout}s)...")
        ready = wait_for_port(nms_host, nms_port, nms_ready_timeout, proc=nms.proc)
        if not nms.is_alive():
            return False
        if ready:
            logger.info("[run_all] NMS port ready, proceeding.")
        else:
            logger.warning(f"[run_all] NMS port not ready after {nms_ready_timeout}s, process is still alive -- proceeding anyway (fallback).")
        return True

    def start_and_wait_worker() -> bool:
        worker.start()
        ready = worker.wait_ready(worker_ready_timeout)
        return worker.is_alive() and ready is not False

    def start_and_wait_relay() -> bool:
        relay.start()
        return relay.is_alive()

    starters = {"NMS": start_and_wait_nms, "WORKER": start_and_wait_worker, "RELAY": start_and_wait_relay}

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