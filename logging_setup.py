#!/usr/bin/env python3
"""
logging_setup.py

Centralized, production-grade logging configuration -- shared by
main.py, stream_worker_stdin.py, push_relay.py.

Console/stdout-only logging (no file logging / rotation) -- a container's
own log collector (Fluent Bit / Loki / CloudWatch / etc, or `docker logs`
locally) is expected to capture stdout instead.

Features:
- DEBUG env var toggle:
      DEBUG=true  -> DEBUG level (shob log dekhabe, verbose)
      DEBUG na thakle / false -> INFO level (shudhu dorkari log)
  LOG_LEVEL env var diye direct override o kora jay (e.g. LOG_LEVEL=WARNING),
  eta DEBUG flag er cheye priority pabe.
- Console handler.
- Extension hook (`attach_handler`) -- pore kono remote log-watch
  service (Loki, Datadog, custom HTTP log shipper, ELK, etc) add
  korte chaile, notun logging.Handler subclass banaiye ei function
  diye attach kora jabe -- existing script (main.py/push_relay.py/
  stream_worker_stdin.py) e ekta line-o change korte hobe na.

Usage (proti script e):
    from logging_setup import setup_logging
    logger = setup_logging("stream_01")
    logger.info("kichu ekta message")
    logger.debug("shudhu DEBUG=true thakle dekhabe")

    # Multi-stream setup e (.env theke STREAM_ID/STREAM_NAME nile), proti
    # log line e automatic stream id bosbe -- alada kore prottek
    # logger.info(...) call e extra={} dite hobe na:
    logger = setup_logging("stream_worker", stream_id=os.environ.get("STREAM_NAME"))
    logger.error("ffmpeg crash hoyeche")
    # -> 2026-07-26 10:00:00 [ERROR] [stream_worker] [stream=stream_01] ffmpeg crash hoyeche

Env vars:
    DEBUG=true|false          -- verbose logging on/off (default: false)
    LOG_LEVEL=DEBUG|INFO|...  -- explicit override, DEBUG flag ke beat kore
    STREAM_ID / STREAM_NAME   -- setup_logging() e stream_id explicitly na
                                 dile, ei env var gulo theke fallback kora hoy
"""

import logging
import os
import sys
from typing import Optional

_LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] [stream=%(stream_id)s] %(message)s"


def _safe_console_stream():
    """
    Defensive helper: even though log messages are now written in plain
    English/ASCII, this guards against any future stray non-ASCII text
    (e.g. a video title, an external API error string) crashing the
    logger with UnicodeEncodeError on a non-UTF-8 console (this is the
    common cause of "--- Logging error ---" spam on Windows cmd/PowerShell
    with legacy codepages). Safe no-op on consoles that already support
    UTF-8 (e.g. Ubuntu/production).
    """
    stream = sys.stdout
    try:
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
    except Exception:
        pass
    return stream


class _StreamIdFilter(logging.Filter):
    """
    Proti log record e automatic 'stream_id' field bosiye dey, jate
    logger.info("...") shorashori call korleo (extra={} na diyeo)
    prottek log line e stream id thake -- multiple stream ekshathe
    chalale kon stream e problem hocche shathe shathe bojha jay.
    """

    def __init__(self, stream_id: str):
        super().__init__()
        self.stream_id = stream_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.stream_id = self.stream_id
        return True


def _str_to_bool(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "on")


def _resolve_level() -> int:
    """
    Priority: LOG_LEVEL (explicit) > DEBUG flag > default INFO.
    """
    explicit = os.environ.get("LOG_LEVEL")
    if explicit:
        level = logging.getLevelName(explicit.strip().upper())
        if isinstance(level, int):
            return level

    debug_flag = os.environ.get("DEBUG", "")
    return logging.DEBUG if _str_to_bool(debug_flag) else logging.INFO


def setup_logging(
    name: str,
    stream_id: Optional[str] = None,
) -> logging.Logger:
    """
    Ekta named logger configure kore return kore (console/stdout-only).

    stream_id: proti log line e "[stream=...]" hisebe bosbe. Explicitly
        na dile, environment theke STREAM_ID -> STREAM_NAME fallback
        kore try kora hoy; kothaoi na paile "-" bosbe (missing bola jabe,
        kintu crash korbe na).

    Root logger touch kora hoy na (propagate=False) -- tai main.py,
    push_relay.py, stream_worker_stdin.py ek shathe import/run hole
    ekjon onnojon er handler duplicate/conflict kore na, kintu shobar
    level logic ei ekই jaygay theke ashe (DRY, consistent).

    Already-configured logger (e.g. accidental double setup_logging
    call) hole existing handlers-e notun kore add kora hoy na --
    duplicate log line thekano jay. Tobe stream_id notun kore dile
    (e.g. .env theke pore stream id read kora hoyeche), purono filter
    shoriye notun stream_id diye update kora hoy.
    """
    resolved_stream_id = (
        stream_id
        or os.environ.get("STREAM_ID")
        or os.environ.get("STREAM_NAME")
        or "-"
    )

    logger = logging.getLogger(name)
    logger.setLevel(_resolve_level())
    logger.propagate = False

    # Purono _StreamIdFilter thakle shoriye felo, jate stream_id refresh
    # (e.g. .env re-load er por notun stream id) update hote pare.
    logger.filters = [f for f in logger.filters if not isinstance(f, _StreamIdFilter)]
    logger.addFilter(_StreamIdFilter(resolved_stream_id))

    if logger.handlers:
        return logger

    formatter = logging.Formatter(_LOG_FORMAT)

    console_handler = logging.StreamHandler(_safe_console_stream())
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def set_stream_id(name: str, stream_id: str) -> None:
    """
    Runtime e stream id update korar jonno helper -- e.g. .env theke
    stream id porer dike load hoy emon case e (worker start howar
    shomoy .env e STREAM_ID na thakle, pore load hoile) eta call
    korle notun log line gulo update kora stream_id shoho jabe.
    """
    logger = logging.getLogger(name)
    logger.filters = [f for f in logger.filters if not isinstance(f, _StreamIdFilter)]
    logger.addFilter(_StreamIdFilter(stream_id))


def attach_handler(name: str, handler: logging.Handler) -> None:
    """
    Extension hook -- pore kono remote log-watch/monitoring service
    use korte chaile (Loki, Datadog, Sentry, custom HTTP shipper),
    ekta custom logging.Handler subclass banaiye ei function diye
    already-running logger e dynamically attach kora jay. Existing
    script gulo e kono change lagbe na, shudhu ei function call korle
    hobe (e.g. ekta chhoto separate "enable_remote_logging.py" script
    theke, ba future e main.py startup e conditionally).

    Example (future use, ekhon implement kora hoyni, just hook):

        class RemoteLogHandler(logging.Handler):
            def __init__(self, watch_url: str):
                super().__init__()
                self.watch_url = watch_url

            def emit(self, record):
                try:
                    payload = self.format(record)
                    # requests.post(self.watch_url, data=payload, timeout=2)
                except Exception:
                    pass  # log shipping fail korle mূল process ke block na kora

        if os.environ.get("LOG_WATCH_URL"):
            remote = RemoteLogHandler(os.environ["LOG_WATCH_URL"])
            remote.setLevel(logging.WARNING)  # shudhu WARNING+ pathao, shob na
            attach_handler("stream_01", remote)
    """
    logger = logging.getLogger(name)
    if not any(type(h) is type(handler) for h in logger.handlers):
        logger.addHandler(handler)


def set_level(name: str, level: int) -> None:
    """
    Runtime e kono logger-er level change korar jonno helper --
    e.g. SIGHUP reload signal handler theke DEBUG on/off toggle
    korte chaile eta call kora jay, script restart na kore.
    """
    logging.getLogger(name).setLevel(level)