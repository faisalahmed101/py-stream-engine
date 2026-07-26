#!/usr/bin/env python3
"""
supabase_client.py

Common Supabase REST (PostgREST) helpers -- shared by stream_worker_stdin.py
(playlist load) r push_relay.py (destination load + status reporting), jate
duijon script e ekই GET/PATCH/retry logic duibar likhte na hoy (age duijon
scripte almost identical copy hoyeche, ekhon ekhaneই centralize kora hocche).

Stdlib urllib-only, kono extra pip dependency (supabase-py/postgrest-py)
lagbe na.

Env vars (proti script nijer .env theke load kore, tarpor ei module import
hobar shomoy os.environ.get() diye poRe -- tai load_env_file() shobar AGE
call korte hobe caller script e, exactly age jemon hoto):
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY   -- RLS bypass kore, tai eta SHUDHU
                                    server-side/backend e rakho
"""

import json
import os
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

# DB unreachable/misconfigured obostay koto second por por retry hobe --
# ffmpeg-missing-binary er moto fixed interval (exponential na, karon eta
# kono hot crash-loop na, shudhu "startup/reload e ekbar DB theke data
# dorkar" er jonno wait).
SUPABASE_RETRY_INTERVAL_SECONDS = 15


class SupabaseFetchError(Exception):
    """
    Raised when a Supabase REST call itself fails -- network error, HTTP
    error status, timeout, or an unexpected/unparseable response body.
    Distinct from ValueError (a *config/data* problem: missing env vars,
    row not found, empty result) -- both are typically retried identically
    by the caller, but kept as separate exception types so the log message
    stays accurate about *why* the fetch didn't work.
    """
    pass


def is_configured() -> bool:
    """SUPABASE_URL r SUPABASE_SERVICE_ROLE_KEY dutoi set ache kina."""
    return bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)


def supabase_get(path: str, params: dict) -> list:
    """
    Minimal Supabase PostgREST GET helper.

    Uses the service-role key (SUPABASE_SERVICE_ROLE_KEY), which bypasses
    Row Level Security -- appropriate here since this is a trusted backend
    worker reading its own assigned stream's data, not an end-user client.
    This key must never be exposed to a browser/client.
    """
    query = urlencode(params)
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{path}?{query}"
    req = urllib.request.Request(
        url,
        headers={
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
    except (urllib.error.URLError, TimeoutError) as e:
        # urllib.error.HTTPError is a subclass of URLError, so 4xx/5xx
        # responses (e.g. bad api key -> 401, RLS/table issue -> 403/404)
        # are also caught here.
        raise SupabaseFetchError(f"Supabase GET '{path}' failed: {e}")

    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        raise SupabaseFetchError(f"Supabase response for '{path}' was not valid JSON: {e}")

    if not isinstance(data, list):
        raise SupabaseFetchError(f"Unexpected Supabase response shape for '{path}': {data!r}")
    return data


def supabase_patch(path: str, params: dict, body: dict, logger=None) -> None:
    """
    Best-effort Supabase PostgREST PATCH -- used for status reporting
    (e.g. stream_destinations.status/error_message/started_at/ended_at).

    Deliberately swallows all errors (just logs a warning if a logger is
    given): a failed status update must NEVER interrupt the actual
    ffmpeg push/relay, since these rows are purely for observability/UI,
    not for the caller's own control flow.
    """
    if not is_configured():
        return
    query = urlencode(params)
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{path}?{query}"
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="PATCH",
        headers={
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        if logger is not None:
            logger.warning("Supabase status update on '%s' failed (non-fatal): %s", path, e)


def wait_for_supabase(fetch_fn, *, description: str, shutdown_check, logger,
                       retry_interval: float = SUPABASE_RETRY_INTERVAL_SECONDS):
    """
    Generic "keep retrying a Supabase fetch until it succeeds or shutdown
    is requested" wrapper -- mirrors the fixed-interval retry pattern used
    for a missing ffmpeg binary elsewhere in this project (not exponential,
    since this isn't a hot crash-loop, just a "data isn't ready/reachable
    yet" wait).

    fetch_fn:       zero-arg callable that returns the desired result, or
                     raises (ValueError, SupabaseFetchError) if not ready.
    description:    log message prefix, e.g. "Could not load playlist for
                     stream_id=abc123".
    shutdown_check: zero-arg callable returning True if the caller wants to
                     stop waiting (e.g. `lambda: _shutdown_requested`).
    logger:         logger to report retries on.
    retry_interval: seconds between attempts (default: SUPABASE_RETRY_INTERVAL_SECONDS).

    Both ValueError (bad/missing data) and SupabaseFetchError (transient
    network/DB issue) are retried identically -- from the caller's point of
    view "DB is down" and "DB says nothing is ready yet" both just mean
    "not ready, keep waiting". Returns None only if shutdown was requested
    while waiting.
    """
    attempt = 0
    while not shutdown_check():
        try:
            return fetch_fn()
        except (ValueError, SupabaseFetchError) as e:
            attempt += 1
            logger.error(
                "%s (attempt %d): %s -- retrying in %.0fs...",
                description, attempt, e, retry_interval,
            )
            waited = 0.0
            while waited < retry_interval and not shutdown_check():
                time.sleep(0.5)
                waited += 0.5
    return None