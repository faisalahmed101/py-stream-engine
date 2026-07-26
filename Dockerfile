# syntax=docker/dockerfile:1

# ============================================================
# Single image running the whole pipeline via main.py:
#   - Node.js  -> node-media-server (spawned as a child process by main.py)
#   - ffmpeg   -> used directly by stream_worker_stdin.py and push_relay.py
#   - Python 3 -> main.py / stream_worker_stdin.py / push_relay.py
#                 (stdlib only -- no requirements.txt needed, see below)
# ============================================================
FROM python:3.12-slim

# --- System dependencies ---
# ffmpeg      -- required by stream_worker_stdin.py (stdin -> RTMP push)
#                and push_relay.py (pull -> per-destination RTMP push).
# nodejs/npm  -- required to run node-media-server.js.
# curl        -- used only by the HEALTHCHECK instruction below.
# (single RUN + apt list cleanup in the same layer to keep the image small)
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        nodejs \
        npm \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- Python side ---
# No external pip dependencies -- main.py, stream_worker_stdin.py,
# push_relay.py, supabase_client.py, and logging_setup.py all use only the
# Python standard library (urllib, json, threading, subprocess, etc.), so
# there's intentionally no requirements.txt / pip install step here.
COPY logging_setup.py supabase_client.py main.py stream_worker_stdin.py push_relay.py ./

# --- Node-Media-Server ---
# Dependencies are installed fresh at build time (npm ci), not copied
# from the host's node_modules/ -- keeps the build reproducible and
# avoids host OS/arch mismatches (e.g. building on macOS, running on
# Linux). package.json/package-lock.json are copied first (separately
# from the rest of the source) so Docker's layer cache can skip
# `npm ci` on rebuilds where only application code changed, not
# dependencies. If you don't have a package-lock.json, replace
# `npm ci` with `npm install` below.
COPY node-media-server/package*.json ./node-media-server/
RUN cd node-media-server && npm ci --omit=dev
COPY node-media-server/ ./node-media-server/

# --- Run as non-root ---
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# --- Config ---
# Real values (STREAM_ID, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, etc.)
# are injected at deploy time via a k8s ConfigMap/Secret or --env-file /
# -e flags -- see env_reference.txt for the full list and which values
# are sensitive. No .env file is baked into this image on purpose.
ENV HEALTH_PORT=8081 \
    NMS_DIR=node-media-server \
    PYTHONUNBUFFERED=1

# Liveness/readiness probe port (main.py's /healthz + /readyz)
EXPOSE 8081
# Uncomment if something outside this container needs to publish/pull
# RTMP directly against node-media-server (default RTMP_URL port):
# EXPOSE 1935

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS "http://localhost:${HEALTH_PORT}/healthz" || exit 1

CMD ["python3", "main.py"]