FROM python:3.11-slim

# ffmpeg -- stream_worker_stdin.py এবং push_relay.py দুটোই ffmpeg subprocess
# spawn করে। Node.js/npm আর দরকার নেই (NMS বাদ, SRS এখন sidecar container)।
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY stream_worker_stdin.py .
COPY push_relay.py .
COPY logging_setup.py .
COPY supabase_client.py .

# HEALTH_PORT default -- main.py serves /healthz, /readyz here
EXPOSE 8081

CMD ["python3", "-u", "main.py"]