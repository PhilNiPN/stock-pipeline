FROM python:3.11-slim AS base

# enviroment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# working directory
WORKDIR /opt/app

# System deps (build tools + curl)
RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
        build-essential gcc curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# App code
COPY app /opt/app

# Keep container alive by default (development friendly setup)
CMD ["bash", "-lc", "echo 'ETL container ready. Run: docker compose exec etl python run_pipeline.py' && tail -f /dev/null"]