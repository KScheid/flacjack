# ==============================================================================
# Stage 1: Build stage (compile wheels & install dependencies)
# ==============================================================================
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build essentials
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# ==============================================================================
# Stage 2: Final runtime image (Optimized for Docker & Unraid)
# ==============================================================================
FROM python:3.12-slim

LABEL maintainer="hifi-bridge"
LABEL description="Self-Hosted Hi-Fi Lossless Aggregator & BitChord Source Module"

# Install ffmpeg for audio handling, curl for health checks, gosu for Unraid PUID/PGID support
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    ca-certificates \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Default Unraid nobody:users mapping is 99:100
RUN groupadd -g 100 appgroup 2>/dev/null || true && \
    useradd -u 99 -g 100 -s /bin/bash -m appuser 2>/dev/null || true

WORKDIR /app

# Copy installed Python packages from builder
COPY --from=builder /root/.local /home/appuser/.local
COPY --from=builder /root/.local /root/.local
ENV PATH=/home/appuser/.local/bin:/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Create persistent storage directories
RUN mkdir -p /data/audio_cache /music /app && \
    chown -R 99:100 /data /music /app /home/appuser

# Copy application code and entrypoint
COPY app /app/app
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Default environment configuration
ENV PORT=8742
ENV HOST=0.0.0.0
ENV PUID=99
ENV PGID=100
ENV DEFAULT_QUALITY=HI_RES_LOSSLESS
ENV ENABLE_FLAC_ARCHIVE=false
ENV CACHE_TTL_SECONDS=3600
ENV DB_PATH=/data/hifi_bridge.db
ENV AUDIO_CACHE_DIR=/data/audio_cache
ENV MAX_CACHE_GB=10
ENV PREFETCH_LOOKAHEAD=2
ENV PREFETCH_ENABLED=true
ENV MUSIC_DIR=/music

EXPOSE 8742

# Healthcheck configuration
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8742/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8742"]
