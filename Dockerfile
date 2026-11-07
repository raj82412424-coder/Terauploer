FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \\
    PYTHONUNBUFFERED=1 \\
    PIP_DISABLE_PIP_VERSION_CHECK=1 \\
    DATA_DIR=/app/data \\
    LOG_WEB_HOST=0.0.0.0

WORKDIR /app

# ffmpeg is needed by yt-dlp when a URL has separate audio/video streams.
RUN apt-get update \\
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg \\
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./
RUN useradd --create-home --uid 10001 botuser \\
    && mkdir -p /app/data \\
    && chown -R botuser:botuser /app
USER botuser

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \\
    CMD python -c "import os,urllib.request; r=urllib.request.Request('http://127.0.0.1:8080/health', headers={'X-Log-Token': os.getenv('LOG_WEB_TOKEN','')}); urllib.request.urlopen(r, timeout=3).read()"

ENTRYPOINT ["python", "bot.py"]
