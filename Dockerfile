# Python 3.11 is the broadest compatible baseline for the Telegram stack.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg aria2 ca-certificates curl unzip \
    && rm -rf /var/lib/apt/lists/*

# Bun is yt-dlp's preferred JavaScript runtime for YouTube's challenge solver
# (see bot_downloader.py); without it, YouTube extraction degrades to
# "no JavaScript runtime" warnings/failures even though the Python
# dependencies installed cleanly. Installed to the default /root/.bun
# location so it matches the BUN_PATH example in sample_config.env.
RUN curl -fsSL https://bun.sh/install | bash \
    && ln -s /root/.bun/bin/bun /usr/local/bin/bun
ENV PATH="/root/.bun/bin:${PATH}"

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY *.py ./
COPY *.sh ./
COPY sample_config.env ./

RUN mkdir -p /data/ytdlbot && chmod 700 /data/ytdlbot

ENV WORK_DIR=/data/ytdlbot \
    FILE_STORE_DB=/data/ytdlbot/file-store.sqlite3 \
    HEALTH_HOST=0.0.0.0 \
    FILE_URL_HOST=0.0.0.0

EXPOSE 8080
VOLUME ["/data/ytdlbot"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD ["python", "-c", "import os; from urllib.request import urlopen; urlopen('http://127.0.0.1:' + os.getenv('HEALTH_PORT', os.getenv('PORT', '8080')) + '/readyz', timeout=3)"]

CMD ["sh", "start.sh"]
