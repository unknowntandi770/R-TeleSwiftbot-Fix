# YTDL Telegram Bot

A production-minded Telegram downloader bot built around Pyrogram and yt-dlp.
It supports YouTube downloads, playlists, search, bounded queues, progress
updates, temporary file links, permanent Telegram archive storage, optional
Redis caching, shared MongoDB metadata, torrents, and optional voice-chat
playback.

The repository is designed to run from the same source on Docker, Railway,
Render, Heroku-compatible hosts, systemd, VPS servers, and Replit.

The clean release package contains only the bot, deployment files,
documentation, and dependency manifests. Replit workspace artifacts and
frontend artifacts are intentionally not part of the hosting package.

## Quick start

```bash
git clone https://github.com/YOUR_ACCOUNT/YOUR_REPOSITORY.git
cd YOUR_REPOSITORY
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
cp .env.example .env
# Edit .env with private values.
sh start.sh
```

The host must provide `ffmpeg` for media conversion. Install `aria2c` too if
magnet/torrent downloads are enabled.

To create a clean GitHub/hosting upload without local state, run:

```bash
the supplied ZIP is already flat
```

The ZIP is written to `release/ytdl-telegram-bot-source.zip` and contains the
source, deployment manifests, documentation, and a release manifest. It does
not contain secrets, Telegram sessions, cookies, databases, caches, logs,
temporary media, or Replit workspace artifacts.

## Required environment

Set these as private host variables or secrets:

- `API_ID` and `API_HASH` from Telegram
- `BOT_TOKEN` from BotFather
- `COOKIE_ENCRYPTION_KEY` or `SESSION_SECRET` (optional; when omitted, the bot
  creates a private persistent fallback key under `WORK_DIR`)
- `MONGODB_URL` is recommended for shared archive metadata, saved-file search,
  statistics, and bans when running more than one bot process. Set
  `MONGODB_DATABASE` to customize the database name; it defaults to `ytdlbot`.

For public `/filestream` buttons, also set `PUBLIC_URL` to the externally
reachable HTTPS base URL, for example `https://bot.example.com`. If you use a
reverse proxy path, set `FILE_URL_BASE` explicitly instead.

Stream and download links expire after 3 hours and are removed automatically.
The bot also periodically cleans stale temporary download directories while
preserving cookies, Telegram sessions, metadata, and bin-channel archives.

Never commit `.env`, cookies, Telegram sessions, database files, or session
strings. The repository ignore rules already exclude them.

## Hosting modes

### One-port PaaS mode

Most managed hosts provide one `PORT`. The bot automatically uses that port
for health checks and file links, so `/healthz`, `/readyz`, and `/file/...`
work through one listener. Set `PUBLIC_URL` and deploy:

```bash
sh start.sh
```

On a one-port host, do not set `HEALTH_PORT` or `FILE_URL_PORT`; both
automatically follow the platform-provided `PORT`. Add these private variables
in the host dashboard before deploying:

```text
API_ID=...
API_HASH=...
BOT_TOKEN=...
PUBLIC_URL=https://your-assigned-host-url
```

If you use encrypted cookie uploads, set `COOKIE_ENCRYPTION_KEY` or
`SESSION_SECRET` to a long random private string. If neither is configured, the
bot creates `WORK_DIR/.cookie-encryption-key` with restricted permissions and
reuses it across restarts. The startup preflight reports missing required
Telegram variables without printing secret values.

If Telegram logs show `SESSION_REVOKED`, the launcher automatically removes
only the stale cached bot session and retries with `BOT_TOKEN`. It does not
remove voice-chat sessions, restricted retrieval sessions, cookies, or stored
metadata.

If startup reports `BOT_TOKEN was rejected by Telegram`, create a new token
with `@BotFather` and replace the private `BOT_TOKEN` variable on the hosting
platform. Restarting with the old token will continue to fail.

The `Procfile`, `railway.toml`, and `render.yaml` are included for common
platforms. If the platform builds from Docker, use the included `Dockerfile`.

### Docker or VPS mode

Docker Compose keeps health and media traffic on separate ports and persists
all state:

```bash
cp .env.example .env
docker compose up -d --build
curl http://127.0.0.1:8080/healthz
```

MongoDB stores file metadata and ban state while Telegram remains the durable
media archive. If MongoDB is unavailable, the bot automatically falls back to
the local SQLite metadata index. Redis remains the optional hot cache for
Telegram file IDs and download metadata.

## Development checks

```bash
python -m compileall -q .
```

### Unit tests

A `tests/` suite (stdlib `unittest`, no extra runtime deps besides
`cryptography`) covers the pure logic in `bot_urls.py`, `bot_quality.py`,
`bot_config.py`, and `bot_cookies.py` — URL/source classification, quality
normalization, environment/settings validation, and the encrypted cookie
store's save/retrieve/permissions behavior. Modules that import `pyrogram`
or `yt_dlp` at module scope (`bot.py`, `bot_main.py`, `bot_downloader.py`,
etc.) aren't covered yet since exercising them needs those dependencies
installed; see `AUDIT.md` for the full breakdown.

```bash
pip install -e ".[dev]"
python -m unittest discover -s tests -v
# or, once ruff/mypy are installed:
ruff check .
mypy .
```

## Smart media commands

- `/mirror URL` automatically chooses the fastest safe path for direct files,
  Google Drive downloads, HLS/DASH manifests, and extractor-supported sources.
- `/leech magnet:?xt=...` or `/leech https://host/file.torrent` downloads
  torrent content with progress and cleanup. For multi-file torrents, select
  only the files you need: `/leech magnet:?xt=... --select 1,3-5`.
- `/vplay URL` automatically detects audio, video, HLS, DASH, YouTube, Drive,
  and replied Telegram media. It resolves public sources to a live stream and
  does not download or upload them to the bin channel. Restricted Telegram
  playback uses a temporary file through the dedicated authorized session,
  without archive reuse or bin-channel upload. Use `/vplay audio ...` or
  `/vplay video ...` to override detection.
- `/save https://t.me/channel/123` retrieves accessible public/private Telegram
  messages through the configured authorized user session. Message ranges and
  media albums are bounded by `RESTRICTED_MAX_MESSAGES` and the normal download
  limit. `/mirror` and plain Telegram message URLs use the same safe path.
- Restricted retrieval uses only the dedicated `/rauthorize` user session.
  `VC_SESSION_STRING` is reserved for voice-chat playback and is never borrowed
  for `/save`, restricted `/mirror`, or restricted `/vplay` retrieval.
- `/savecheck https://t.me/c/123456/123` checks whether the authorized user
  session can resolve a private chat and read a message without downloading it.
- `/rauthorize` lets the configured bot administrator authorize a dedicated
  Telegram user account for restricted-content retrieval. Run it in a private
  chat with the bot; the phone number, login code, and 2FA password are deleted
  immediately. Use `/rauthorize reset` to remove that restricted session without
  changing the voice-chat assistant session.

Supported HTTP(S) inputs include direct files, extensionless media endpoints,
temporary signed URLs, cloud-share downloads, social/video pages recognized by
yt-dlp, HLS/DASH manifests, and public `.torrent` files. Unsupported or private
network destinations are rejected instead of being fetched blindly.

## GitHub upload

1. Create an empty private or public GitHub repository.
2. Do not upload `.env`, Telegram credentials, session files, cookies, or
   generated databases.
3. From this directory run:

```bash
git init
git add .
git commit -m "Prepare hosting-ready Telegram bot"
git branch -M main
git remote add origin https://github.com/YOUR_ACCOUNT/YOUR_REPOSITORY.git
git push -u origin main
```

The included GitHub Actions workflow compiles the Python bot and helper scripts
on every push and pull request.

See [`HOSTING.md`](HOSTING.md) for platform-specific setup and the
complete environment reference.