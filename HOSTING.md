# Universal hosting guide

The bot is a single Python service. It keeps Telegram interaction, the
range-aware file streamer, shared MongoDB or SQLite metadata, encrypted cookie
files, and temporary media under one configurable work directory. It supports
both one-port PaaS hosts and Docker/VPS deployments with separate ports.

## Required secrets

Set these as private environment variables on every host:

- `API_ID`
- `API_HASH`
- `BOT_TOKEN`
- `SESSION_SECRET` (or a separate `COOKIE_ENCRYPTION_KEY`) is optional. If
  neither is configured, the bot creates a strong persistent fallback key
  under `WORK_DIR` for encrypted cookie storage.

Optional features are disabled safely when their settings are blank:

- `BIN_CHANNEL_ID` enables the permanent archive and `/store` flows.
- `VC_CHAT_ID` and `VC_SESSION_STRING` enable private-chat voice playback only.
  `VC_SESSION_STRING` is never used for restricted-content retrieval.
- The dedicated restricted-content user session is configured separately with
  the admin-only private `/rauthorize` flow. That session must already have
  access to the source chat; the bot never auto-joins invite links.
- Alternatively, the configured `ADMIN_ID` can run `/rauthorize` in a private
  chat with the bot to authorize a separate Telegram user session specifically
  for restricted-content retrieval. The login prompts are deleted immediately,
  and `/rauthorize reset` removes only that restricted session. This flow does
  not replace `VC_SESSION_STRING` or the voice-chat assistant account.
- `REDIS_URL` enables shared cache storage.
- `MONGODB_URL` enables the shared metadata store for archive indexes, saved
  files, statistics, and bans. `MONGODB_DATABASE` selects the database and
  defaults to `ytdlbot`.
- `POT_PROVIDER_URL` enables the optional YouTube PO-token provider.

The smart media pipeline accepts direct HTTP(S) files, extensionless media,
cloud/share links, extractor-supported pages, HLS/DASH manifests, magnet links,
and public `.torrent` files. Private-network URLs and embedded credentials are
rejected.

Never commit `.env`, browser cookies, Telegram session files, or session
strings. The bot encrypts uploaded cookies at rest. If you provide an
encryption secret, protect it like a password; otherwise protect the
`WORK_DIR` volume because it contains the generated fallback key.

## Docker (recommended)

```bash
cp sample_config.env config.env
# edit config.env with the required private values
docker compose up -d --build
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

`docker-compose.yml` builds the image from the included `Dockerfile`, exposes
`HEALTH_PORT` (8080) and `FILE_URL_PORT` (8081) as separate ports, and
persists `/data/ytdlbot` (SQLite index, encrypted cookies, sessions, cache)
in a named volume across restarts/rebuilds.

The same release can be started on a generic host with:

```bash
sh start.sh
```

This runs a dependency-free preflight first and prints actionable missing
environment-variable errors instead of exiting with an opaque container error.

Mount or back up the `bot-data` volume. It contains the SQLite fallback index,
encrypted cookies, voice assistant session state, and temporary work files.
When `MONGODB_URL` is configured, archive metadata is shared through MongoDB
and the existing SQLite index is migrated idempotently on startup. Telegram
continues to hold the actual archived media.

For public `/filestream` links, set `PUBLIC_URL` or `FILE_URL_BASE` to the
externally reachable HTTPS URL of this bot. The bot exposes
`/file/{token}/{stream|download}` directly and preserves HTTP ranges. Configure
TLS at the platform or reverse proxy. Both stream and download links expire
after 3 hours; expired tokens return `404 Not Found` and are removed by the
link-store cleanup loop. The bot also removes stale transient download
directories automatically, without deleting cookies, Telegram sessions,
metadata, or bin-channel archive media.

## systemd or a generic process host

Install Python 3.11 or newer, `ffmpeg`, and `aria2c`, then install the project:

```bash
python -m venv .venv
. .venv/bin/activate
pip install .
sh start.sh
```

Run `sh start.sh` under the host's process supervisor and set
`HEALTH_PORT` to the port used by its readiness check. The bot handles shutdown
cleanup through its normal asyncio lifecycle; stop one instance before
starting another when using the SQLite file store.

## One-port PaaS hosts

Railway, Render, Heroku-compatible hosts, and similar services usually provide
one port through `PORT`. The bot automatically uses that port for health checks
and public file links when `HEALTH_PORT` and `FILE_URL_PORT` are not set.

Set `PUBLIC_URL` and the required Telegram variables:

```text
PUBLIC_URL=https://your-service.example.com
```

Leave `HEALTH_PORT` and `FILE_URL_PORT` unset. They automatically follow the
platform-provided `PORT`, allowing `/readyz` and `/file/...` to share one
listener. If the platform requires explicit values, set both to its assigned
port. Use `/readyz` as the platform health check; use `/healthz` only for
process liveness monitoring.

For Tranger Cloud or similar ZIP-based container hosts, upload the release ZIP,
use `sh start.sh` if a start command is requested, and add these variables in
the environment section:

```text
API_ID
API_HASH
BOT_TOKEN
PUBLIC_URL
```

`SESSION_SECRET` or `COOKIE_ENCRYPTION_KEY` may also be added as an optional
private variable when you want to manage the cookie-encryption key yourself.
If omitted, the bot generates and persists one inside `WORK_DIR`.

Do not paste secrets into the ZIP or into the source code. If the platform
reports that a container was removed because of a plan limit, restarting the
container will not fix it; reduce resource settings or use a plan that permits
the container to run. If logs show `SESSION_REVOKED`, deploy this updated
package: it removes only the stale bot session database and retries with the
current `BOT_TOKEN`. It does not remove the separate voice or restricted-user
sessions.

If the next log says `BOT_TOKEN was rejected by Telegram`, the token itself is
expired or revoked. Create a new token with `@BotFather`, replace the private
`BOT_TOKEN` variable in Tranger Cloud, and restart the app. Do not paste the
token into the ZIP or into chat.

## Replit

Run `python bot_main.py` and add the required Telegram secrets in Replit's
private secret manager. Set `PUBLIC_URL` or `FILE_URL_BASE` to the public HTTPS
URL if `/filestream` links are enabled. The PO-token provider is optional;
leaving `POT_PROVIDER_URL` blank keeps the bot usable without cloning a second
service at startup.

## Operations

- `/healthz` only means the process is alive.
- `/readyz` means Telegram has connected and the bot finished initialization.
- File stream/download links are intentionally limited to a maximum of 3 hours.
- Keep `MAX_UPLOAD_MB` and `MAX_DOWNLOAD_MB` realistic for the host's disk and
  Telegram plan.
- `RESTRICTED_MAX_MESSAGES` bounds `/save` message ranges and media albums
  (default: `20`). Restricted retrieval also uses `MAX_DOWNLOAD_MB`.
- `FILE_STREAM_CONCURRENCY` limits concurrent public media streams.
- Redis is recommended for more than one process; a Redis outage falls back to
  a local cache so downloads continue, but duplicate cache uploads are possible.
- MongoDB is recommended for more than one process because it shares archive
  metadata and bans. If MongoDB cannot connect, the bot reports the fallback
  and continues with SQLite.
- Use `python -m compileall -q .` and
  `pnpm run typecheck` before deploying.
- Do not run two bot replicas against the same SQLite volume unless MongoDB is
  configured as the active metadata store.