#!/bin/sh
# R-TeleSwiftBot startup script
# ─────────────────────────────────────────────────────────────────────────────
# Works for: local dev, Docker, Railway, Render, Heroku-like hosts, systemd.
# On PaaS hosts the PORT variable is provided automatically; leave HEALTH_PORT
# and FILE_URL_PORT unset so both services share the single exposed port.
# ─────────────────────────────────────────────────────────────────────────────
set -eu

# Always resolve paths relative to this release, even when a process manager
# starts the script from another working directory.
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON:-python3}"
command -v "$PYTHON_BIN" >/dev/null 2>&1 || PYTHON_BIN=python

# Load .env when present (local development only — PaaS injects vars directly)
if [ -f ".env" ]; then
    # shellcheck disable=SC2046
    export $(grep -v '^#' .env | grep -v '^[[:space:]]*$' | xargs)
fi

# Run environment preflight checks (exits non-zero if required vars are missing)
"${PYTHON_BIN}" preflight.py || exit 1

# Clean up stale Telegram bot session files that cause SESSION_REVOKED errors.
# The bot caches its MTProto session as ytdlbot.session. On a fresh deployment
# or after a BOT_TOKEN rotation Telegram will reject the old session; removing
# it forces a clean re-auth without touching user cookies, VC sessions, or data.
WORK_DIR="${WORK_DIR:-tmp/ytdlbot}"
SESSION_FILE="${WORK_DIR}/ytdlbot.session"
if [ -f "${SESSION_FILE}" ]; then
    # Only remove if the session is more than 30 days old (stale heuristic)
    if find "${SESSION_FILE}" -mtime +30 -print 2>/dev/null | grep -q .; then
        echo "INFO: Removing stale Telegram session (>30 days): ${SESSION_FILE}"
        rm -f "${SESSION_FILE}"
    fi
fi

echo "Starting R-TeleSwiftBot..."
# bot.py is the real entry point: it applies startup bug-fix patches, wires
# up per-user rate limiting, registers the /ping /stats /broadcast commands,
# runs the tool preflight check, and installs graceful SIGTERM handling
# before delegating to bot_main.Bot. Running bot_main.py directly (as older
# versions of this script did) silently skips every one of those features.
exec "${PYTHON_BIN}" bot.py
