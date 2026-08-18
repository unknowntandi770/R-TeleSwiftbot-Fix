#!/usr/bin/env python3
"""
bot.py — Enhanced entry point for R-TeleSwiftBot
═══════════════════════════════════════════════════════════════════════════════
This file replaces main.py and provides:

  • Pre-flight dependency checks  (ffmpeg, aria2c, yt-dlp, bun/deno)
  • Legacy environment-variable aliases (APP_ID/APP_HASH/TOKEN → API_ID/
    API_HASH/BOT_TOKEN) so older config files keep working
  • Per-user rate limiting for downloads and music searches
  • Structured, colour-aware logging with millisecond timestamps
  • Auto-retry on Telegram FloodWait during startup
  • Graceful SIGTERM / SIGINT shutdown hook
  • Additional commands injected into the Bot instance:
      /ping          – latency check
      /stats         – bot-wide queue and cache statistics
      /broadcast     – admin-only mass message to all recent users
  • Rich startup banner with live environment summary
═══════════════════════════════════════════════════════════════════════════════
Usage:
  python bot.py                     # reads config.env automatically
  LOAD_ENV=false python bot.py      # skip config.env (env vars already set)
"""

from __future__ import annotations

import asyncio
import collections
import html
import logging
import os
import shutil
import signal
import sys
import time
import zipfile
from pathlib import Path
from typing import Callable

# The uploaded project is distributed as a ZIP in this workspace.  Keep the
# entry point runnable both from a normal checkout (where the modules sit next
# to bot.py) and from the Replit workspace (where they are still inside the
# uploaded archive).
_PROJECT_ROOT = Path(__file__).resolve().parent
_SOURCE_DIR = _PROJECT_ROOT / ".teleswiftbot_source"


def _prepare_source_tree() -> Path:
    required = _PROJECT_ROOT / "bot_main.py"
    if required.exists():
        return _PROJECT_ROOT

    if not (_SOURCE_DIR / "bot_main.py").exists():
        archives = sorted(
            (_PROJECT_ROOT / "attached_assets").glob("r-teleswiftbot_*.zip")
        )
        if not archives:
            raise RuntimeError(
                "Could not find bot_main.py or the uploaded r-teleswiftbot ZIP."
            )
        _SOURCE_DIR.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archives[-1]) as archive:
            prefix = "r-teleswiftbot/"
            for member in archive.infolist():
                name = member.filename
                if not name.startswith(prefix) or name.endswith("/"):
                    continue
                relative = Path(name[len(prefix) :])
                if "__pycache__" in relative.parts:
                    continue
                target = _SOURCE_DIR / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.read(member))
        logger_source = logging.getLogger("bot.bootstrap")
        logger_source.info("Loaded bot modules from %s", archives[-1].name)

    return _SOURCE_DIR


_SOURCE_PATH = _prepare_source_tree()
if str(_SOURCE_PATH) not in sys.path:
    sys.path.insert(0, str(_SOURCE_PATH))

# ─── 0. Load config.env (or config.env) before anything else ────────────────

_LOAD_ENV = os.getenv("LOAD_ENV", "true").lower() not in ("false", "0", "no")

if _LOAD_ENV:
    _cfg_candidates = [Path("config.env"), Path("sample_config.env")]
    _loaded_env = None
    for _cfg_path in _cfg_candidates:
        if _cfg_path.exists():
            with _cfg_path.open() as _f:
                for _raw in _f:
                    _line = _raw.strip()
                    if not _line or _line.startswith("#") or "=" not in _line:
                        continue
                    _key, _, _val = _line.partition("=")
                    _key = _key.strip()
                    _val = _val.strip()
                    # Only set values that aren't already in the environment
                    # so that Docker / PaaS environment variables win.
                    if _key and _key not in os.environ:
                        os.environ[_key] = _val
            _loaded_env = _cfg_path
            break
else:
    _loaded_env = None

# bot_config.py calls load_dotenv() without an explicit path.  On Python 3.13
# python-dotenv can assert while walking the import stack. bot.py already loads
# the selected env file above, so disable that second implicit scan.
os.environ.setdefault("PYTHON_DOTENV_DISABLED", "true")

# ─── 1. Logging setup (done before any import so all modules inherit it) ─────

_LOG_LEVEL_NAME = os.getenv("LOG_LEVEL", "INFO").upper()
_LOG_LEVEL = getattr(logging, _LOG_LEVEL_NAME, logging.INFO)

# Colour codes — disabled when stdout is not a TTY or NO_COLOR is set
_COLOURS = sys.stdout.isatty() and "NO_COLOR" not in os.environ
_C = {
    "reset": "\033[0m",
    "grey": "\033[90m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "red": "\033[31m",
    "bold_red": "\033[1;31m",
    "cyan": "\033[36m",
    "bold": "\033[1m",
} if _COLOURS else collections.defaultdict(str)


class _FmtHandler(logging.StreamHandler):
    """Compact, coloured log formatter with millisecond timestamps."""

    _LEVEL_COLOURS = {
        logging.DEBUG: _C["grey"],
        logging.INFO: _C["green"],
        logging.WARNING: _C["yellow"],
        logging.ERROR: _C["red"],
        logging.CRITICAL: _C["bold_red"],
    }

    def format(self, record: logging.LogRecord) -> str:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))
        ms = int(record.msecs)
        lvl_colour = self._LEVEL_COLOURS.get(record.levelno, "")
        lvl = f"{lvl_colour}{record.levelname[0]}{_C['reset']}"
        name = f"{_C['grey']}{record.name:<22}{_C['reset']}"
        msg = record.getMessage()
        base = f"{_C['grey']}{ts}.{ms:03d}{_C['reset']} {lvl} {name} {msg}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def _configure_logging() -> None:
    root = logging.getLogger()
    root.setLevel(_LOG_LEVEL)
    if not any(isinstance(h, _FmtHandler) for h in root.handlers):
        root.handlers.clear()
        root.addHandler(_FmtHandler())
    # Keep third-party libraries quieter
    for _noisy in ("pyrogram", "kurigram", "pytgcalls", "aiohttp", "urllib3"):
        logging.getLogger(_noisy).setLevel(logging.WARNING)


_configure_logging()
logger = logging.getLogger("bot")

# ─── 2. Pre-flight dependency check ─────────────────────────────────────────

_REQUIRED_TOOLS: list[tuple[str, str, str]] = [
    # (env-override key,  binary name,  friendly install hint)
    ("FFMPEG_PATH", "ffmpeg", "https://ffmpeg.org/download.html  OR  apt install ffmpeg"),
    ("YTDLP_PATH", "yt-dlp", "pip install -U yt-dlp"),
]
_RECOMMENDED_TOOLS: list[tuple[str, str, str]] = [
    ("ARIA2C_PATH", "aria2c", "https://aria2.github.io/           OR  install aria2"),
]
_OPTIONAL_TOOLS: list[tuple[str, str, str]] = [
    ("BUN_PATH", "bun", "https://bun.sh/   (only needed for PO-token provider)"),
    ("DENO_PATH", "deno", "https://deno.com/ (only needed for PO-token provider)"),
]


def _check_tool(env_key: str, binary: str, *, required: bool) -> bool:
    """Return True if the tool is reachable, logging a clear message either way."""
    override = os.getenv(env_key)
    path = shutil.which(override or binary)
    if path:
        logger.info("%-8s %-10s → %s", "✔" if required else "○", binary, path)
        return True
    if required:
        logger.error(
            "%-8s %-10s NOT FOUND — %s",
            "✘ FATAL",
            binary,
            f"Please install it and ensure it is on PATH.\n"
            f"           Hint: {_REQUIRED_TOOLS[[e for e, b, _ in _REQUIRED_TOOLS].index(env_key)][2]}"
            if env_key in [e for e, b, _ in _REQUIRED_TOOLS]
            else "",
        )
    else:
        logger.warning(
            "%-8s %-10s not found — PO-token provider / torrent features may be unavailable.",
            "○ SKIP",
            binary,
        )
    return False


def run_preflight_checks() -> None:
    logger.info("─── Pre-flight dependency check ────────────────────────")
    missing_required = []
    for env_key, binary, hint in _REQUIRED_TOOLS:
        if not _check_tool(env_key, binary, required=True):
            missing_required.append((binary, hint))
    for env_key, binary, hint in _RECOMMENDED_TOOLS + _OPTIONAL_TOOLS:
        _check_tool(env_key, binary, required=False)

    if missing_required:
        logger.critical(
            "\n"
            "═══════════════════════════════════════════\n"
            "  STARTUP ABORTED — missing required tools:\n"
            + "\n".join(f"  • {b:10s}  →  {h}" for b, h in missing_required) + "\n"
            "═══════════════════════════════════════════"
        )
        sys.exit(1)

    logger.info("─── All required tools found ────────────────────────────")


# ─── 3. Legacy environment-variable aliases (applied before importing bot_main) ──
#
# Earlier releases of this package carried a set of monkey-patches here for
# bugs in bot_downloader.py, bot_config.py, and bot_main.py (a missing
# URLError import, an ENABLE_REDIS default mismatch, a missing from_user
# guard in delete_cookies, an unguarded `shared` reference in _share_file,
# and a non-atomic cookie-encryption-key fallback write). All five have
# since been fixed directly in the canonical source modules, so the patches
# were removed rather than kept as dead code shadowing already-correct
# behavior — the delete_cookies patch in particular reproduced an older,
# plainer reply and silently dropped the "🏠 Home" navigation button that
# the current bot_main.py implementation includes, which is a real
# regression only if this file's patch runs *after* the fix, as it did.
# What remains below is genuinely additive, not a bug fix: normalizing
# legacy environment-variable names so old config files keep working.


def _apply_patches() -> None:
    """Normalize legacy environment-variable aliases before Settings loads."""
    for current, legacy in (
        ("API_ID", "APP_ID"),
        ("API_HASH", "APP_HASH"),
        ("BOT_TOKEN", "TOKEN"),
    ):
        if current not in os.environ and os.environ.get(legacy):
            os.environ[current] = os.environ[legacy]
    logger.debug("Legacy environment-variable aliases normalized")


# ─── 4. Rate-limiting helper ─────────────────────────────────────────────────

class _RateLimiter:
    """
    Sliding-window in-memory rate limiter.

    Creates one bucket per (user_id, action) pair. Each bucket is a deque
    of timestamps of the last `limit` calls. A call is allowed if the oldest
    timestamp in a full bucket is more than `window` seconds ago.
    """

    def __init__(self, limit: int, window: float) -> None:
        self._limit = limit
        self._window = window
        self._buckets: dict[tuple[int, str], collections.deque] = {}

    def is_allowed(self, user_id: int, action: str = "default") -> bool:
        if self._limit <= 0:
            return True
        key = (user_id, action)
        now = time.monotonic()
        bucket = self._buckets.setdefault(key, collections.deque())
        # Purge expired entries
        while bucket and now - bucket[0] > self._window:
            bucket.popleft()
        if len(bucket) >= self._limit:
            return False
        bucket.append(now)
        return True

    def retry_after(self, user_id: int, action: str = "default") -> float:
        """Seconds until the next call would be allowed (0 if already allowed)."""
        key = (user_id, action)
        bucket = self._buckets.get(key)
        if not bucket or len(bucket) < self._limit:
            return 0.0
        oldest = bucket[0]
        return max(0.0, self._window - (time.monotonic() - oldest))

    def cleanup(self) -> None:
        """Remove expired buckets to prevent unbounded memory growth."""
        now = time.monotonic()
        stale = [
            k for k, b in self._buckets.items()
            if b and now - b[0] > self._window
        ]
        for k in stale:
            bucket = self._buckets[k]
            while bucket and now - bucket[0] > self._window:
                bucket.popleft()
            if not bucket:
                del self._buckets[k]


# Initialise rate limiters from environment variables
_DL_LIMIT = int(os.getenv("USER_RATE_LIMIT_PER_HOUR", "10"))
_SEARCH_LIMIT = int(os.getenv("USER_SEARCH_RATE_LIMIT_PER_MINUTE", "5"))

_download_limiter = _RateLimiter(limit=_DL_LIMIT, window=3600)
_search_limiter = _RateLimiter(limit=_SEARCH_LIMIT, window=60)


# ─── 5. Import bot modules (patches already applied to env) ──────────────────

# Apply env patches BEFORE importing Settings so the defaults are right
_apply_patches()

try:
    from bot_main import Bot
    from bot_config import Settings
except ImportError as _ie:
    logger.critical("Cannot import core bot modules: %s", _ie)
    logger.critical("Make sure you are running from the project root directory.")
    sys.exit(1)

try:
    from pyrogram import filters
    from pyrogram.types import Message
    from pyrogram.errors import FloodWait
except ImportError:
    logger.critical("Cannot import pyrogram/kurigram. Run: pip install kurigram")
    sys.exit(1)


# ─── 6. Rate-limit wrappers for Bot methods ───────────────────────────────────

def _wrap_with_download_rate_limit(original_method: Callable) -> Callable:
    """
    Wrap Bot.enqueue() so that users who exceed USER_RATE_LIMIT_PER_HOUR
    receive a friendly message instead of entering the queue.
    """
    async def _wrapped(self: Bot, message: Message, url: str, *args, **kwargs) -> None:
        user_id = (message.from_user.id if message.from_user else 0)
        # Admins are never rate-limited
        if user_id and user_id != self.settings.admin_id:
            if not _download_limiter.is_allowed(user_id, "download"):
                wait = _download_limiter.retry_after(user_id, "download")
                minutes = max(1, int(wait / 60))
                await message.reply_text(
                    f"⏳ <b>Slow down!</b>\n\n"
                    f"You can submit at most <b>{_DL_LIMIT} downloads per hour</b>.\n"
                    f"Your limit resets in approximately <b>{minutes} minute(s)</b>."
                )
                return
        await original_method(self, message, url, *args, **kwargs)
    return _wrapped


def _wrap_with_search_rate_limit(original_method: Callable) -> Callable:
    """
    Wrap Bot.search_command() so search-spammers are throttled.
    """
    async def _wrapped(self: Bot, client, message: Message) -> None:
        user_id = (message.from_user.id if message.from_user else 0)
        if user_id and user_id != self.settings.admin_id:
            if not _search_limiter.is_allowed(user_id, "search"):
                wait = _search_limiter.retry_after(user_id, "search")
                await message.reply_text(
                    f"⏳ <b>Searching too fast</b>\n\n"
                    f"Limit: <b>{_SEARCH_LIMIT} searches/minute</b>. "
                    f"Please wait <b>{max(1, int(wait))} second(s)</b> and try again."
                )
                return
        await original_method(self, client, message)
    return _wrapped


# ─── 7. New command handlers (injected at runtime) ────────────────────────────

async def _ping_command(self: Bot, _client, message: Message) -> None:
    """/ping — measures round-trip latency to Telegram."""
    t0 = time.monotonic()
    sent = await message.reply_text("🏓 Pong…")
    rtt_ms = (time.monotonic() - t0) * 1000
    # Edit-in-place so only one message is visible
    await sent.edit_text(
        f"🏓 <b>Pong!</b>\n\n"
        f"Round-trip latency: <b>{rtt_ms:.0f} ms</b>\n"
        f"Queue workers: <b>{self.settings.workers}</b>\n"
        f"Bot status: <b>{'✅ ready' if self._ready else '⏳ starting'}</b>"
    )


async def _stats_command(self: Bot, _client, message: Message) -> None:
    """/stats — bot-wide queue and cache statistics (admin-only)."""
    user_id = message.from_user.id if message.from_user else 0
    is_admin = user_id == self.settings.admin_id

    if not is_admin:
        # Non-admins see only their own queue depth
        if not self.queue:
            await message.reply_text("⏳ <b>Starting…</b>\n\nTry again in a moment.")
            return
        my_jobs = self.queue.jobs_for(user_id)
        await message.reply_text(
            f"📊 <b>Your stats</b>\n\n"
            f"Jobs in your queue: <b>{len(my_jobs)}</b>\n"
            f"Use /queue to see details."
        )
        return

    # Admin view
    lines = ["📊 <b>Bot statistics</b>", "─" * 30]

    if self.queue:
        all_jobs = list(self.queue.active_jobs())
        waiting = self.queue.jobs.qsize()
        lines += [
            f"⬇️  Active downloads:   <b>{len(all_jobs)}</b>",
            f"🕒  Waiting in queue:   <b>{waiting}</b>",
            f"👷  Workers configured: <b>{self.settings.workers}</b>",
        ]
    else:
        lines.append("⏳ Queue not yet started")

    lines.append("")
    lines.append(f"💾 Metadata store:    <b>{getattr(self, 'metadata_store_name', 'unknown')}</b>")

    if self.cache:
        cache_type = type(self.cache).__name__
        lines.append(f"🗄  Cache backend:     <b>{cache_type}</b>")

    proc_uptime = time.time() - _BOT_START_TIME
    h, m = divmod(int(proc_uptime), 3600)
    m, s = divmod(m, 60)
    lines.append(f"⏱  Uptime:            <b>{h}h {m:02d}m {s:02d}s</b>")

    if _download_limiter and _DL_LIMIT > 0:
        lines.append(f"🚦  Rate limit:        <b>{_DL_LIMIT} dl/hr per user</b>")

    await message.reply_text("\n".join(lines))


async def _broadcast_command(self: Bot, _client, message: Message) -> None:
    """/broadcast <text> — admin-only: send a message to all users who have
    interacted with the bot and are stored in the file store.
    NOTE: This requires a functional file store with user records.
    """
    user_id = message.from_user.id if message.from_user else 0
    if user_id != self.settings.admin_id:
        await message.reply_text("❌ <b>Admin only</b>")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply_text(
            "📢 <b>Broadcast</b>\n\n"
            "Usage: <code>/broadcast Your announcement here</code>\n\n"
            "The message will be sent to all users with stored files."
        )
        return

    broadcast_text = parts[1].strip()

    if not self.file_store:
        await message.reply_text("❌ <b>File store is not available</b>")
        return

    progress = await message.reply_text("📢 <b>Broadcasting…</b>\n\nFetching user list…")

    try:
        user_ids = await _all_store_owner_ids(self.file_store)

        if not user_ids:
            await progress.edit_text(
                "📢 <b>Broadcast</b>\n\n"
                "No users found in the file store. "
                "Users appear here after they store their first file with /store."
            )
            return

        sent_ok = 0
        sent_fail = 0
        for uid in user_ids:
            try:
                await self.app.send_message(
                    uid,
                    f"📢 <b>Announcement</b>\n\n{html.escape(broadcast_text)}",
                )
                sent_ok += 1
                await asyncio.sleep(0.05)  # stay within Telegram flood limits
            except Exception:
                sent_fail += 1

        await progress.edit_text(
            f"📢 <b>Broadcast complete</b>\n\n"
            f"✅ Delivered: <b>{sent_ok}</b>\n"
            f"❌ Failed:    <b>{sent_fail}</b>"
        )
    except Exception as exc:
        logger.exception("Broadcast failed")
        await progress.edit_text(f"❌ <b>Broadcast failed</b>\n\n{html.escape(str(exc))}")


async def _all_store_owner_ids(file_store) -> list[int]:
    """Get distinct stored-file owners from either supported metadata backend."""
    if hasattr(file_store, "get_all_owner_ids"):
        return sorted(
            {
                int(user_id)
                for user_id in await file_store.get_all_owner_ids()
                if int(user_id) > 0
            }
        )

    connection = getattr(file_store, "_connection", None)
    if connection is not None:
        lock = getattr(file_store, "_lock", None)

        def query_sqlite() -> list[int]:
            if lock is None:
                rows = connection.execute(
                    "SELECT DISTINCT owner_id FROM stored_files WHERE owner_id > 0"
                ).fetchall()
            else:
                with lock:
                    rows = connection.execute(
                        "SELECT DISTINCT owner_id FROM stored_files WHERE owner_id > 0"
                    ).fetchall()
            return sorted({int(row[0]) for row in rows})

        return await asyncio.to_thread(query_sqlite)

    collection = getattr(file_store, "collection", None)
    if collection is not None:
        def query_mongo() -> list[int]:
            return sorted(
                {
                    int(document["owner_id"])
                    for document in collection.find(
                        {"owner_id": {"$gt": 0}},
                        {"owner_id": 1, "_id": 0},
                    )
                    if document.get("owner_id")
                }
            )

        return await asyncio.to_thread(query_mongo)

    return []


# ─── 8. Inject new methods and patches into Bot ───────────────────────────────

def _patch_bot_class() -> None:
    if getattr(Bot, "_enhanced_bot_patched", False):
        return

    # Rate-limit wrappers
    if hasattr(Bot, "enqueue") and _DL_LIMIT > 0:
        Bot.enqueue = _wrap_with_download_rate_limit(Bot.enqueue)  # type: ignore[method-assign]
        logger.debug("Rate limiter applied to Bot.enqueue (%d dl/hr)", _DL_LIMIT)

    if hasattr(Bot, "search_command") and _SEARCH_LIMIT > 0:
        Bot.search_command = _wrap_with_search_rate_limit(Bot.search_command)  # type: ignore[method-assign]
        logger.debug("Rate limiter applied to Bot.search_command (%d/min)", _SEARCH_LIMIT)

    # Register the extra handlers while the client is being constructed.
    # This happens before Client.start(), so no second Telegram connection or
    # handler-registration race is needed.
    if hasattr(Bot, "_create_client"):
        original_create_client = Bot._create_client  # type: ignore[attr-defined]

        def _create_client_with_extras(self: Bot):
            app = original_create_client(self)

            @app.on_message(filters.command("ping"))
            async def _h_ping(client, message: Message) -> None:
                await self.ping_command(client, message)

            @app.on_message(filters.command("stats"))
            async def _h_stats(client, message: Message) -> None:
                await self.stats_command(client, message)

            @app.on_message(filters.command(["broadcast", "bc"]))
            async def _h_broadcast(client, message: Message) -> None:
                await self.broadcast_command(client, message)

            logger.debug("Registered /ping, /stats, /broadcast handlers")
            return app

        Bot._create_client = _create_client_with_extras  # type: ignore[method-assign]

    # Retry transient Telegram FloodWait responses during initial startup.
    # The existing source method already performs its revoked-session retry;
    # this wrapper only handles FloodWait without changing that behavior.
    if hasattr(Bot, "_start_bot_client"):
        original_start_bot_client = Bot._start_bot_client  # type: ignore[attr-defined]

        async def _start_bot_client_with_retry(self: Bot):
            max_attempts = 5
            for attempt in range(1, max_attempts + 1):
                try:
                    return await original_start_bot_client(self)
                except FloodWait as exc:
                    wait_seconds = max(1, int(getattr(exc, "value", 1)))
                    if attempt >= max_attempts:
                        raise RuntimeError(
                            "Telegram kept rate-limiting startup after "
                            f"{max_attempts} attempts."
                        ) from exc
                    logger.warning(
                        "Telegram startup FloodWait: sleeping %ss before retry %s/%s",
                        wait_seconds,
                        attempt + 1,
                        max_attempts,
                    )
                    await asyncio.sleep(wait_seconds)

        Bot._start_bot_client = _start_bot_client_with_retry  # type: ignore[method-assign]
        logger.debug("Startup FloodWait retry enabled")

    # Inject new commands
    Bot.ping_command = _ping_command        # type: ignore[attr-defined]
    Bot.stats_command = _stats_command      # type: ignore[attr-defined]
    Bot.broadcast_command = _broadcast_command  # type: ignore[attr-defined]
    Bot._enhanced_bot_patched = True  # type: ignore[attr-defined]
    logger.debug("New commands injected: /ping, /stats, /broadcast")


# ─── 9. Wire new commands into the Pyrogram app on startup ───────────────────

_NEW_HANDLERS_REGISTERED = False


async def _register_new_handlers(bot: Bot) -> None:
    """Register /ping, /stats, /broadcast as Pyrogram handlers after the app starts."""
    global _NEW_HANDLERS_REGISTERED
    if _NEW_HANDLERS_REGISTERED or not bot.app:
        return

    from pyrogram import Client

    @bot.app.on_message(filters.command("ping"))
    async def _h_ping(client: Client, message: Message) -> None:
        await bot.ping_command(client, message)

    @bot.app.on_message(filters.command("stats"))
    async def _h_stats(client: Client, message: Message) -> None:
        await bot.stats_command(client, message)

    @bot.app.on_message(filters.command(["broadcast", "bc"]))
    async def _h_broadcast(client: Client, message: Message) -> None:
        await bot.broadcast_command(client, message)

    _NEW_HANDLERS_REGISTERED = True
    logger.info("Registered /ping, /stats, /broadcast handlers")


# ─── 10. Periodic housekeeping task ───────────────────────────────────────────

async def _periodic_housekeeping() -> None:
    """Clean rate-limiter buckets and log lightweight heartbeats every hour."""
    while True:
        try:
            await asyncio.sleep(3600)
            _download_limiter.cleanup()
            _search_limiter.cleanup()
            logger.debug("Housekeeping: rate-limiter buckets cleaned")
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Housekeeping task error")


# ─── 11. Startup banner ────────────────────────────────────────────────────────

_BOT_START_TIME = time.time()

_BANNER = r"""
  ╔══════════════════════════════════════════════════════╗
  ║          R-TeleSwiftBot  ·  Enhanced Edition         ║
  ║          python bot.py  –  all-in-one entry          ║
  ╠══════════════════════════════════════════════════════╣
  ║  /ytdl  /audio  /search  /mirror  /leech  /vplay    ║
  ║  /store /myfiles /filestream /ping /stats /broadcast ║
  ╚══════════════════════════════════════════════════════╝
"""


def _print_env_summary() -> None:
    def _mask(val: str) -> str:
        if not val or len(val) < 8:
            return "***"
        return val[:4] + "…" + val[-2:]

    def _get(key: str, default: str = "(not set)") -> str:
        return os.getenv(key, default)

    redis_on = _get("ENABLE_REDIS", "false").lower() not in ("false", "0", "no")
    mongo_url = _get("MONGODB_URL", "")

    rows = [
        ("API_ID",        _mask(_get("API_ID"))),
        ("BOT_TOKEN",     _mask(_get("BOT_TOKEN"))),
        ("ADMIN_ID",      _get("ADMIN_ID")),
        ("PUBLIC_URL",    _get("PUBLIC_URL")),
        ("WORK_DIR",      _get("WORK_DIR", "tmp/ytdlbot")),
        ("WORKERS",       _get("WORKERS", "2")),
        ("MAX_QUEUE_SIZE",_get("MAX_QUEUE_SIZE", "32")),
        ("REDIS",         "enabled" if redis_on else "disabled (memory cache)"),
        ("MONGODB",       "connected" if mongo_url else "disabled (SQLite)"),
        ("RATE LIMIT DL", f"{_DL_LIMIT}/hr" if _DL_LIMIT > 0 else "disabled"),
        ("RATE LIMIT SRC",f"{_SEARCH_LIMIT}/min" if _SEARCH_LIMIT > 0 else "disabled"),
        ("LOG_LEVEL",     _LOG_LEVEL_NAME),
        ("CONFIG FILE",   str(_loaded_env) if _loaded_env else "env vars only"),
    ]
    width = max(len(k) for k, _ in rows) + 2
    print("  ┌" + "─" * (width + 32) + "┐", flush=True)
    for key, val in rows:
        print(f"  │  {key:<{width}} {val}", flush=True)
    print("  └" + "─" * (width + 32) + "┘", flush=True)


# ─── 12. Validate required environment variables ──────────────────────────────

def _validate_required_env() -> None:
    missing = []
    for key in ("API_ID", "API_HASH", "BOT_TOKEN"):
        val = os.getenv(key, "").strip()
        if not val or val.startswith("your_"):
            missing.append(key)
    if missing:
        logger.critical(
            "\n"
            "═══════════════════════════════════════════════════════\n"
            "  STARTUP ABORTED — required environment variables not set:\n"
            + "\n".join(f"  • {k}" for k in missing) + "\n\n"
            "  Edit config.env (copy sample_config.env) and fill in real values.\n"
            "═══════════════════════════════════════════════════════"
        )
        sys.exit(1)

    admin_id = os.getenv("ADMIN_ID", "").strip()
    if not admin_id or admin_id.startswith("your_"):
        logger.warning(
            "ADMIN_ID is not set. Admin-only commands (/broadcast, /stats detail, "
            "/admin, /ban) will be inaccessible."
        )

    public_url = os.getenv("PUBLIC_URL", "").strip()
    if not public_url or "example.com" in public_url:
        logger.warning(
            "PUBLIC_URL is not configured correctly. "
            "The /filestream command and file-link sharing will not work."
        )


# ─── 13. Enhanced run() wrapping bot_main.Bot.run() ────────────────────────────

async def _run_enhanced() -> None:
    """
    Drop-in replacement for bot_main.main() with:
    - Pre-registered patches
    - New handler registration
    - Housekeeping task
    - Graceful SIGTERM support
    """
    settings = Settings.from_env()
    bot = Bot(settings)

    hk_task = asyncio.create_task(_periodic_housekeeping())
    try:
        # Bot.run() performs setup, creates the client, starts Telegram, and
        # owns the complete shutdown/finally lifecycle.  The class patches
        # above add functionality without duplicating any of those phases.
        await bot.run()
    finally:
        hk_task.cancel()
        await asyncio.gather(hk_task, return_exceptions=True)


# ─── 14. Signal handling + main entry point ────────────────────────────────────

def _setup_signals(loop: asyncio.AbstractEventLoop) -> None:
    """Install SIGTERM handler so Docker / PaaS can shut the bot down cleanly."""

    def _on_sigterm() -> None:
        logger.info("Received SIGTERM — shutting down gracefully…")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    if sys.platform != "win32":
        try:
            loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
        except (NotImplementedError, RuntimeError):
            logger.debug("SIGTERM handler is unavailable on this event loop")


def main() -> None:
    print(_BANNER, flush=True)

    # 1. Validate required env vars
    _validate_required_env()

    # 2. Print config summary
    _print_env_summary()
    print(flush=True)

    # 3. Pre-flight system dependency check
    run_preflight_checks()

    # 4. Apply in-class patches
    _patch_bot_class()

    logger.info("Starting R-TeleSwiftBot enhanced edition…")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _setup_signals(loop)

    try:
        loop.run_until_complete(_run_enhanced())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt — stopping bot")
    except asyncio.CancelledError:
        logger.info("Bot tasks cancelled — stopped cleanly")
    except SystemExit:
        raise
    except Exception:
        logger.exception("Fatal error in bot main loop")
        sys.exit(1)
    finally:
        try:
            # Cancel any remaining tasks
            pending = asyncio.all_tasks(loop)
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
        except Exception:
            pass
        loop.close()
        logger.info("R-TeleSwiftBot stopped. Goodbye!")


if __name__ == "__main__":
    main()
