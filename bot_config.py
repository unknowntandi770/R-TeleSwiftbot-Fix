from __future__ import annotations

import base64
import hashlib
import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

MAX_FILE_LINK_TTL_SECONDS = 3 * 60 * 60


def _required(name: str, *aliases: str) -> str:
    for candidate in (name, *aliases):
        value = os.getenv(candidate, "").strip()
        if value:
            return value
    raise RuntimeError(
        f"Missing {name}. Set it in your host's secret manager or in a local .env file."
    )


def _as_bool(value: str, default: bool = False) -> bool:
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _as_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a whole number.") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be at least {minimum}.")
    return value


def _optional_positive_int(name: str) -> int | None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a numeric Telegram user ID.") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive Telegram user ID.")
    return value


def _optional_chat_id(name: str) -> int | None:
    """Read an optional Telegram group/channel ID without unsafe defaults."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a numeric Telegram chat ID.") from exc
    if value == 0 or value < -2_000_000_000_000 or value > 2_000_000_000_000:
        raise RuntimeError(f"{name} is outside the valid Telegram chat ID range.")
    return value


def _load_or_create_cookie_secret(work_dir: Path) -> str:
    """Use the configured secret or persist a host-local fallback.

    Cookie encryption must not prevent the bot from starting on hosts that do
    not expose an optional cookie secret.  The fallback lives under WORK_DIR,
    which is the persistent application volume on supported hosts.
    """
    configured = os.getenv("COOKIE_ENCRYPTION_KEY", "").strip() or os.getenv(
        "SESSION_SECRET", ""
    ).strip()
    if configured:
        return configured

    key_path = Path(
        os.getenv("COOKIE_KEY_FILE", str(work_dir / ".cookie-encryption-key"))
    )
    key_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = key_path.read_text(encoding="utf-8").strip()
        if existing:
            os.chmod(key_path, 0o600)
            return existing
        key_path.unlink()
    except FileNotFoundError:
        pass
    return _create_cookie_key(key_path)


def _create_cookie_key(key_path: Path) -> str:
    """Create a private fallback key without exposing it in logs."""
    value = secrets.token_urlsafe(48)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(str(key_path), flags, 0o600)
    except FileExistsError:
        existing = key_path.read_text(encoding="utf-8").strip()
        if existing:
            os.chmod(key_path, 0o600)
            return existing
        return _rewrite_empty_cookie_key(key_path, value)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(value + "\n")
    except BaseException:
        try:
            key_path.unlink()
        except FileNotFoundError:
            pass
        raise
    return value


def _rewrite_empty_cookie_key(key_path: Path, value: str) -> str:
    """Repair an empty fallback key left by an interrupted first write.

    Uses a write-to-temp-file-then-atomic-replace pattern rather than
    truncating the target file in place, so a second process racing to
    repair the same empty key can never observe a half-written file.
    """
    try:
        descriptor = os.open(str(key_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        existing = key_path.read_text(encoding="utf-8").strip()
        if existing:
            os.chmod(key_path, 0o600)
            return existing
        replacement = key_path.with_name(f".{key_path.name}.replacement")
        replacement.write_text(value + "\n", encoding="utf-8")
        os.chmod(replacement, 0o600)
        os.replace(replacement, key_path)
        return value
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(value + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return value


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    bot_token: str
    redis_url: str
    work_dir: Path
    max_queue_size: int
    workers: int
    cache_ttl: int
    max_upload_mb: int
    enable_redis: bool
    mongodb_url: str
    mongodb_database: str
    cookie_dir: Path
    cookie_key: bytes
    pot_provider_url: str
    file_url_base: str
    file_url_host: str
    file_url_port: int
    file_url_ttl: int
    bin_channel_id: int | None
    file_store_db: Path
    vc_session_string: str
    vc_session_path: Path
    restricted_session_path: Path
    vc_chat_id: int | None
    admin_id: int | None
    file_stream_concurrency: int
    max_download_bytes: int
    restricted_max_messages: int
    health_host: str
    health_port: int

    @classmethod
    def from_env(cls) -> "Settings":
        api_id_raw = _required("API_ID", "APP_ID")
        try:
            api_id = int(api_id_raw)
        except ValueError as exc:
            raise RuntimeError("API_ID must be a numeric Telegram application ID.") from exc
        if api_id <= 0:
            raise RuntimeError("API_ID must be greater than zero.")

        work_dir = Path(os.getenv("WORK_DIR", "tmp/ytdlbot"))
        cookie_dir = Path(os.getenv("COOKIE_DIR", str(work_dir / "cookies")))
        secret = _load_or_create_cookie_secret(work_dir)
        cookie_key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())

        port_value = os.getenv("PORT", "8080").strip() or "8080"
        try:
            default_health_port = int(port_value)
        except ValueError:
            default_health_port = 8080
        public_url = os.getenv("PUBLIC_URL", "").strip().rstrip("/")
        file_url_base = (
            os.getenv("FILE_URL_BASE", "").strip().rstrip("/")
            or public_url
            or f"http://127.0.0.1:{default_health_port}"
        )
        parsed_file_url = urlparse(file_url_base)
        if parsed_file_url.scheme not in {"http", "https"} or not parsed_file_url.netloc:
            raise RuntimeError(
                "FILE_URL_BASE must be an absolute http(s) URL, for example "
                "https://your-domain.example/api."
            )
        max_upload_mb = max(1, _as_int("MAX_UPLOAD_MB", 2000))
        max_download_mb = max(
            1,
            _as_int("MAX_DOWNLOAD_MB", max_upload_mb),
        )
        return cls(
            api_id=api_id,
            api_hash=_required("API_HASH", "APP_HASH"),
            bot_token=_required("BOT_TOKEN", "TOKEN"),
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            work_dir=work_dir,
            max_queue_size=max(1, _as_int("MAX_QUEUE_SIZE", 32)),
            workers=max(1, _as_int("WORKERS", 2)),
            cache_ttl=max(60, _as_int("CACHE_TTL", 86400)),
            max_upload_mb=max_upload_mb,
            enable_redis=_as_bool(os.getenv("ENABLE_REDIS", "false"), False),
            mongodb_url=os.getenv("MONGODB_URL", "").strip(),
            mongodb_database=(
                os.getenv("MONGODB_DATABASE", "ytdlbot").strip() or "ytdlbot"
            ),
            cookie_dir=cookie_dir,
            cookie_key=cookie_key,
            pot_provider_url=os.getenv("POT_PROVIDER_URL", "").strip().rstrip("/"),
            file_url_base=file_url_base,
            file_url_host=os.getenv("FILE_URL_HOST", "0.0.0.0").strip() or "0.0.0.0",
            file_url_port=max(1, _as_int("FILE_URL_PORT", default_health_port)),
            # Never permit public stream/download links to live longer than
            # the documented three-hour retention window.
            file_url_ttl=max(
                60,
                min(
                    MAX_FILE_LINK_TTL_SECONDS,
                    _as_int("FILE_URL_TTL", MAX_FILE_LINK_TTL_SECONDS),
                ),
            ),
            bin_channel_id=_optional_chat_id("BIN_CHANNEL_ID"),
            file_store_db=Path(
                os.getenv("FILE_STORE_DB", str(work_dir / "file-store.sqlite3"))
            ),
            vc_session_string=os.getenv("VC_SESSION_STRING", "").strip(),
            vc_session_path=work_dir / "vc-assistant",
            restricted_session_path=work_dir / "restricted-user",
            vc_chat_id=_optional_chat_id("VC_CHAT_ID"),
            admin_id=_optional_positive_int("ADMIN_ID"),
            file_stream_concurrency=max(1, _as_int("FILE_STREAM_CONCURRENCY", 8)),
            max_download_bytes=max_download_mb * 1024 * 1024,
            restricted_max_messages=max(
                1,
                _as_int("RESTRICTED_MAX_MESSAGES", 20, minimum=1),
            ),
            health_host=os.getenv("HEALTH_HOST", "0.0.0.0").strip() or "0.0.0.0",
            health_port=max(
                1,
                _as_int(
                    "HEALTH_PORT",
                    default_health_port,
                ),
            ),
        )

    def prepare_directories(self) -> None:
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.cookie_dir.mkdir(parents=True, exist_ok=True)
        for directory in (self.work_dir, self.cookie_dir):
            try:
                directory.chmod(0o700)
            except OSError:
                pass