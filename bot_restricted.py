from __future__ import annotations

import asyncio
import logging
import re
import shutil
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlparse
from uuid import uuid4

from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    PasswordHashInvalid,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    PhoneNumberInvalid,
    RPCError,
    SessionPasswordNeeded,
)

from bot_downloader import (
    CancelCheck,
    DownloadCancelled,
    DownloadError,
    DownloadItem,
    DownloadResult,
    ProgressCallback,
    ProgressSnapshot,
)

logger = logging.getLogger("ytdlbot.restricted")


_TELEGRAM_HOSTS = {"t.me", "telegram.me", "www.t.me", "www.telegram.me"}
_TELEGRAM_PATH_RE = re.compile(
    r"^/(?:(?P<scope>c|b)/)?(?P<chat>[A-Za-z0-9_]{1,64})/"
    r"(?P<start>\d+)(?:-(?P<end>\d+))?/?$",
    re.IGNORECASE,
)
_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:t\.me|telegram\.me)(?:/[^\s<>]+)+",
    re.IGNORECASE,
)
_INVITE_RE = re.compile(
    r"^/(?:\+|joinchat/|addlist/)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class RestrictedSource:
    chat_id: int | str
    start_id: int
    end_id: int
    url: str

    @property
    def count(self) -> int:
        return self.end_id - self.start_id + 1

    @property
    def is_private_link(self) -> bool:
        return isinstance(self.chat_id, int)


def parse_restricted_source(text: str) -> RestrictedSource | None:
    """Parse a Telegram message URL without joining chats or following redirects."""
    match = _URL_RE.search(text or "")
    if not match:
        return None
    candidate = match.group(0).rstrip(".,!?)]}>\"'")
    parsed = urlparse(candidate)
    if (parsed.hostname or "").lower().rstrip(".") not in _TELEGRAM_HOSTS:
        return None
    path = parsed.path or ""
    if _INVITE_RE.match(path):
        return None
    path_match = _TELEGRAM_PATH_RE.match(path)
    if not path_match:
        return None
    scope = (path_match.group("scope") or "").lower()
    chat = path_match.group("chat")
    try:
        start_id = int(path_match.group("start"))
        end_id = int(path_match.group("end") or start_id)
    except (TypeError, ValueError):
        return None
    if start_id <= 0 or end_id < start_id:
        return None
    if scope == "c":
        if not chat.isdigit():
            return None
        chat_id: int | str = int(f"-100{chat}")
    else:
        chat_id = chat
    # Ignore URL tracking parameters, but retain a stable canonical message URL.
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    canonical = f"https://t.me/{scope + '/' if scope else ''}{chat}/{start_id}"
    if end_id != start_id:
        canonical += f"-{end_id}"
    if query.get("single") is not None:
        canonical += "?single"
    return RestrictedSource(chat_id, start_id, end_id, canonical)


def _message_media(message: object) -> tuple[str, int, str | None, bool] | None:
    for attribute, fallback_name in (
        ("document", "document.bin"),
        ("video", "video.mp4"),
        ("audio", "audio.mp3"),
        ("photo", "photo.jpg"),
        ("animation", "animation.mp4"),
        ("voice", "voice.ogg"),
        ("video_note", "video-note.mp4"),
    ):
        media = getattr(message, attribute, None)
        if not media:
            continue
        name = Path(getattr(media, "file_name", None) or fallback_name).name
        size = max(0, int(getattr(media, "file_size", 0) or 0))
        mime_type = getattr(media, "mime_type", None)
        if attribute == "photo":
            mime_type = mime_type or "image/jpeg"
        audio_only = attribute in {"audio", "voice"} or (
            str(mime_type or "").lower().startswith("audio/")
        )
        return name or fallback_name, size, mime_type, audio_only
    return None


class RestrictedContentError(DownloadError):
    """Safe user-facing error for authorized Telegram message retrieval."""


class RestrictedSessionManager:
    """Own a dedicated, optional Telegram user session for restricted media."""

    def __init__(self, api_id: int, api_hash: str, session_path: Path) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._session_path = session_path
        self._client: Client | None = None
        self._login_client: Client | None = None
        self._phone_number = ""
        self._phone_code_hash = ""

    @property
    def session_file(self) -> Path:
        return self._session_path.with_suffix(".session")

    @property
    def authorized_client(self) -> Client | None:
        client = self._client
        if client and client.is_connected:
            return client
        return None

    @property
    def authorized(self) -> bool:
        return self.authorized_client is not None

    async def identity(self) -> object | None:
        client = self.authorized_client
        if not client:
            return None
        return await client.get_me()

    def _new_client(self) -> Client:
        self._session_path.parent.mkdir(parents=True, exist_ok=True)
        self._session_path.parent.chmod(0o700)
        return Client(
            self._session_path.name,
            api_id=self._api_id,
            api_hash=self._api_hash,
            workdir=str(self._session_path.parent),
            hide_password=True,
            skip_updates=True,
        )

    async def start(self) -> bool:
        """Load an existing local session without initiating a login."""
        if not self.session_file.exists():
            return False
        client = self._new_client()
        try:
            authorized = await client.connect()
            if not authorized:
                await client.disconnect()
                return False
            me = await client.get_me()
            if not me or getattr(me, "is_bot", False):
                await client.disconnect()
                return False
            self._client = client
            self.session_file.chmod(0o600)
            return True
        except Exception:
            logger.warning("Restricted user session could not be loaded", exc_info=True)
            if client.is_connected:
                await client.disconnect()
            return False

    async def begin_login(self, phone_number: str) -> bool:
        """Start phone-code login. Returns true if an existing session was loaded."""
        await self.cancel_login()
        client = self._new_client()
        try:
            authorized = await client.connect()
            if authorized:
                me = await client.get_me()
                if me and not getattr(me, "is_bot", False):
                    self._client = client
                    self.session_file.chmod(0o600)
                    return True
                raise RestrictedContentError(
                    "That session belongs to a bot account. Use a real Telegram user account."
                )
            sent_code = await client.send_code(phone_number)
            self._login_client = client
            self._phone_number = phone_number
            self._phone_code_hash = sent_code.phone_code_hash
            return False
        except PhoneNumberInvalid as exc:
            raise RestrictedContentError(
                "Telegram rejected that phone number. Use international format, "
                "for example +15551234567."
            ) from exc
        except FloodWait as exc:
            raise RestrictedContentError(
                "Telegram is temporarily rate-limiting authorization. "
                "Wait a little and run /rauthorize again."
            ) from exc
        except RestrictedContentError:
            raise
        except RPCError as exc:
            raise RestrictedContentError(
                "Telegram could not start authorization. Check the number and try again."
            ) from exc
        finally:
            if not self._login_client and client.is_connected and client is not self._client:
                await client.disconnect()

    async def finish_code(self, code: str) -> bool:
        """Finish the code step. Returns true when Telegram requires 2FA."""
        client = self._login_client
        if not client:
            raise RestrictedContentError("The authorization request expired. Start again.")
        try:
            user = await client.sign_in(
                self._phone_number,
                self._phone_code_hash,
                code,
            )
        except SessionPasswordNeeded:
            return True
        except (PhoneCodeInvalid, PhoneCodeExpired) as exc:
            raise RestrictedContentError(
                "That Telegram login code is invalid or expired. Run /rauthorize again."
            ) from exc
        except FloodWait as exc:
            raise RestrictedContentError(
                "Telegram is temporarily rate-limiting authorization. "
                "Run /rauthorize again later."
            ) from exc
        if not user or getattr(user, "is_bot", False):
            await self.cancel_login()
            raise RestrictedContentError(
                "This login is not a real Telegram user account."
            )
        self._client = client
        self._login_client = None
        self._clear_login_values()
        self.session_file.chmod(0o600)
        return False

    async def finish_password(self, password: str) -> None:
        client = self._login_client
        if not client:
            raise RestrictedContentError("The authorization request expired. Start again.")
        try:
            user = await client.check_password(password)
        except PasswordHashInvalid as exc:
            raise RestrictedContentError(
                "That Telegram 2-step verification password is incorrect."
            ) from exc
        except FloodWait as exc:
            raise RestrictedContentError(
                "Telegram is temporarily rate-limiting authorization. "
                "Run /rauthorize again later."
            ) from exc
        if not user or getattr(user, "is_bot", False):
            await self.cancel_login()
            raise RestrictedContentError(
                "This login is not a real Telegram user account."
            )
        self._client = client
        self._login_client = None
        self._clear_login_values()
        self.session_file.chmod(0o600)

    async def cancel_login(self) -> None:
        client = self._login_client
        self._login_client = None
        self._clear_login_values()
        if client and client.is_connected:
            await client.disconnect()

    async def close(self) -> None:
        await self.cancel_login()
        client = self._client
        self._client = None
        if client and client.is_connected:
            await client.disconnect()

    async def reset(self) -> None:
        """Disconnect and remove only the dedicated restricted session."""
        await self.close()
        self.session_file.unlink(missing_ok=True)
        for suffix in ("-journal", "-wal", "-shm"):
            self.session_file.with_name(self.session_file.name + suffix).unlink(
                missing_ok=True
            )

    def _clear_login_values(self) -> None:
        self._phone_number = ""
        self._phone_code_hash = ""


class RestrictedMessageDownloader:
    """Bounded downloader for Telegram message links using one authorized client."""

    def __init__(
        self,
        client_provider: Callable[[], Client | None],
        work_dir: Path,
        max_bytes: int,
        max_messages: int = 20,
    ) -> None:
        self._client_provider = client_provider
        self.work_dir = work_dir
        self.max_bytes = max(1, int(max_bytes))
        self.max_messages = max(1, int(max_messages))

    async def download(
        self,
        source_url: str,
        user_id: int,
        progress: ProgressCallback | None = None,
        cancel_check: CancelCheck | None = None,
    ) -> DownloadResult:
        del user_id
        source = parse_restricted_source(source_url)
        if not source:
            raise RestrictedContentError(
                "Send a Telegram message link such as "
                "https://t.me/channel/123 or https://t.me/c/123456/123."
            )
        if source.count > self.max_messages:
            raise RestrictedContentError(
                f"That message range is too large. The limit is "
                f"{self.max_messages} messages per request."
            )
        client = self._client_provider()
        if client is None:
            raise RestrictedContentError(
                "Restricted Telegram retrieval is not configured. "
                "Authorize the dedicated restricted-content user session with "
                "<code>/rauthorize</code> first."
            )
        if cancel_check and cancel_check():
            raise DownloadCancelled("Download cancelled.")

        messages = await self._get_messages(client, source)
        if not messages:
            raise RestrictedContentError(
                "Telegram did not return any accessible messages for that link."
            )
        messages = await self._expand_media_groups(client, source, messages)
        if len(messages) > self.max_messages:
            raise RestrictedContentError(
                f"That album/range contains too many messages. The limit is "
                f"{self.max_messages} messages."
            )
        known_bytes = sum(
            _message_media(message)[1]
            for message in messages
            if _message_media(message)
        )
        if known_bytes > self.max_bytes:
            raise RestrictedContentError(
                "The requested Telegram content exceeds the configured download limit."
            )

        output_dir = self.work_dir / "restricted" / uuid4().hex
        output_dir.mkdir(parents=True, exist_ok=True)
        items: list[DownloadItem] = []
        completed = False
        try:
            for index, message in enumerate(messages, 1):
                if cancel_check and cancel_check():
                    raise DownloadCancelled("Download cancelled.")
                if progress:
                    await progress(
                        ProgressSnapshot(
                            percent=(index - 1) * 100 / max(1, len(messages)),
                            status="downloading",
                            title=f"Telegram message {message.id}",
                            playlist_index=index,
                            playlist_count=len(messages),
                        )
                    )
                item = await self._materialize(
                    message,
                    source,
                    output_dir,
                    cancel_check=cancel_check,
                )
                if item:
                    items.append(item)
            if not items:
                raise RestrictedContentError(
                    "Those messages contain no supported media or text."
                )
            if progress:
                await progress(
                    ProgressSnapshot(
                        percent=100,
                        status="complete",
                        title=f"{len(items)} Telegram message(s)",
                    )
                )
            completed = True
            return DownloadResult(
                items=items,
                url=source.url,
                item_count=len(items),
            )
        except asyncio.CancelledError:
            raise
        except RestrictedContentError:
            raise
        except Exception as exc:
            raise RestrictedContentError(
                "Telegram could not retrieve that content. Confirm the authorized "
                "account can access the source chat and try again."
            ) from exc
        finally:
            # The queue callback owns successful items. Any failed or cancelled
            # retrieval must remove both partial files and its private directory.
            if not completed:
                for item in items:
                    item.path.unlink(missing_ok=True)
                shutil.rmtree(output_dir, ignore_errors=True)

    async def inspect(self, source_url: str) -> dict[str, object]:
        """Check access and message availability without downloading media."""
        source = parse_restricted_source(source_url)
        if not source:
            raise RestrictedContentError(
                "Send a Telegram message link such as "
                "https://t.me/channel/123 or https://t.me/c/123456/123."
            )
        client = self._client_provider()
        if client is None:
            raise RestrictedContentError(
                "Restricted Telegram retrieval is not configured. "
                "Authorize the dedicated restricted-content user session with "
                "<code>/rauthorize</code> first."
            )
        try:
            chat = await self._retry_telegram(
                lambda: client.get_chat(source.chat_id)
            )
            messages = await self._get_messages(client, source)
        except RestrictedContentError:
            raise
        except RPCError as exc:
            raise self._access_error(exc, source) from exc
        return {
            "source": source,
            "chat_title": getattr(chat, "title", None)
            or getattr(chat, "first_name", None)
            or str(source.chat_id),
            "message_count": len(messages),
            "media_count": sum(
                1 for message in messages if _message_media(message)
            ),
        }

    async def _get_messages(
        self,
        client: Client,
        source: RestrictedSource,
    ) -> list[object]:
        try:
            await self._retry_telegram(
                lambda: client.get_chat(source.chat_id)
            )
            result = await self._retry_telegram(
                lambda: client.get_messages(
                    source.chat_id,
                    list(range(source.start_id, source.end_id + 1)),
                )
            )
        except RPCError as exc:
            raise self._access_error(exc, source) from exc
        if not isinstance(result, list):
            result = [result]
        return [
            message
            for message in result
            if message is not None and not getattr(message, "empty", False)
        ]

    @staticmethod
    def _access_error(exc: Exception, source: RestrictedSource) -> RestrictedContentError:
        error_name = type(exc).__name__
        if error_name in {
            "ChannelInvalid",
            "ChannelPrivate",
            "ChatNotFound",
            "Forbidden",
            "PeerIdInvalid",
            "UserNotParticipant",
            "UsernameNotOccupied",
        }:
            if source.is_private_link:
                return RestrictedContentError(
                    "The authorized Telegram account cannot access this private chat. "
                    "Join the source channel/group with that same user account, "
                    "then restart the bot. The bot cannot bypass Telegram membership "
                    "or invite restrictions."
                )
            return RestrictedContentError(
                "The authorized Telegram account cannot access that Telegram chat. "
                "Confirm the public username and account permissions."
            )
        return RestrictedContentError(
            "Telegram could not access that chat or message. "
            "Confirm the authorized account can view the source."
        )

    async def _expand_media_groups(
        self,
        client: Client,
        source: RestrictedSource,
        messages: list[object],
    ) -> list[object]:
        by_id: dict[int, object] = {}
        for message in messages:
            message_id = int(getattr(message, "id", 0) or 0)
            if message_id:
                by_id[message_id] = message
            media_group_id = getattr(message, "media_group_id", None)
            if not media_group_id:
                continue
            try:
                group = await self._retry_telegram(
                    lambda: client.get_media_group(source.chat_id, message_id)
                )
            except RPCError:
                continue
            for grouped in group:
                grouped_id = int(getattr(grouped, "id", 0) or 0)
                if grouped_id:
                    by_id[grouped_id] = grouped
                    if len(by_id) >= self.max_messages:
                        break
            if len(by_id) >= self.max_messages:
                break
        return [by_id[key] for key in sorted(by_id)]

    async def _materialize(
        self,
        message: object,
        source: RestrictedSource,
        output_dir: Path,
        *,
        cancel_check: CancelCheck | None,
    ) -> DownloadItem | None:
        message_id = int(getattr(message, "id", 0) or 0)
        media = _message_media(message)
        item_url = f"{source.url}#message={message_id}"
        if media:
            name, declared_size, _mime_type, audio_only = media
            if declared_size > self.max_bytes:
                raise RestrictedContentError(
                    "A Telegram message exceeds the configured download limit."
                )
            target = output_dir / f"{message_id}-{Path(name).name}"
            try:
                downloaded = await self._retry_telegram(
                    lambda: message.download(file_name=str(target))
                )
            except RPCError as exc:
                raise RestrictedContentError(
                    "Telegram refused access to one of the requested messages."
                ) from exc
            path = Path(downloaded) if downloaded else target
            if not path.exists() or not path.is_file():
                raise RestrictedContentError(
                    "Telegram did not return a usable media file."
                )
            if path.stat().st_size > self.max_bytes:
                path.unlink(missing_ok=True)
                raise RestrictedContentError(
                    "A Telegram message exceeds the configured download limit."
                )
            title = Path(name).stem or f"Telegram message {message_id}"
            return DownloadItem(
                path=path,
                title=title[:180],
                url=item_url,
                duration=getattr(getattr(message, "video", None), "duration", None),
                audio_only=audio_only,
            )

        text = str(
            getattr(message, "text", None)
            or getattr(message, "caption", None)
            or ""
        ).strip()
        if not text:
            return None
        if cancel_check and cancel_check():
            raise DownloadCancelled("Download cancelled.")
        target = output_dir / f"{message_id}-telegram-message.txt"
        target.write_text(text, encoding="utf-8")
        return DownloadItem(
            path=target,
            title=f"Telegram message {message_id}",
            url=item_url,
            duration=None,
            audio_only=False,
        )

    @staticmethod
    async def _retry_telegram(operation: Callable[[], Awaitable[object]]) -> object:
        """Wait once for a short FloodWait, then fail with a safe bounded error."""
        for attempt in range(2):
            try:
                return await operation()
            except FloodWait as exc:
                wait_seconds = max(0, int(getattr(exc, "value", 0) or 0))
                if attempt or wait_seconds > 30:
                    raise RestrictedContentError(
                        "Telegram is temporarily rate-limiting this account. "
                        "Please wait and try again."
                    ) from exc
                await asyncio.sleep(wait_seconds)
        raise RestrictedContentError(
            "Telegram could not retrieve that content right now."
        )
