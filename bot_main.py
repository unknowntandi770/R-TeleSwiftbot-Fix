from __future__ import annotations

import asyncio
import html
import logging
import re
import shutil
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4
from urllib.parse import urlparse

from pyrogram import Client, StopPropagation, filters
from pyrogram.errors import RPCError
from pyrogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ChatAdministratorRights,
)

from bot_cache import cache_key, create_cache
from bot_config import Settings
from bot_cookies import CookieStore, looks_like_cookie_document
from bot_downloader import (
    DownloadCancelled,
    DownloadError,
    DownloadItem,
    DownloadResult,
    ProgressSnapshot,
    SearchResult,
    YTDLPDownloader,
)
from bot_file_links import FileLinkServer, FileLinkStore
from bot_file_store import FileStore, StoredFile
from bot_mongodb_store import MongoFileStore
from bot_health import HealthServer
from bot_voice_chat import VoiceChatController
from bot_quality import (
    AUDIO_QUALITIES,
    STREAM_VIDEO_QUALITIES,
    VIDEO_QUALITIES,
    normalize_quality,
    normalize_stream_quality,
    quality_label,
    stream_quality_label,
)
from bot_queue import DownloadJob, DownloadQueue, UserQueueBusy
from bot_restricted import (
    RestrictedContentError,
    RestrictedMessageDownloader,
    RestrictedSessionManager,
    parse_restricted_source,
)
from bot_urls import (
    extract_source,
    extract_url,
    is_playlist_url,
    is_stream_manifest,
    is_youtube_url,
    is_torrent_source,
    is_torrent_url,
    normalize_url,
    source_kind,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("ytdlbot")

TRANSIENT_CLEANUP_INTERVAL_SECONDS = 5 * 60
TRANSIENT_MIN_AGE_SECONDS = 6 * 60 * 60
TRANSIENT_ROOT_NAME_RE = re.compile(r"^[0-9a-f]{32}$")

BRAND = "YouTube Studio"
DIVIDER = "━━━━━━━━━━━━━━━━━━━━"
OWNER_HANDLE = "@ashish_tandi110"
OWNER_INSTAGRAM = "https://instagram.com/ashish_tandi110"
STARTUP_BANNER = r"""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   █████╗  ███████╗██╗  ██╗██╗███████╗██╗  ██╗              ║
║  ██╔══██╗ ██╔════╝██║  ██║██║██╔════╝██║  ██║              ║
║  ███████║ ███████╗███████║██║███████╗███████║              ║
║  ██╔══██║ ╚════██║██╔══██║██║╚════██║██╔══██║              ║
║  ██║  ██║ ███████║██║  ██║██║███████║██║  ██║              ║
║  ╚═╝  ╚═╝ ╚══════╝╚═╝  ╚═╝╚═╝╚══════╝╚═╝  ╚═╝              ║
║                                                              ║
║              YouTube Studio • Telegram Media Bot            ║
║              Owner: @ashish_tandi110                        ║
╚══════════════════════════════════════════════════════════════╝
"""


@dataclass
class PendingChoice:
    url: str
    user_id: int
    chat_id: int


@dataclass
class PendingSearch:
    query: str
    results: list[SearchResult]
    user_id: int
    chat_id: int


@dataclass
class PendingFileUpload:
    user_id: int
    chat_id: int
    expires_at: float


@dataclass
class PendingRestrictedAuthorization:
    chat_id: int
    phase: str
    expires_at: float
    processing: bool = False


@dataclass
class PendingVoiceChoice:
    message: Message
    user_id: int
    chat_id: int


class Bot:
    def __init__(self, settings: Settings) -> None:
        settings.prepare_directories()
        self.settings = settings
        self.cookies = CookieStore(settings.cookie_dir, settings.cookie_key)
        self.cache = None
        self.queue: DownloadQueue | None = None
        self.downloader: YTDLPDownloader | None = None
        self.file_links: FileLinkStore | None = None
        self.file_link_server: FileLinkServer | None = None
        self.file_store: FileStore | None = None
        self.metadata_store_name = "starting"
        self.voice_chat = VoiceChatController(
            settings.api_id,
            settings.api_hash,
            settings.vc_session_path,
            settings.work_dir,
            self.cookies,
            settings.pot_provider_url,
            session_string=settings.vc_session_string,
            max_queue_size=settings.max_queue_size,
        )
        self.restricted_session = RestrictedSessionManager(
            settings.api_id,
            settings.api_hash,
            settings.restricted_session_path,
        )
        self.restricted = RestrictedMessageDownloader(
            lambda: self.restricted_session.authorized_client,
            settings.work_dir,
            settings.max_download_bytes,
            settings.restricted_max_messages,
        )
        self.vc_chat_id = settings.vc_chat_id
        self.app: Client | None = None
        self.pending_choices: dict[str, PendingChoice] = {}
        self.pending_searches: dict[str, PendingSearch] = {}
        self.pending_file_uploads: dict[int, PendingFileUpload] = {}
        self.pending_restricted_auth: dict[int, PendingRestrictedAuthorization] = {}
        self.pending_voice_choices: dict[str, PendingVoiceChoice] = {}
        self._restricted_auth_lock = asyncio.Lock()
        self._restricted_auth_io_lock = asyncio.Lock()
        self.pending_music_searches: set[int] = set()
        self.pending_store_searches: set[int] = set()
        self._archive_locks: dict[str, asyncio.Lock] = {}
        self._store_locks: dict[str, asyncio.Lock] = {}
        self._telegram_media_locks: dict[int, asyncio.Lock] = {}
        self._pending_cleanup_task: asyncio.Task[None] | None = None
        self._file_links_unavailable = False
        self._ready = False
        self.health_server = HealthServer(
            settings.health_host,
            settings.health_port,
            self.health_snapshot,
        )

    def health_snapshot(self) -> dict[str, object]:
        return {
            "status": "ready" if self._ready else "starting",
            "ready": self._ready,
            "telegram_connected": bool(self.app and self.app.is_connected),
            "queue_workers": len(self.queue._tasks) if self.queue else 0,
            "file_streamer": bool(self.file_link_server),
            "metadata_store": self.metadata_store_name,
        }

    @staticmethod
    def _home_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⬇️ Download", callback_data="yt:download:menu"),
                    InlineKeyboardButton("🎧 Find music", callback_data="yt:music:menu"),
                ],
                [
                    InlineKeyboardButton("📁 File tools", callback_data="yt:files:menu"),
                    InlineKeyboardButton("🎙 Voice chat", callback_data="yt:voice:menu"),
                ],
                [
                    InlineKeyboardButton("📋 Activity", callback_data="yt:queue:menu"),
                    InlineKeyboardButton("⚙️ Settings", callback_data="yt:settings:menu"),
                ],
                [
                    InlineKeyboardButton("❓ Help", callback_data="yt:help:menu"),
                    InlineKeyboardButton("🧰 Advanced", callback_data="yt:advanced:menu"),
                ],
            ]
        )

    @staticmethod
    def _welcome_text() -> str:
        return (
            f"👋 <b>Welcome to {BRAND}</b>\n"
            f"{DIVIDER}\n"
            "A fast, private media assistant for Telegram.\n\n"
            f"👑 Owner: <a href=\"{OWNER_INSTAGRAM}\">{OWNER_HANDLE}</a>\n\n"
            "<b>Start here</b>\n"
            "• Paste a YouTube link to download it.\n"
            "• Tap <b>Find music</b> to search by artist or song.\n"
            "• Reply to a Telegram file to create a link, save it, or play it.\n\n"
            "You do not need to remember commands — use the buttons below.\n\n"
            "✨ Video thumbnails · ⚡ Playlists · 🔒 Encrypted cookies\n"
            "Use <code>/help</code> anytime for all commands and examples."
        )

    @staticmethod
    def _help_text() -> str:
        return (
            f"📖 <b>Simple guide</b>\n"
            f"{DIVIDER}\n"
            "<b>Download</b>\n"
            "Paste a YouTube, direct media, Google Drive, HLS/DASH, or playlist "
            "link. Pick <b>Video</b> or <b>MP3</b>, then choose quality.\n"
            "Example: <code>/ytdl https://youtu.be/…</code>\n\n"
            "<b>Music</b>\n"
            "Open <b>Find music</b>, type an artist or song, choose a result, "
            "and select MP3.\n\n"
            "<b>Telegram files</b>\n"
            "Reply to a file and choose <b>File tools</b> for temporary links, "
            "permanent storage, or retrieval.\n"
            "Examples: <code>/filestream</code> · <code>/store</code> · "
            "<code>/mirror</code>\n\n"
            "For accessible protected Telegram messages, use <code>/save</code>, "
            "or pass a <code>t.me</code> message link to <code>/mirror</code>, "
            "<code>/filestream</code>, or <code>/store</code>.\n\n"
            "<b>Voice chat</b>\n"
            "Use <code>/vplay audio URL</code> or <code>/vplay video URL</code> "
            "to start playback. Replied Telegram video is also supported.\n"
            "Controls: <code>/vcpanel</code>, <code>/vqueue</code>, "
            "<code>/vpause</code>, <code>/vresume</code>, <code>/vskip</code>, "
            "<code>/vstop</code>, <code>/vclear</code>.\n\n"
            "<b>Mirror / leech</b>\n"
            "Use <code>/mirror URL</code> for media and <code>/leech magnet:?…</code> "
            "for torrents. Repeated files reuse the Telegram archive.\n\n"
            "<b>Restricted Telegram messages</b>\n"
            "Use <code>/save https://t.me/channel/123</code> or private "
            "<code>/save https://t.me/c/123456/123</code>.\n"
            "Check access first with <code>/savecheck LINK</code>. Ranges and "
            "albums are bounded and archived copies are reused.\n\n"
            "<b>Cookies and account access</b>\n"
            "<code>/cookies</code> · <code>/cookie-status</code> · "
            "<code>/deletecookies</code> · <code>/rauthorize</code>\n\n"
            "<b>Storage</b>\n"
            "<code>/myfiles</code> · <code>/store_search TEXT</code> · "
            "<code>/store_stats</code>\n\n"
            "Need a button-based flow? Send <code>/start</code> and use the menu."
        )

    @staticmethod
    def _download_text() -> str:
        return (
            "⬇️ <b>Download media</b>\n"
            f"{DIVIDER}\n"
            "The easiest way: paste a supported media link in this chat.\n\n"
            "Then choose:\n"
            "• <b>Video</b> for picture + sound\n"
            "• <b>MP3</b> for audio only\n\n"
            "For advanced users, <code>/mirror URL</code> and <code>/leech URL</code> "
            "start the same bounded download-and-upload flow.\n\n"
            "You can also use the buttons below for a quick explanation."
        )

    @staticmethod
    def _download_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🎵 Find music instead", callback_data="yt:music:menu")],
                [InlineKeyboardButton("📋 See my activity", callback_data="yt:queue:menu")],
                [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
            ]
        )

    @staticmethod
    def _music_text() -> str:
        return (
            "🎧 <b>Find music</b>\n"
            f"{DIVIDER}\n"
            "Type what you want to hear, for example:\n"
            "<code>Daft Punk Get Lucky</code>\n\n"
            "I’ll show matching results. Tap one, then choose MP3 or video.\n\n"
            "You can also paste a direct YouTube link if you already know it."
        )

    @staticmethod
    def _music_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬇️ Download from a link", callback_data="yt:download:menu")],
                [InlineKeyboardButton("📋 See my activity", callback_data="yt:queue:menu")],
                [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
            ]
        )

    @staticmethod
    def _files_text() -> str:
        return (
            "📁 <b>File tools</b>\n"
            f"{DIVIDER}\n"
            "Reply to any supported Telegram media file with the matching command:\n\n"
            "🔗 <code>/filestream</code> — temporary stream and download links\n"
            "📦 <code>/store</code> — save permanently and get a share link\n"
            "📚 <code>/myfiles</code> — list your saved files\n\n"
            "Cookies are protected separately. Never put a browser cookie file "
            "in the permanent store."
        )

    @staticmethod
    def _files_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📚 My saved files", callback_data="yt:files_list:menu")],
                [InlineKeyboardButton("🔎 Search saved files", callback_data="yt:files_search:menu")],
                [InlineKeyboardButton("📊 Storage stats", callback_data="yt:files_stats:menu")],
                [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
            ]
        )

    @staticmethod
    def _voice_text() -> str:
        return (
            "🎙 <b>Voice chat</b>\n"
            f"{DIVIDER}\n"
            "Play YouTube audio/video or replied Telegram audio/video in your "
            "group’s video chat.\n\n"
            "1. Use <code>/vplay audio …</code> or <code>/vplay video …</code>.\n"
            "2. The assistant creates and joins the group video chat automatically.\n"
            "3. Use the control panel to pause, skip, loop, or clear the queue.\n\n"
            "Voice controls are limited to group administrators."
        )

    @staticmethod
    def _parse_vplay_request(command_text: str) -> tuple[str, str]:
        args = command_text.split()
        mode = "audio"
        if len(args) > 1 and args[1].lower() in {"video", "--video", "-v"}:
            mode = "video"
            args.pop(1)
        elif len(args) > 1 and args[1].lower() in {"audio", "--audio", "-a"}:
            args.pop(1)
        source = " ".join(args[1:]).strip()
        if len(source) >= 2 and source[0] == source[-1] and source[0] in {"'", '"'}:
            source = source[1:-1].strip()
        return mode, source

    @staticmethod
    def _infer_vplay_mode(mode: str, source: str) -> str:
        """Default explicit media URLs to video when they are video sources.

        ``/vplay`` historically defaulted to audio for search text. Keeping
        that behavior is useful, but a bare video URL should not silently
        discard its video track. Explicit audio/video modes always win.
        """
        if mode != "audio" or not source:
            return mode
        parsed = urlparse(source)
        if parsed.scheme.lower() not in {"http", "https"}:
            return mode
        host = (parsed.hostname or "").lower().rstrip(".")
        if (
            host == "youtu.be"
            or host.endswith(".youtube.com")
            or host == "youtube.com"
            or host in {
                "drive.google.com",
                "docs.google.com",
                "drive.usercontent.google.com",
            }
        ):
            return "video"
        if is_stream_manifest(source):
            return "video"
        video_extensions = {
            ".3g2",
            ".3gp",
            ".avi",
            ".flv",
            ".m2ts",
            ".m4v",
            ".mkv",
            ".mov",
            ".mp4",
            ".mpeg",
            ".mpg",
            ".ogv",
            ".ts",
            ".webm",
            ".wmv",
            ".m3u8",
            ".mpd",
        }
        if Path(parsed.path).suffix.lower() in video_extensions:
            return "video"
        return mode

    @staticmethod
    def _parse_leech_request(command_text: str) -> tuple[str, str | None]:
        """Parse a magnet and optional aria2 file selection from /leech."""
        tokens = command_text.split()
        select_files: str | None = None
        source: str | None = None
        index = 1
        while index < len(tokens):
            token = tokens[index]
            lowered = token.lower()
            if lowered in {"--select", "--select-file", "-s"}:
                if index + 1 >= len(tokens):
                    raise ValueError("Add file numbers after --select, for example 1,3-5.")
                select_files = tokens[index + 1].replace(" ", "")
                index += 2
                continue
            if lowered.startswith("--select=") or lowered.startswith("--select-file="):
                select_files = token.split("=", 1)[1].replace(" ", "")
                index += 1
                continue
            if token.lower().startswith("magnet:?") or is_torrent_url(token):
                source = token
                index += 1
                continue
            index += 1
        if not source:
            return "", select_files
        if select_files and not re.fullmatch(
            r"\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*",
            select_files,
        ):
            raise ValueError("File selection must look like 1,3-5.")
        return source, select_files

    @staticmethod
    def _vplay_mode_is_explicit(command_text: str) -> bool:
        args = command_text.split()
        return len(args) > 1 and args[1].lower() in {
            "audio",
            "--audio",
            "-a",
            "video",
            "--video",
            "-v",
        }

    @staticmethod
    def _replied_media_mode(
        file_name: str,
        mime_type: str | None,
    ) -> str:
        normalized_mime = (mime_type or "").split(";", 1)[0].strip().lower()
        if normalized_mime.startswith("video/"):
            return "video"
        if normalized_mime.startswith("audio/"):
            return "audio"
        extension = Path(file_name).suffix.lower()
        if extension in {
            ".3g2",
            ".3gp",
            ".avi",
            ".flv",
            ".m2ts",
            ".m4v",
            ".mkv",
            ".mov",
            ".mp4",
            ".mpeg",
            ".mpg",
            ".ogv",
            ".ts",
            ".webm",
            ".wmv",
        }:
            return "video"
        return "audio"

    @staticmethod
    def _voice_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🎛 Open control panel", callback_data="yt:voice_panel:menu")],
                [InlineKeyboardButton("🔎 Check voice setup", callback_data="yt:voice_status:menu")],
                [InlineKeyboardButton("⚙️ Setup assistant", callback_data="yt:voice_setup:menu")],
                [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
            ]
        )

    @staticmethod
    def _settings_text() -> str:
        return (
            "⚙️ <b>Settings</b>\n"
            f"{DIVIDER}\n"
            "Manage optional YouTube browser cookies for age-restricted videos "
            "or access checks.\n\n"
            "Cookie values are encrypted and never displayed. They are separate "
            "from permanent file storage."
        )

    @staticmethod
    def _settings_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🍪 Cookie instructions", callback_data="yt:cookies:menu")],
                [InlineKeyboardButton("🔐 Check cookie status", callback_data="yt:cookie_status:menu")],
                [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
            ]
        )

    @staticmethod
    def _advanced_text() -> str:
        return (
            "🧰 <b>Advanced commands</b>\n"
            f"{DIVIDER}\n"
            "<b>Downloads</b>\n"
            "<code>/audio</code> <code>/search</code> <code>/song</code> "
            "<code>/queue</code> <code>/cancel</code>\n\n"
            "<b>Files</b>\n"
            "<code>/filestream</code> <code>/store</code> <code>/myfiles</code> "
            "<code>/store_search</code> <code>/store_stats</code>\n\n"
            "<b>Cookies</b>\n"
            "<code>/cookies</code> <code>/cookie_status</code> "
            "<code>/deletecookies</code>\n\n"
            "<b>Voice</b>\n"
            "<code>/vplay</code> <code>/vqueue</code> <code>/vcpanel</code> "
            "<code>/vpause</code> <code>/vresume</code> <code>/vskip</code> "
            "<code>/vstop</code> <code>/vseek</code> <code>/vvolume</code> "
            "<code>/vloop</code> <code>/vclear</code> <code>/vcsetup</code> "
            "<code>/vcstatus</code>"
        )

    @staticmethod
    def _section_nav(*, include_help: bool = True) -> InlineKeyboardMarkup:
        rows = [
            [
                InlineKeyboardButton("⬇️ Download", callback_data="yt:download:menu"),
                InlineKeyboardButton("🎧 Music", callback_data="yt:music:menu"),
            ],
            [
                InlineKeyboardButton("📁 Files", callback_data="yt:files:menu"),
                InlineKeyboardButton("🎙 Voice", callback_data="yt:voice:menu"),
            ],
        ]
        if include_help:
            rows.append(
                [
                    InlineKeyboardButton("⚙️ Settings", callback_data="yt:settings:menu"),
                    InlineKeyboardButton("🧰 Advanced", callback_data="yt:advanced:menu"),
                ]
            )
        rows.append([InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")])
        return InlineKeyboardMarkup(rows)

    @staticmethod
    def _cookie_help_text() -> str:
        return (
            "🍪 <b>Secure cookie connection</b>\n"
            f"{DIVIDER}\n"
            "Cookies can help with age-restricted videos and YouTube access checks.\n\n"
            "<b>Fast path</b>\n"
            "1. Export cookies from a browser currently signed in to YouTube.\n"
            "2. Send the JSON or Netscape cookies.txt file here as a document.\n"
            "3. Retry your YouTube link.\n\n"
            "🔒 Values are encrypted before storage.\n"
            "🚫 Cookie values are never displayed.\n"
            "🗑 Use Delete cookies whenever you want to remove them."
        )

    def _create_client(self) -> Client:
        app = Client(
            "ytdlbot",
            api_id=self.settings.api_id,
            api_hash=self.settings.api_hash,
            bot_token=self.settings.bot_token,
            workdir=str(self.settings.work_dir),
        )
        app.on_message(filters.all, group=-1)(self.enforce_user_ban)
        app.on_callback_query(group=-1)(self.enforce_callback_ban)
        app.on_message(filters.command("start"))(self.start)
        app.on_message(filters.command(["help", "about"]))(self.help)
        app.on_message(filters.command("queue"))(self.queue_status)
        app.on_message(filters.command("cancel"))(self.cancel_command)
        app.on_message(filters.command("cookies"))(self.cookies_help)
        app.on_message(filters.command(["cookie-status", "cookie_status", "cookiestatus"]))(self.cookie_status)
        app.on_message(filters.command("deletecookies"))(self.delete_cookies)
        app.on_message(filters.command(["vplay", "vcplay"]))(self.vc_play)
        app.on_message(filters.command(["vqueue", "vcqueue"]))(self.vc_queue)
        app.on_message(filters.command(["vcpanel", "vccontrol"]))(self.vc_panel)
        app.on_message(filters.command(["vpause", "vcpause"]))(self.vc_pause)
        app.on_message(filters.command(["vresume", "vcresume"]))(self.vc_resume)
        app.on_message(filters.command(["vskip", "vcskip"]))(self.vc_skip)
        app.on_message(filters.command(["vstop", "vcstop"]))(self.vc_stop)
        app.on_message(filters.command(["vseek", "vcseek"]))(self.vc_seek)
        app.on_message(filters.command(["vvolume", "vcvolume"]))(self.vc_volume)
        app.on_message(filters.command(["vloop", "vcloop"]))(self.vc_loop)
        app.on_message(filters.command(["vclear", "vcclear"]))(self.vc_clear)
        app.on_message(filters.command(["ytdl", "youtube", "yt"]))(self.ytdl_command)
        app.on_message(filters.command("vcsetup"))(self.vc_setup)
        app.on_message(filters.command("vcstatus"))(self.vc_status)
        app.on_message(filters.command("rauthorize"))(self.restricted_authorize_command)
        app.on_message(filters.command("admin"))(self.admin_command)
        app.on_message(filters.command("admin_cancel_all"))(self.admin_cancel_all)
        app.on_message(filters.command("ban"))(self.ban_command)
        app.on_message(filters.command("unban"))(self.unban_command)
        app.on_message(filters.command("filestream"))(
            self.file_url_command
        )
        app.on_message(filters.command(["save", "savechat", "saverestricted"]))(
            self.save_restricted_command
        )
        app.on_message(filters.command(["savecheck", "checksave"]))(
            self.save_restricted_check_command
        )
        app.on_message(filters.command(["mirror", "leech"]))(self.mirror_command)
        app.on_message(filters.command("store"))(self.store_command)
        app.on_message(filters.command(["myfiles", "store_list"]))(self.my_files_command)
        app.on_message(filters.command(["store_search", "filesearch"]))(
            self.store_search_command
        )
        app.on_message(filters.command("store_stats"))(self.store_stats_command)
        upload_media = (
            filters.document
            | filters.video
            | filters.audio
            | filters.photo
            | filters.animation
            | filters.voice
            | filters.video_note
        )
        app.on_message(upload_media)(self.receive_media)
        app.on_message(filters.command("audio"))(self.audio_command)
        app.on_message(filters.command(["search", "song"]))(self.search_command)
        app.on_message(
            filters.private & filters.text & ~filters.regex(r"^/")
        )(self.restricted_authorization_input)
        app.on_message(filters.text & ~filters.regex(r"^/"))(self.url_message)
        app.on_callback_query()(self.format_choice)
        return app

    @staticmethod
    def _is_revoked_bot_session(error: BaseException) -> bool:
        """Identify a stale bot session without masking invalid credentials."""
        return type(error).__name__ in {
            "SessionRevoked",
            "AuthKeyUnregistered",
            "SessionExpired",
        }

    @staticmethod
    def _is_expired_bot_token(error: BaseException) -> bool:
        return type(error).__name__ in {
            "AccessTokenExpired",
            "AccessTokenInvalid",
            "Unauthorized",
        }

    async def _start_bot_client(self) -> Client:
        """Start the bot, recovering once from a revoked cached session.

        Telegram bot authentication is backed by a local Pyrogram session
        database. If an operator terminates all Telegram sessions, that local
        database becomes unusable even though BOT_TOKEN is still valid. Remove
        only this bot's session and retry; user sessions used by restricted
        retrieval and voice playback must remain untouched.
        """
        app = self._create_client()
        try:
            await app.start()
            return app
        except RPCError as exc:
            if self._is_expired_bot_token(exc):
                raise RuntimeError(
                    "BOT_TOKEN was rejected by Telegram. Create a new token "
                    "with @BotFather and replace the BOT_TOKEN secret on the host."
                ) from exc
            if not self._is_revoked_bot_session(exc):
                raise
            try:
                if app.is_connected:
                    await app.stop()
            except Exception:
                logger.debug(
                    "Could not stop revoked bot session cleanly",
                    exc_info=True,
                )
            session_base = self.settings.work_dir / "ytdlbot.session"
            removed = 0
            for path in (
                session_base,
                session_base.with_name(session_base.name + "-journal"),
                session_base.with_name(session_base.name + "-shm"),
                session_base.with_name(session_base.name + "-wal"),
            ):
                try:
                    if path.exists():
                        path.unlink()
                        removed += 1
                except OSError:
                    logger.warning("Could not remove stale bot session file %s", path)
            logger.warning(
                "Cached Telegram bot session was revoked; removed %s session "
                "file(s) and retrying with BOT_TOKEN",
                removed,
            )
            replacement = self._create_client()
            try:
                await replacement.start()
            except RPCError as retry_error:
                if self._is_expired_bot_token(retry_error):
                    raise RuntimeError(
                        "BOT_TOKEN was rejected by Telegram. Create a new token "
                        "with @BotFather and replace the BOT_TOKEN secret on the host."
                    ) from retry_error
                raise
            except Exception:
                logger.exception(
                    "Telegram bot could not start after refreshing its cached "
                    "session. Check API_ID, API_HASH, and BOT_TOKEN."
                )
                raise
            return replacement

    async def ytdl_command(self, _: Client, message: Message) -> None:
        source = extract_url(message.text or message.caption or "")
        if not source:
            await message.reply_text(
                "Usage: <code>/ytdl YouTube video or playlist URL</code>"
            )
            return
        if not is_youtube_url(source):
            await message.reply_text(
                "Please use a YouTube video or playlist URL with <code>/ytdl</code>."
            )
            return
        await self._send_link_ack(message, source)
        await self.choose_format(message, source)

    async def vc_play(
        self,
        _: Client,
        message: Message,
        *,
        stream_quality: str | None = None,
    ) -> None:
        if not await self._vc_admin_allowed(message):
            return
        target_chat_id = self._vc_target_chat_id(message)
        command_text = message.text or message.caption or ""
        mode, source = self._parse_vplay_request(command_text)
        if source and not self._vplay_mode_is_explicit(command_text):
            mode = self._infer_vplay_mode(mode, source)
        replied = message.reply_to_message
        replied_media = self._media_details(replied) if replied else None
        if replied_media and not source and not self._vplay_mode_is_explicit(command_text):
            mode = self._replied_media_mode(replied_media[0], replied_media[2])
        restricted_source = parse_restricted_source(source or "")
        if not source and not replied_media:
            await message.reply_text(
                "🎙 <b>Voice chat playback</b>\n\n"
                "Usage:\n"
                "• <code>/vplay audio YouTube URL or search text</code>\n"
                "• <code>/vplay video YouTube URL or any video URL</code>\n"
                "• <code>/vplay video restricted Telegram link</code>\n"
                "• Reply to Telegram audio/video with <code>/vplay</code>\n\n"
                "Video playback opens a quality chooser. "
                "<b>Original</b> means the highest resolution published by the source; "
                "it never upscales a lower-quality file.\n\n"
                "The group video chat starts automatically when playback begins."
            )
            return
        needs_quality = (
            stream_quality is None
            and (
                mode == "video"
                or restricted_source is not None
                or (replied_media is not None and mode == "video")
            )
        )
        if needs_quality:
            await self._show_voice_quality_menu(
                message,
                user_id=message.from_user.id if message.from_user else 0,
            )
            return
        stream_quality = normalize_stream_quality(stream_quality)
        detected_kind = source_kind(source) if source else "telegram"
        kind_label = {
            "youtube": "YouTube",
            "manifest": "HLS/DASH",
            "drive": "Google Drive",
            "direct": "direct media",
            "search": "search",
            "telegram": "Telegram media",
            "restricted": "restricted Telegram media",
        }.get(detected_kind, "media")
        if restricted_source:
            kind_label = "restricted Telegram media"
        status = await message.reply_text(
            f"🔎 <b>Resolving {mode} stream…</b>\n"
            f"Source: <b>{kind_label}</b>"
        )
        downloaded_path: Path | None = None
        restricted_items: list[DownloadItem] = []
        transferred_paths: set[Path] = set()
        try:
            requester_id = message.from_user.id if message.from_user else 0
            if restricted_source:
                restricted_result = await self._download_restricted(
                    restricted_source.url,
                    requester_id,
                    progress=None,
                    cancel_check=lambda: False,
                    use_archive=False,
                )
                restricted_items = restricted_result.items
                if not restricted_items:
                    raise RestrictedContentError(
                        "The Telegram link did not contain playable media."
                    )
                # A bare Telegram message link has no extension that can tell
                # us whether it is audio or A/V. Inspect the retrieved media
                # before choosing the PyTgCalls stream type.
                if not self._vplay_mode_is_explicit(command_text):
                    mode = (
                        "video"
                        if any(not item.audio_only for item in restricted_items)
                        else "audio"
                    )
                playable_items = [
                    item for item in restricted_items
                    if not (mode == "video" and item.audio_only)
                ]
                if not playable_items:
                    raise ValueError(
                        "The Telegram link contains audio only. Use "
                        "<code>/vplay audio</code> for this content."
                    )
                for item in playable_items:
                    track, position = await self.voice_chat.add_file(
                        target_chat_id,
                        item.path,
                        item.title,
                        requester_id,
                        video=mode == "video",
                        stream_quality=stream_quality,
                    )
                    # The voice worker owns the temporary restricted download
                    # after it is queued; no archive/bin-channel copy is made.
                    transferred_paths.add(item.path)
                downloaded_path = None
            elif replied_media and not source:
                file_name, _, mime_type, _ = replied_media
                mode_from_file = self._replied_media_mode(file_name, mime_type)
                normalized_mime = (mime_type or "").split(";", 1)[0].strip().lower()
                playable = (
                    not normalized_mime
                    or normalized_mime.startswith("audio/")
                    or normalized_mime.startswith("video/")
                    or normalized_mime
                    in {"application/ogg", "application/octet-stream"}
                )
                if not playable:
                    raise ValueError(
                        "Reply to an audio, video, voice message, or playable document."
                    )
                if mode == "video" and mode_from_file == "audio":
                    raise ValueError(
                        "This reply contains audio only. Use /vplay audio or reply "
                        "to a video file for video playback."
                    )
                if not replied:
                    raise ValueError("The replied file is unavailable.")
                target_dir = self.settings.work_dir / "voice-chat"
                target_dir.mkdir(parents=True, exist_ok=True)
                target = target_dir / f"{uuid4().hex}-{Path(file_name).name}"
                downloaded = await replied.download(file_name=str(target))
                downloaded_path = Path(downloaded) if downloaded else target
                if not downloaded_path.exists():
                    raise ValueError("Telegram did not return the media file.")
                track, position = await self.voice_chat.add_file(
                    target_chat_id,
                    downloaded_path,
                    Path(file_name).stem or "Telegram audio",
                    requester_id,
                    video=mode == "video",
                    stream_quality=stream_quality,
                )
                # The voice-chat worker owns this file and deletes it after playback.
                downloaded_path = None
            else:
                track, position = await self.voice_chat.add(
                    target_chat_id,
                    source,
                    requester_id,
                    video=mode == "video",
                    stream_quality=stream_quality,
                )
        except RuntimeError as exc:
            await status.edit_text(
                "⚙️ <b>Voice chat setup needed</b>\n\n"
                f"{html.escape(str(exc))}"
            )
            return
        except ValueError as exc:
            await status.edit_text(f"❌ <b>{html.escape(str(exc))}</b>")
            return
        except RestrictedContentError as exc:
            await status.edit_text(f"❌ <b>{html.escape(str(exc))}</b>")
            return
        except Exception as exc:
            logger.exception("VC play failed for chat %s", message.chat.id)
            await status.edit_text(
                "❌ <b>Could not start voice chat playback</b>\n\n"
                f"{html.escape(self.voice_chat.user_error(exc))}\n\n"
                "Check that the assistant is a member of this group and has "
                "permission to manage video chats."
            )
            return
        finally:
            if downloaded_path:
                downloaded_path.unlink(missing_ok=True)
            for item in restricted_items:
                if item.path not in transferred_paths:
                    item.path.unlink(missing_ok=True)
            for parent in {item.path.parent for item in restricted_items}:
                if parent.exists() and not any(
                    item.path in transferred_paths and item.path.parent == parent
                    for item in restricted_items
                ):
                    shutil.rmtree(parent, ignore_errors=True)
        now_playing = position == 0
        await status.edit_text(
            ("▶️" if now_playing else "📝")
            + f" <b>{'Now playing' if now_playing else 'Added to queue'}</b>\n\n"
            f"<b>{html.escape(track.title)}</b>\n"
            f"{'Playing now' if now_playing else f'Queue position: {position}'}\n\n"
            f"Mode: <b>{mode}</b>\n"
            f"Quality: <b>{html.escape(stream_quality_label(track.stream_quality))}</b>\n"
            "Controls: <code>/vpause</code> · <code>/vresume</code> · "
            "<code>/vskip</code> · <code>/vstop</code> (leave video chat)",
            reply_markup=self._vc_keyboard(),
        )

    async def _show_voice_quality_menu(self, message: Message, *, user_id: int) -> None:
        token = uuid4().hex[:12]
        self.pending_voice_choices[token] = PendingVoiceChoice(
            message=message,
            user_id=user_id,
            chat_id=message.chat.id,
        )
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔝 Original / maximum",
                        callback_data=f"vqy:q:original:{token}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        STREAM_VIDEO_QUALITIES[1][1],
                        callback_data=f"vqy:q:{STREAM_VIDEO_QUALITIES[1][0]}:{token}",
                    ),
                    InlineKeyboardButton(
                        STREAM_VIDEO_QUALITIES[2][1],
                        callback_data=f"vqy:q:{STREAM_VIDEO_QUALITIES[2][0]}:{token}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        STREAM_VIDEO_QUALITIES[3][1],
                        callback_data=f"vqy:q:{STREAM_VIDEO_QUALITIES[3][0]}:{token}",
                    ),
                    InlineKeyboardButton(
                        STREAM_VIDEO_QUALITIES[4][1],
                        callback_data=f"vqy:q:{STREAM_VIDEO_QUALITIES[4][0]}:{token}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        STREAM_VIDEO_QUALITIES[5][1],
                        callback_data=f"vqy:q:{STREAM_VIDEO_QUALITIES[5][0]}:{token}",
                    ),
                    InlineKeyboardButton("✖️ Cancel", callback_data=f"vqy:x:{token}"),
                ],
            ]
        )
        prompt = await message.reply_text(
            "🎚 <b>Choose voice-chat video quality</b>\n"
            f"{DIVIDER}\n"
            "🔝 <b>Original / maximum</b> requests the highest resolution "
            "published by the source.\n\n"
            "A numbered option caps the maximum resolution to improve stability. "
            "If the source does not provide that exact quality, the closest lower "
            "quality is selected automatically.\n\n"
            "Direct files and restricted Telegram media always play at their "
            "original uploaded resolution; Telegram cannot add detail that is not "
            "in the source file.",
            reply_markup=keyboard,
        )
        asyncio.create_task(self._expire_voice_quality_choice(token, prompt))

    async def _expire_voice_quality_choice(self, token: str, prompt: Message) -> None:
        try:
            await asyncio.sleep(120)
            self.pending_voice_choices.pop(token, None)
            await prompt.delete()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("Could not expire voice quality menu %s", token, exc_info=True)

    async def _voice_quality_choice(
        self,
        callback: CallbackQuery,
        quality: str,
        token: str,
    ) -> None:
        pending = self.pending_voice_choices.get(token)
        if not pending:
            await callback.answer(
                "This quality menu has expired. Send /vplay again.",
                show_alert=True,
            )
            return
        if callback.from_user.id != pending.user_id:
            await callback.answer("This quality menu belongs to another user.", show_alert=True)
            return
        quality = normalize_stream_quality(quality)
        self.pending_voice_choices.pop(token, None)
        await callback.answer("Resolving the selected stream quality…")
        if callback.message:
            try:
                await callback.message.delete()
            except RPCError:
                pass
        await self.vc_play(
            None,  # type: ignore[arg-type]
            pending.message,
            stream_quality=quality,
        )

    async def _voice_quality_cancel(
        self,
        callback: CallbackQuery,
        token: str,
    ) -> None:
        pending = self.pending_voice_choices.pop(token, None)
        if pending and callback.from_user.id != pending.user_id:
            self.pending_voice_choices[token] = pending
            await callback.answer("This quality menu belongs to another user.", show_alert=True)
            return
        await callback.answer("Voice playback cancelled.")
        if callback.message:
            try:
                await callback.message.delete()
            except RPCError:
                pass

    async def vc_setup(self, _: Client, message: Message) -> None:
        if not await self._vc_admin_allowed(message):
            return
        if not self.voice_chat.ready:
            await message.reply_text(
                "🔐 <b>Voice assistant login is required</b>\n\n"
                "Configure the protected <code>VC_SESSION_STRING</code> value "
                "with a real Telegram user session for voice playback only, "
                "restart the Telegram Bot workflow, then run "
                "<code>/vcsetup</code> again. Restricted Telegram retrieval "
                "uses the separate <code>/rauthorize</code> session."
            )
            return
        target_chat_id = self._vc_target_chat_id(message, allow_argument=True)
        if not await self._vc_target_admin_allowed(message, target_chat_id):
            return
        status = await message.reply_text(
            "⚙️ <b>Setting up voice chat access…</b>\n\n"
            f"Target chat: <code>{target_chat_id}</code>"
        )
        try:
            result = await self._auto_setup_vc_chat(target_chat_id)
        except Exception as exc:
            logger.exception("Automatic VC setup failed for %s", target_chat_id)
            await status.edit_text(
                "❌ <b>Automatic setup could not finish</b>\n\n"
                f"{html.escape(self.voice_chat.user_error(exc))}\n\n"
                "The bot must be an administrator with permission to invite and "
                "promote members."
            )
            return
        lines = [
            "✅ <b>Voice chat setup check complete</b>",
            DIVIDER,
            f"• Target: <code>{target_chat_id}</code>",
            f"• Chat: <b>{html.escape(result['chat_title'])}</b>",
            f"• Bot status: <b>{html.escape(result['bot_status'])}</b>",
            f"• Assistant: <code>{result['assistant_id']}</code>",
            f"• Assistant status: <b>{html.escape(result['assistant_status'])}</b>",
        ]
        if result["added"]:
            lines.append("• Assistant membership: <b>added automatically</b>")
        if result["promoted"]:
            lines.append("• Voice permission: <b>granted automatically</b>")
        if result["warnings"]:
            lines.extend(["", "<b>Action still needed</b>"])
            lines.extend(f"• {html.escape(item)}" for item in result["warnings"])
        else:
            lines.extend(
                [
                    "",
                    "✅ Assistant access is ready.",
                    "Use <code>/vplay audio URL</code> or "
                    "<code>/vplay video URL</code> to start playback automatically.",
                ]
            )
        await status.edit_text("\n".join(lines))

    async def vc_status(self, _: Client, message: Message) -> None:
        if not await self._vc_admin_allowed(message):
            return
        target_chat_id = self._vc_target_chat_id(message)
        if not self.voice_chat.ready:
            await message.reply_text(
                "⚙️ <b>Voice assistant is not connected.</b>\n\n"
                "Configure the protected <code>VC_SESSION_STRING</code> with "
                "a real Telegram user session for voice playback only, restart "
                "the Telegram Bot workflow, then run <code>/vcsetup</code>. "
                "Restricted retrieval uses the separate "
                "<code>/rauthorize</code> session."
            )
            return
        try:
            diagnostics = await self.voice_chat.chat_diagnostics(target_chat_id)
        except Exception as exc:
            await message.reply_text(
                "❌ <b>Voice chat diagnostics failed</b>\n\n"
                f"{html.escape(self.voice_chat.user_error(exc))}\n\n"
                "Add the assistant user to this group/channel, grant voice-chat "
                "permissions, then retry /vplay."
            )
            return
        state = self.voice_chat.status(target_chat_id)
        queue_count = len(state.queue) if state else 0
        await message.reply_text(
            "🔎 <b>Voice chat status</b>\n"
            f"{DIVIDER}\n"
            f"• Chat: <b>{html.escape(str(diagnostics['chat_title']))}</b>\n"
            f"• Assistant: <code>{diagnostics['assistant_id']}</code> "
            f"({html.escape(str(diagnostics['assistant_name']))})\n"
            f"• Assistant account: <b>{'user' if not diagnostics.get('assistant_is_bot') else 'bot'}</b>\n"
            f"• Membership: <b>{html.escape(str(diagnostics['member_status']))}</b>\n"
            f"• Playback connected: <b>{'yes' if diagnostics['connected'] else 'no'}</b>\n"
            f"• Queued tracks: <b>{queue_count}</b>\n\n"
            "The assistant must be a member of this exact chat. Start a voice "
                "chat, then run /vplay; the assistant will start the call automatically."
        )

    async def restricted_authorize_command(
        self, _: Client, message: Message
    ) -> None:
        """Start or cancel the admin-only private restricted-session login."""
        if not await self._require_configured_admin(message):
            return
        if getattr(message.chat.type, "name", "") != "PRIVATE":
            await message.reply_text(
                "🔐 For security, run <code>/rauthorize</code> in a private chat "
                "with this bot."
            )
            return
        user_id = message.from_user.id if message.from_user else 0
        args = getattr(message, "command", None) or []
        action = args[1].lower() if len(args) > 1 else ""
        if action in {"cancel", "stop"}:
            async with self._restricted_auth_lock:
                self.pending_restricted_auth.pop(user_id, None)
            async with self._restricted_auth_io_lock:
                await self.restricted_session.cancel_login()
            await message.reply_text(
                "Authorization cancelled. No login data was retained."
            )
            return
        if action in {"reset", "logout", "remove"}:
            async with self._restricted_auth_lock:
                self.pending_restricted_auth.pop(user_id, None)
            async with self._restricted_auth_io_lock:
                await self.restricted_session.reset()
            await message.reply_text(
                "✅ <b>Restricted-content authorization removed</b>\n\n"
                "The dedicated local user session was deleted. "
                "The voice assistant session was not changed."
            )
            return
        if self.restricted_session.authorized:
            identity = await self.restricted_session.identity()
            name = (
                getattr(identity, "username", None)
                or getattr(identity, "first_name", None)
                or "authorized user"
            )
            await message.reply_text(
                "✅ <b>Restricted-content user is already authorized</b>\n\n"
                f"Account: <b>{html.escape(str(name)[:120])}</b>\n"
                "Use <code>/savecheck Telegram message link</code> to verify "
                "access to a private chat.\n\n"
                "To replace this account, use <code>/rauthorize reset</code>, "
                "then run <code>/rauthorize</code> again."
            )
            return
        async with self._restricted_auth_lock:
            if user_id in self.pending_restricted_auth:
                already_pending = True
            else:
                already_pending = False
                self.pending_restricted_auth[user_id] = PendingRestrictedAuthorization(
                    chat_id=message.chat.id,
                    phase="phone",
                    expires_at=time.time() + 300,
                )
        if already_pending:
            await message.reply_text(
                "⏳ <b>Authorization is already in progress</b>\n\n"
                "Continue with the current login step, or send "
                "<code>/rauthorize cancel</code> before starting over."
            )
            return
        await message.reply_text(
            "🔐 <b>Authorize restricted-content user</b>\n\n"
            "This setup is for the bot administrator only and must be done "
            "in this private chat.\n"
            "Send the Telegram phone number for the user account that can access "
            "the protected channel, including the country code "
            "(example: <code>+15551234567</code>).\n\n"
            "Your phone number, login code, and 2FA password will be deleted "
            "immediately after each step and will never be logged.\n\n"
            "Send <code>/rauthorize cancel</code> to stop."
        )

    async def restricted_authorization_input(
        self, _: Client, message: Message
    ) -> None:
        """Consume one private admin login input and delete it immediately."""
        if (
            not message.from_user
            or getattr(message.chat.type, "name", "") != "PRIVATE"
            or self.settings.admin_id is None
            or message.from_user.id != self.settings.admin_id
        ):
            return
        user_id = message.from_user.id
        async with self._restricted_auth_lock:
            pending = self.pending_restricted_auth.get(user_id)
            if not pending or message.chat.id != pending.chat_id:
                return
            if pending.processing:
                return
            # Serialize only state transitions. Telegram RPCs and message
            # operations must stay outside this lock so one slow login cannot
            # block authorization input from every other user.
            pending.processing = True

        app = self.app

        async def remove_pending() -> bool:
            async with self._restricted_auth_lock:
                if self.pending_restricted_auth.get(user_id) is not pending:
                    return False
                self.pending_restricted_auth.pop(user_id, None)
                return True

        try:
            try:
                await message.delete()
            except RPCError:
                logger.warning("Could not delete restricted authorization input")
            if pending.expires_at <= time.time():
                removed = await remove_pending()
                if removed:
                    async with self._restricted_auth_io_lock:
                        await self.restricted_session.cancel_login()
                    if app:
                        await app.send_message(
                            pending.chat_id,
                            "⌛ <b>Authorization expired</b>\n\n"
                            "Run <code>/rauthorize</code> again.",
                        )
                raise StopPropagation
            value = (message.text or "").strip()
            if not value:
                raise StopPropagation
            try:
                if pending.phase == "phone":
                    if not re.fullmatch(r"\+[1-9]\d{6,14}", value):
                        raise RestrictedContentError(
                            "Use an international phone number such as "
                            "<code>+15551234567</code>. Run /rauthorize again."
                        )
                    async with self._restricted_auth_io_lock:
                        existing = await self.restricted_session.begin_login(value)
                    if existing:
                        await remove_pending()
                        if app:
                            await app.send_message(
                                pending.chat_id,
                                "✅ <b>Restricted-content user authorized</b>\n\n"
                                "Use <code>/savecheck Telegram message link</code> "
                                "to confirm access before downloading.",
                            )
                        raise StopPropagation
                    async with self._restricted_auth_lock:
                        if self.pending_restricted_auth.get(user_id) is pending:
                            pending.phase = "code"
                    if app:
                        await app.send_message(
                            pending.chat_id,
                            "📩 Telegram sent a login code to that account.\n\n"
                            "Send the code here. Telegram may display it with spaces; "
                            "spaces are accepted. The message will be deleted immediately.",
                        )
                    raise StopPropagation
                if pending.phase == "code":
                    code = re.sub(r"\s+", "", value)
                    if not re.fullmatch(r"\d{4,8}", code):
                        raise RestrictedContentError(
                            "That does not look like a Telegram login code. "
                            "Run /rauthorize again."
                        )
                    async with self._restricted_auth_io_lock:
                        needs_password = await self.restricted_session.finish_code(code)
                    if needs_password:
                        async with self._restricted_auth_lock:
                            if self.pending_restricted_auth.get(user_id) is pending:
                                pending.phase = "password"
                        if app:
                            await app.send_message(
                                pending.chat_id,
                                "🔑 <b>2-step verification is enabled</b>\n\n"
                                "Send the Telegram 2FA password. It will be deleted "
                                "immediately and never logged.",
                            )
                        raise StopPropagation
                elif pending.phase == "password":
                    async with self._restricted_auth_io_lock:
                        await self.restricted_session.finish_password(value)
                else:
                    raise RestrictedContentError(
                        "Authorization state expired. Run /rauthorize again."
                    )
            except StopPropagation:
                # This is intentional handler control flow, not an auth failure.
                raise
            except RestrictedContentError as exc:
                removed = await remove_pending()
                if removed:
                    async with self._restricted_auth_io_lock:
                        await self.restricted_session.cancel_login()
                    if app:
                        await app.send_message(
                            pending.chat_id,
                            f"❌ <b>{html.escape(str(exc))}</b>",
                        )
                raise StopPropagation
            except Exception as exc:
                logger.error(
                    "Restricted user authorization failed: %s",
                    type(exc).__name__,
                )
                removed = await remove_pending()
                if removed:
                    async with self._restricted_auth_io_lock:
                        await self.restricted_session.cancel_login()
                    if app:
                        await app.send_message(
                            pending.chat_id,
                            "❌ <b>Telegram authorization could not be completed</b>\n\n"
                            "The login step was safely reset. Run "
                            "<code>/rauthorize</code> again. If it repeats, use "
                            "<code>/rauthorize reset</code> first.",
                        )
                raise StopPropagation
            await remove_pending()
            if app:
                await app.send_message(
                    pending.chat_id,
                    "✅ <b>Restricted-content user authorized</b>\n\n"
                    "Use <code>/savecheck Telegram message link</code> to confirm "
                    "that this account can access the protected chat.",
                )
            raise StopPropagation
        finally:
            async with self._restricted_auth_lock:
                if self.pending_restricted_auth.get(user_id) is pending:
                    pending.processing = False

    async def vc_queue(self, _: Client, message: Message) -> None:
        if not await self._vc_admin_allowed(message):
            return
        target_chat_id = self._vc_target_chat_id(message)
        try:
            self.voice_chat._require_enabled()
        except RuntimeError as exc:
            await message.reply_text(f"⚙️ <b>Voice chat setup needed</b>\n\n{exc}")
            return
        await message.reply_text(
            self._vc_panel_text(target_chat_id),
            reply_markup=self._vc_keyboard(),
        )

    async def vc_panel(self, _: Client, message: Message) -> None:
        await self.vc_queue(_, message)

    async def vc_pause(self, _: Client, message: Message) -> None:
        await self._vc_action(message, "pause")

    async def vc_resume(self, _: Client, message: Message) -> None:
        await self._vc_action(message, "resume")

    async def vc_skip(self, _: Client, message: Message) -> None:
        await self._vc_action(message, "skip")

    async def vc_stop(self, _: Client, message: Message) -> None:
        await self._vc_action(message, "stop")

    async def vc_seek(self, _: Client, message: Message) -> None:
        if not await self._vc_admin_allowed(message):
            return
        target_chat_id = self._vc_target_chat_id(message)
        parts = (message.text or message.caption or "").split(maxsplit=1)
        if len(parts) != 2:
            await message.reply_text("Usage: <code>/vseek seconds</code>")
            return
        try:
            seconds = int(parts[1])
            changed = await self.voice_chat.seek(target_chat_id, seconds)
        except (TypeError, ValueError) as exc:
            await message.reply_text(f"❌ <b>{html.escape(str(exc))}</b>")
            return
        except RuntimeError as exc:
            await message.reply_text(f"⚙️ <b>Voice chat setup needed</b>\n\n{exc}")
            return
        await message.reply_text(
            f"⏩ <b>{'Seeked to ' + self._format_seconds(seconds) if changed else 'Nothing is playing.'}</b>"
        )

    async def vc_volume(self, _: Client, message: Message) -> None:
        if not await self._vc_admin_allowed(message):
            return
        target_chat_id = self._vc_target_chat_id(message)
        parts = (message.text or message.caption or "").split(maxsplit=1)
        if len(parts) != 2:
            await message.reply_text("Usage: <code>/vvolume 1-200</code>")
            return
        try:
            volume = int(parts[1])
            changed = await self.voice_chat.set_volume(target_chat_id, volume)
        except (TypeError, ValueError) as exc:
            await message.reply_text(f"❌ <b>{html.escape(str(exc))}</b>")
            return
        except RuntimeError as exc:
            await message.reply_text(f"⚙️ <b>Voice chat setup needed</b>\n\n{exc}")
            return
        await message.reply_text(
            f"🔊 <b>{f'Volume set to {volume}%' if changed else 'Nothing is playing.'}</b>"
        )

    async def vc_loop(self, _: Client, message: Message) -> None:
        if not await self._vc_admin_allowed(message):
            return
        target_chat_id = self._vc_target_chat_id(message)
        try:
            enabled = await self.voice_chat.set_loop(target_chat_id)
        except RuntimeError as exc:
            await message.reply_text(f"⚙️ <b>Voice chat setup needed</b>\n\n{exc}")
            return
        await message.reply_text(
            f"🔁 <b>{'Loop enabled.' if enabled else 'Loop disabled.'}</b>"
        )

    async def vc_clear(self, _: Client, message: Message) -> None:
        if not await self._vc_admin_allowed(message):
            return
        target_chat_id = self._vc_target_chat_id(message)
        try:
            count = await self.voice_chat.clear_queue(target_chat_id)
        except RuntimeError as exc:
            await message.reply_text(f"⚙️ <b>Voice chat setup needed</b>\n\n{exc}")
            return
        await message.reply_text(
            f"🧹 <b>{count} upcoming track(s) cleared.</b>"
            if count
            else "📭 <b>There are no upcoming tracks.</b>"
        )

    @staticmethod
    def _format_seconds(seconds: int) -> str:
        minutes, remainder = divmod(max(0, seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}:{minutes:02d}:{remainder:02d}"
        return f"{minutes}:{remainder:02d}"

    async def _vc_action(self, message: Message, action: str) -> None:
        if not await self._vc_admin_allowed(message):
            return
        target_chat_id = self._vc_target_chat_id(message)
        try:
            changed = await getattr(self.voice_chat, action)(target_chat_id)
        except RuntimeError as exc:
            await message.reply_text(f"⚙️ <b>Voice chat setup needed</b>\n\n{exc}")
            return
        labels = {
            "pause": ("⏸", "Playback paused."),
            "resume": ("▶️", "Playback resumed."),
            "skip": ("⏭", "Track skipped."),
            "stop": ("⏹", "Playback stopped and queue cleared."),
        }
        icon, text = labels[action]
        await message.reply_text(
            f"{icon} <b>{text if changed else 'Nothing is playing.'}</b>"
        )

    async def _vc_callback(self, callback: CallbackQuery) -> None:
        if not callback.message:
            await callback.answer("This control is no longer available.", show_alert=True)
            return
        if not await self._vc_admin_allowed(callback.message):
            await callback.answer("Admin permission required.", show_alert=True)
            return
        action = (callback.data or "").split(":", 1)[1]
        chat_id = self._vc_target_chat_id(callback.message)
        if action == "refresh":
            await callback.answer("Updated.")
            await callback.message.edit_text(
                self._vc_panel_text(chat_id),
                reply_markup=self._vc_keyboard(),
            )
            return
        try:
            if action in {"pause", "resume", "skip", "stop"}:
                changed = await getattr(self.voice_chat, action)(chat_id)
                result = (
                    {
                        "pause": "Playback paused.",
                        "resume": "Playback resumed.",
                        "skip": "Track skipped.",
                        "stop": "Playback stopped and queue cleared.",
                    }[action]
                    if changed
                    else "Nothing is playing."
                )
            elif action == "loop":
                enabled = await self.voice_chat.set_loop(chat_id)
                result = f"Loop {'enabled' if enabled else 'disabled'}."
            elif action == "clear":
                count = await self.voice_chat.clear_queue(chat_id)
                result = (
                    f"{count} upcoming track(s) cleared."
                    if count
                    else "There are no upcoming tracks."
                )
            else:
                await callback.answer("Unknown control.", show_alert=True)
                return
        except RuntimeError as exc:
            await callback.answer(self.voice_chat.user_error(exc), show_alert=True)
            return
        await callback.answer(result)
        await callback.message.edit_text(
            self._vc_panel_text(chat_id),
            reply_markup=self._vc_keyboard(),
        )

    @staticmethod
    def _vc_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⏸ Pause", callback_data="vc:pause"),
                    InlineKeyboardButton("▶️ Resume", callback_data="vc:resume"),
                ],
                [
                    InlineKeyboardButton("⏭ Skip", callback_data="vc:skip"),
                    InlineKeyboardButton("🔁 Loop", callback_data="vc:loop"),
                ],
                [
                    InlineKeyboardButton("🧹 Clear queue", callback_data="vc:clear"),
                    InlineKeyboardButton("🔄 Refresh", callback_data="vc:refresh"),
                ],
                [InlineKeyboardButton("⏹ Cancel & leave video chat", callback_data="vc:stop")],
            ]
        )

    def _vc_panel_text(self, chat_id: int) -> str:
        state = self.voice_chat.status(chat_id)
        if not state or (not state.current and not state.queue):
            return (
                "📭 <b>Voice chat queue is empty</b>\n\n"
                "Use <code>/vplay YouTube URL or search text</code> "
                "or reply to Telegram media with <code>/vplay</code>."
            )
        lines = ["🎙 <b>Voice chat control panel</b>", DIVIDER]
        if state.current:
            position = self.voice_chat.current_position(chat_id)
            duration = state.current.duration
            timing = ""
            if position is not None:
                timing = f" · {self._format_seconds(position)}"
                if duration:
                    timing += f"/{self._format_seconds(duration)}"
            lines.append(
                f"▶️ <b>Now:</b> {html.escape(state.current.title)}"
                f"{html.escape(timing)}"
            )
            lines.append(
                f"🔊 Volume: <b>{state.volume}%</b> · "
                f"🔁 Loop: <b>{'on' if state.loop else 'off'}</b> · "
                f"⏯ <b>{'paused' if state.paused else 'playing'}</b>"
            )
        if state.queue:
            lines.append("")
            lines.append(f"📝 <b>Up next ({len(state.queue)}):</b>")
            for index, track in enumerate(state.queue[:10], 1):
                lines.append(f"{index}. {html.escape(track.title)}")
            if len(state.queue) > 10:
                lines.append(f"… and {len(state.queue) - 10} more")
        return "\n".join(lines)

    async def _vc_admin_allowed(self, message: Message) -> bool:
        if not message.from_user:
            await message.reply_text("Only a Telegram user can control playback.")
            return False
        if self._configured_admin(message.from_user.id, message.chat.id):
            return True
        if getattr(message.chat.type, "name", "") == "PRIVATE":
            if self.vc_chat_id is None:
                await message.reply_text(
                    "⚙️ <b>Voice chat target is not configured</b>\n\n"
                    "Set <code>VC_CHAT_ID</code> before controlling a group "
                    "voice chat from private messages."
                )
                return False
            return await self._vc_target_admin_allowed(message, self.vc_chat_id)
        if not self.app:
            return False
        try:
            member = await self.app.get_chat_member(
                message.chat.id,
                message.from_user.id,
            )
            status = getattr(member.status, "name", str(member.status)).lower()
            if status in {"administrator", "owner"}:
                return True
        except RPCError:
            pass
        await message.reply_text(
            "🔐 <b>Admin permission required</b>\n\n"
            "Only group administrators can control voice-chat playback."
        )
        return False

    async def _vc_target_admin_allowed(
        self,
        message: Message,
        target_chat_id: int,
    ) -> bool:
        """Require setup callers to administer the chat they are configuring."""
        if not message.from_user or not self.app:
            return False
        if self._configured_admin(message.from_user.id, target_chat_id):
            return True
        if (
            getattr(message.chat.type, "name", "") != "PRIVATE"
            and target_chat_id == message.chat.id
        ):
            return True
        try:
            member = await self.app.get_chat_member(
                target_chat_id,
                message.from_user.id,
            )
            status = getattr(member.status, "name", str(member.status)).lower()
            if status in {"administrator", "owner"}:
                return True
        except RPCError:
            pass
        await message.reply_text(
            "🔐 <b>Target-chat admin permission required</b>\n\n"
            "You must be an administrator in the chat you are configuring."
        )
        return False

    def _vc_target_chat_id(
        self,
        message: Message,
        allow_argument: bool = False,
    ) -> int:
        parts = (message.text or message.caption or "").split()
        if allow_argument and len(parts) > 1:
            try:
                return int(parts[1])
            except ValueError:
                pass
        if getattr(message.chat.type, "name", "") == "PRIVATE":
            if self.vc_chat_id is not None:
                return self.vc_chat_id
        return message.chat.id

    async def _auto_setup_vc_chat(self, chat_id: int) -> dict[str, object]:
        if not self.app or not self.voice_chat.assistant:
            raise RuntimeError(self.voice_chat.setup_hint)
        bot = await self.app.get_me()
        assistant = await self.voice_chat.assistant.get_me()
        chat = await self.app.get_chat(chat_id)
        bot_member = await self.app.get_chat_member(chat_id, bot.id)
        bot_status = self._member_status(bot_member)
        warnings: list[str] = []
        added = False
        promoted = False

        if bot_status not in {"administrator", "owner"}:
            warnings.append(
                "Make the bot an administrator first; it cannot add or promote "
                "the assistant as a normal member."
            )
        else:
            try:
                assistant_member = await self.voice_chat.assistant.get_chat_member(
                    chat_id, assistant.id
                )
            except RPCError:
                assistant_member = None
            if assistant_member is None or self._member_status(assistant_member) in {
                "left",
                "kicked",
            }:
                try:
                    failures = await self.app.add_chat_members(chat_id, assistant.id)
                    added = not failures
                except RPCError as exc:
                    warnings.append(
                        "Telegram would not add the assistant automatically: "
                        + self.voice_chat.user_error(exc)
                    )
            try:
                assistant_member = await self.voice_chat.assistant.get_chat_member(
                    chat_id, assistant.id
                )
                assistant_status = self._member_status(assistant_member)
            except RPCError:
                assistant_status = "not a member"
            if assistant_status not in {"administrator", "owner"}:
                try:
                    await self.app.promote_chat_member(
                        chat_id,
                        assistant.id,
                        privileges=ChatAdministratorRights(
                            can_manage_video_chats=True,
                            can_invite_users=True,
                        ),
                    )
                    promoted = True
                    assistant_status = "administrator"
                except RPCError as exc:
                    warnings.append(
                        "Assistant could not be promoted automatically: "
                        + self.voice_chat.user_error(exc)
                    )

        try:
            assistant_member = await self.voice_chat.assistant.get_chat_member(
                chat_id, assistant.id
            )
            assistant_status = self._member_status(assistant_member)
        except RPCError:
            assistant_status = "not a member"
        if assistant_status in {"left", "kicked", "not a member"}:
            warnings.append(
                "Add the assistant user to this exact chat; Telegram did not expose "
                "it as a member."
            )
        if assistant_status not in {"administrator", "owner"}:
            warnings.append(
                "Assistant needs administrator permission to manage voice chats."
            )
        return {
            "chat_title": str(getattr(chat, "title", None) or chat_id),
            "bot_status": bot_status,
            "assistant_id": assistant.id,
            "assistant_status": assistant_status,
            "added": added,
            "promoted": promoted,
            "warnings": list(dict.fromkeys(warnings)),
        }

    @staticmethod
    def _member_status(member: object) -> str:
        return getattr(
            getattr(member, "status", None),
            "name",
            str(getattr(member, "status", "unknown")),
        ).lower()

    async def start(self, _: Client, message: Message) -> None:
        command = getattr(message, "command", None) or []
        payload = command[1] if len(command) > 1 else ""
        if payload.startswith("store_"):
            await self._deliver_stored_file(message, payload[6:])
            return
        await message.reply_text(
            self._welcome_text(),
            reply_markup=self._home_keyboard(),
        )

    async def help(self, _: Client, message: Message) -> None:
        await message.reply_text(
            self._help_text(),
            reply_markup=self._section_nav(include_help=False),
        )

    async def cookies_help(self, _: Client, message: Message) -> None:
        await message.reply_text(
            self._cookie_help_text(),
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔐 Check cookie status", callback_data="yt:cookie_status:menu")],
                    [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
                ]
            ),
        )

    async def delete_cookies(self, _: Client, message: Message) -> None:
        if not message.from_user:
            await message.reply_text(
                "❌ <b>Cannot identify user</b>\n\n"
                "This command must be sent from a personal account, not a channel."
            )
            return
        deleted = self.cookies.delete(message.from_user.id)
        await message.reply_text(
            (
                "✅ <b>Cookies removed</b>\n\n"
                "Your stored browser session was securely deleted."
                if deleted
                else "ℹ️ <b>Nothing to remove</b>\n\nYou do not have stored cookies."
            ),
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")]]
            ),
        )

    def _configured_admin(self, user_id: int, chat_id: int) -> bool:
        del chat_id
        return user_id == getattr(self.settings, "admin_id", None)

    async def enforce_user_ban(self, _: Client, message: Message) -> None:
        user = message.from_user
        if not user or self._configured_admin(user.id, message.chat.id):
            return
        if self.file_store and await self.file_store.is_user_banned(user.id):
            await message.reply_text(
                "🚫 <b>Access blocked</b>\n\n"
                "Your account is not allowed to use this bot."
            )
            raise StopPropagation

    async def enforce_callback_ban(
        self, _: Client, callback: CallbackQuery
    ) -> None:
        user = callback.from_user
        chat_id = callback.message.chat.id if callback.message else 0
        if not user or self._configured_admin(user.id, chat_id):
            return
        if self.file_store and await self.file_store.is_user_banned(user.id):
            await callback.answer("Your access to this bot is blocked.", show_alert=True)
            raise StopPropagation

    async def _require_configured_admin(self, message: Message) -> bool:
        user_id = message.from_user.id if message.from_user else 0
        if self._configured_admin(user_id, message.chat.id):
            return True
        await message.reply_text(
            "🔐 <b>Bot administrator permission required</b>\n\n"
            "This command is limited to the configured bot administrators."
        )
        return False

    async def admin_command(self, _: Client, message: Message) -> None:
        if not await self._require_configured_admin(message):
            return
        queue_count = len(self.queue._known) if self.queue else 0
        active_count = len(self.queue._active) if self.queue else 0
        cache_name = type(self.cache).__name__ if self.cache else "starting"
        await message.reply_text(
            "🛡 <b>Bot administrator console</b>\n"
            f"{DIVIDER}\n"
            f"• Queued jobs: <b>{queue_count}</b>\n"
            f"• Active jobs: <b>{active_count}</b>\n"
            f"• Cache: <code>{html.escape(cache_name)}</code>\n"
            f"• Voice assistant: <b>{'ready' if self.voice_chat.ready else 'offline'}</b>\n\n"
            "Use <code>/admin_cancel_all</code> to request cancellation of every "
            "active and queued download."
        )

    async def admin_cancel_all(self, _: Client, message: Message) -> None:
        if not await self._require_configured_admin(message):
            return
        if not self.queue:
            await message.reply_text("The download queue is still starting.")
            return
        count = sum(
            self.queue.cancel(job.user_id)
            for job in self.queue._known.values()
        )
        await message.reply_text(
            f"🛑 <b>Cancellation requested for {count} download(s).</b>\n\n"
            "Active transfers stop at their next safe checkpoint."
        )

    @staticmethod
    def _moderation_target(message: Message) -> int | None:
        replied = message.reply_to_message
        if replied and replied.from_user:
            return replied.from_user.id
        parts = (message.text or message.caption or "").split()
        if len(parts) < 2:
            return None
        try:
            user_id = int(parts[1])
        except ValueError:
            return None
        return user_id if user_id > 0 else None

    async def ban_command(self, _: Client, message: Message) -> None:
        if not await self._require_configured_admin(message):
            return
        target = self._moderation_target(message)
        admin_id = message.from_user.id if message.from_user else 0
        if not target:
            await message.reply_text(
                "Usage: <code>/ban USER_ID</code>\n"
                "You can also reply to a user's message with <code>/ban</code>."
            )
            return
        if target == admin_id:
            await message.reply_text("You cannot ban the configured administrator.")
            return
        if not self.file_store:
            await message.reply_text("Moderation storage is still starting.")
            return
        changed = await self.file_store.ban_user(target, admin_id)
        await message.reply_text(
            f"🚫 User <code>{target}</code> is now banned from this bot."
            if changed
            else f"User <code>{target}</code> was already banned."
        )

    async def unban_command(self, _: Client, message: Message) -> None:
        if not await self._require_configured_admin(message):
            return
        target = self._moderation_target(message)
        if not target:
            await message.reply_text(
                "Usage: <code>/unban USER_ID</code>\n"
                "You can also reply to a user's message with <code>/unban</code>."
            )
            return
        if not self.file_store:
            await message.reply_text("Moderation storage is still starting.")
            return
        changed = await self.file_store.unban_user(target)
        await message.reply_text(
            f"✅ User <code>{target}</code> can use this bot again."
            if changed
            else f"User <code>{target}</code> was not banned."
        )

    async def cookie_status(self, _: Client, message: Message) -> None:
        health = self.cookies.health(message.from_user.id)
        if not health["present"]:
            await message.reply_text(
                "🍪 <b>No cookies connected</b>\n\n"
                "Use /cookies, then upload a JSON or Netscape browser export.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🍪 Cookie help", callback_data="yt:cookies:menu")]]
                ),
            )
            return
        if not health.get("valid"):
            await message.reply_text(
                "⚠️ <b>Cookie file needs attention</b>\n\n"
                "The stored file is not readable as a valid browser export. "
                "Please upload a fresh JSON or Netscape cookie file.",
            )
            return
        domains = ", ".join(health["youtube_domains"]) or "none"
        status = "healthy" if not health["expired"] else "partly expired"
        await message.reply_text(
            f"🍪 <b>Cookie session: {status}</b>\n"
            f"{DIVIDER}\n"
            f"• Records: <code>{health['records']}</code>\n"
            f"• YouTube domains: <code>{domains}</code>\n"
            f"• Expired records: <code>{health['expired']}</code>\n\n"
            "🔒 Cookie values are encrypted and never shown.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🗑 Delete cookies", callback_data="yt:deletecookies:menu")],
                    [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
                ]
            ),
        )

    async def _edit_cookie_status(self, message: Message, user_id: int) -> None:
        health = self.cookies.health(user_id)
        if not health["present"]:
            await message.edit_text(
                "🍪 <b>No cookies connected</b>\n\n"
                "Use /cookies, then upload a JSON or Netscape browser export.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("🍪 Cookie help", callback_data="yt:cookies:menu")],
                        [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
                    ]
                ),
            )
            return
        if not health.get("valid"):
            await message.edit_text(
                "⚠️ <b>Cookie file needs attention</b>\n\n"
                "The stored file is not readable as a valid browser export. "
                "Please upload a fresh JSON or Netscape cookie file.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("🍪 Cookie help", callback_data="yt:cookies:menu")],
                        [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
                    ]
                ),
            )
            return
        domains = ", ".join(health["youtube_domains"]) or "none"
        status = "healthy" if not health["expired"] else "partly expired"
        await message.edit_text(
            f"🍪 <b>Cookie session: {status}</b>\n"
            f"{DIVIDER}\n"
            f"• Records: <code>{health['records']}</code>\n"
            f"• YouTube domains: <code>{domains}</code>\n"
            f"• Expired records: <code>{health['expired']}</code>\n\n"
            "🔒 Cookie values are encrypted and never shown.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🗑 Delete cookies", callback_data="yt:deletecookies:menu")],
                    [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
                ]
            ),
        )

    async def receive_media(self, _: Client, message: Message) -> None:
        document = message.document
        media = self._media_details(message)
        logger.info(
            "Received Telegram media upload: chat=%s media=%s name=%s",
            message.chat.id,
            next(
                (
                    attribute
                    for attribute, _ in (
                        ("document", "document.bin"),
                        ("video", "video.mp4"),
                        ("audio", "audio.mp3"),
                        ("photo", "photo.jpg"),
                        ("animation", "animation.mp4"),
                        ("voice", "voice.ogg"),
                        ("video_note", "video-note.mp4"),
                    )
                    if getattr(message, attribute, None)
                ),
                "unknown",
            ),
            media[0] if media else "unavailable",
        )
        if await self._receive_shared_file(message):
            return
        if document and looks_like_cookie_document(document.file_name):
            await self.receive_cookies(_, message)
            return
        await self._share_file(message)

    async def receive_cookies(self, _: Client, message: Message) -> None:
        """Keep cookie uploads separate from ordinary file-link uploads."""
        document = message.document
        user_id = message.from_user.id if message.from_user else 0
        if not document or not looks_like_cookie_document(document.file_name):
            return
        if (document.file_size or 0) > 2_000_000:
            await message.reply_text("That cookie file is too large.")
            return
        target = (
            self.settings.work_dir
            / f"incoming-cookie-{user_id}-{uuid4().hex}.txt"
        )
        downloaded_path: Path | None = None
        try:
            downloaded = await message.download(file_name=str(target))
            downloaded_path = Path(downloaded) if downloaded else target
            if not downloaded_path.exists() and target.exists():
                downloaded_path = target
            if not downloaded_path.exists():
                raise RuntimeError("Cookie upload did not produce a local file.")
            self.cookies.save(user_id, downloaded_path.read_bytes())
        except Exception:
            logger.exception("Could not process cookie upload for user %s", user_id)
            await message.reply_text(
                "❌ <b>Could not connect these cookies</b>\n\n"
                "Export a fresh JSON or Netscape cookie file and try again."
            )
        else:
            await message.reply_text(
                "✅ <b>Cookies connected securely</b>\n\n"
                "Your next YouTube download can use this browser session.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")]]
                ),
            )
        finally:
            target.unlink(missing_ok=True)
            if downloaded_path and downloaded_path != target:
                downloaded_path.unlink(missing_ok=True)

    async def file_url_command(self, _: Client, message: Message) -> None:
        source = extract_url(message.text or message.caption or "")
        if source and parse_restricted_source(source):
            await self.enqueue(
                message,
                source,
                False,
                mode="restricted",
                completion_callback=self._complete_file_links,
            )
            return
        replied = message.reply_to_message
        if replied and self._media_details(replied):
            document = replied.document
            if document and looks_like_cookie_document(document.file_name):
                await message.reply_text(
                    "🍪 This looks like a browser cookie file. "
                    "Use /cookies to store it securely instead of sharing it."
                )
                return
            await self._share_file(
                replied,
                owner_id=message.from_user.id if message.from_user else 0,
                response_to=message,
            )
            return
        user_id = message.from_user.id if message.from_user else 0
        self.pending_file_uploads[user_id] = PendingFileUpload(
            user_id=user_id,
            chat_id=message.chat.id,
            expires_at=time.time() + 120,
        )
        await message.reply_text(
            "📎 <b>File links ready</b>\n\n"
            "Send a document, video, audio, photo, animation, voice message, "
            "or video note in the next 2 minutes.\n"
            "I’ll return separate <b>Stream</b> and <b>Download</b> links.\n\n"
            "🔐 Links are random and temporary. Files self-destruct automatically."
        )

    async def mirror_command(self, _: Client, message: Message) -> None:
        """Mirror/leech a URL or replied Telegram media into the current chat.

        YouTube URLs use the existing format/quality flow, direct HTTP(S)
        files use the mirror worker, and magnet links use the torrent worker.
        Replied media uses Telegram's file-id based delivery path and keeps the
        same upload limit, cookie-file protections, and archive behavior.
        """
        replied = message.reply_to_message
        if replied and self._media_details(replied):
            document = replied.document
            if document and looks_like_cookie_document(document.file_name):
                await message.reply_text(
                    "🍪 Cookie files cannot be mirrored. "
                    "Use <code>/cookies</code> to store them securely."
                )
                return
            try:
                await self._send_replied_media(
                    message,
                    replied,
                    owner_id=message.from_user.id if message.from_user else 0,
                )
            except Exception:
                logger.exception("Could not mirror replied Telegram media")
                await message.reply_text(
                    "❌ <b>Could not mirror this Telegram file</b>\n\n"
                    "Check the file size limit and try again."
                )
            return

        source = extract_source(
            getattr(message, "text", None)
            or getattr(message, "caption", None)
            or ""
        )
        if not source:
            await message.reply_text(
                "🔁 <b>Mirror / leech</b>\n\n"
                "Send <code>/mirror file URL</code> or <code>/leech magnet URL</code>, "
                "or reply to an audio/video/document with the command."
            )
            return
        command_name = (getattr(message, "command", None) or ["mirror"])[0].lower()
        if is_torrent_source(source):
            if command_name != "leech":
                await message.reply_text(
                    "Use <code>/leech magnet:?…</code> for torrent magnet links."
                )
                return
            try:
                source, select_files = self._parse_leech_request(
                    message.text or message.caption or ""
                )
            except ValueError as exc:
                await message.reply_text(f"❌ <b>{html.escape(str(exc))}</b>")
                return
            if not source:
                await message.reply_text(
                    "🧲 <b>Smart leech</b>\n\n"
                    "Usage: <code>/leech magnet:?xt=…</code>\n"
                    "Select files from a multi-file torrent with "
                    "<code>--select 1,3-5</code>."
                )
                return
            await self.enqueue(
                message,
                source,
                False,
                mode="torrent",
                torrent_select_files=select_files,
            )
            return
        if command_name == "leech":
            await message.reply_text(
                "🧲 <b>Smart leech</b>\n\n"
                "Leech accepts magnet links and public <code>.torrent</code> URLs.\n"
                "Usage: <code>/leech magnet:?xt=…</code> or "
                "<code>/leech https://host/file.torrent</code>"
            )
            return
        kind = source_kind(source)
        if parse_restricted_source(source):
            if command_name == "leech":
                await message.reply_text(
                    "Use <code>/save</code> or <code>/mirror</code> for Telegram "
                    "message links; <code>/leech</code> is for torrents."
                )
                return
            await self.enqueue(
                message,
                source,
                False,
                mode="restricted",
            )
            return
        if kind == "youtube":
            await self._send_link_ack(message, source)
            await self.choose_format(message, source)
            return
        if kind == "manifest":
            await self._send_link_ack(message, source)
            await self.enqueue(message, source, False, mode="ytdlp")
            return
        if not self.queue or not self.cache or not self.app:
            # Keep the pre-existing link flow for a bot object that has not
            # finished startup. In a running bot direct links use the mirror
            # worker below; this branch also keeps command handlers safe to
            # invoke during the startup window.
            await self._send_link_ack(message, source)
            await self.choose_format(message, source)
            return
        await self.enqueue(message, source, False, mode="smart")

    async def save_restricted_command(self, _: Client, message: Message) -> None:
        """Retrieve an authorized Telegram message link through the safe queue."""
        source = extract_url(message.text or message.caption or "")
        if not source or not parse_restricted_source(source):
            await message.reply_text(
                "🔐 <b>Save restricted Telegram content</b>\n\n"
                "Usage: <code>/save https://t.me/channel/123</code>\n"
                "Private links use <code>https://t.me/c/123456/123</code>.\n"
                "Ranges are supported, for example <code>/save https://t.me/c/123/10-14</code>.\n\n"
                "The configured authorized user account must already be able to "
                "access the source chat."
            )
            return
        await self.enqueue(message, source, False, mode="restricted")

    async def save_restricted_check_command(
        self, _: Client, message: Message
    ) -> None:
        """Diagnose authorized-user access without creating a downloaded file."""
        source = extract_url(message.text or message.caption or "")
        if not source or not parse_restricted_source(source):
            await message.reply_text(
                "Usage: <code>/savecheck https://t.me/c/123456/123</code>\n\n"
                "This checks whether the configured authorized user account can "
                "resolve the chat and read the message without downloading it."
            )
            return
        status = await message.reply_text("🔎 Checking Telegram access…")
        try:
            result = await self.restricted.inspect(source)
            parsed = result["source"]
            await status.edit_text(
                "✅ <b>Telegram message is accessible</b>\n"
                f"{DIVIDER}\n"
                f"Chat: <b>{html.escape(str(result['chat_title'])[:180])}</b>\n"
                f"Messages found: <b>{result['message_count']}</b>\n"
                f"Media found: <b>{result['media_count']}</b>\n"
                f"Range size: <b>{parsed.count}</b>"
            )
        except RestrictedContentError as exc:
            await status.edit_text(f"❌ <b>{html.escape(str(exc))}</b>")
        except Exception:
            logger.exception("Restricted Telegram access check failed")
            await status.edit_text(
                "❌ <b>Could not check Telegram access</b>\n\n"
                "Confirm that the authorized user account is a member of the "
                "source chat and try again."
            )

    async def _send_replied_media(
        self,
        command: Message,
        replied: Message,
        *,
        owner_id: int,
    ) -> None:
        if not self.app:
            await command.reply_text("The bot is still starting. Try again in a moment.")
            return
        media_lock = self._telegram_media_locks.setdefault(
            owner_id, asyncio.Lock()
        )
        if media_lock.locked():
            await command.reply_text(
                "⏳ <b>A Telegram media transfer is already running</b>\n\n"
                "Wait for it to finish before starting another one."
            )
            return
        async with media_lock:
            await self._send_replied_media_locked(
                command,
                replied,
                owner_id=owner_id,
            )

    async def _send_replied_media_locked(
        self,
        command: Message,
        replied: Message,
        *,
        owner_id: int,
    ) -> None:
        if not self.app:
            await command.reply_text("The bot is still starting. Try again in a moment.")
            return
        media = self._media_details(replied)
        if not media:
            raise DownloadError("This message does not contain supported media.")
        file_name, file_size, mime_type, _ = media
        max_bytes = self.settings.max_upload_mb * 1024 * 1024
        if file_size > max_bytes:
            raise DownloadError(
                f"That file is too large. The limit is {self.settings.max_upload_mb} MB."
            )
        status = await command.reply_text(
            "🔁 <b>Mirroring Telegram media…</b>\n\n"
            f"<code>{html.escape(file_name[:180])}</code>"
        )
        target_dir = self.settings.work_dir / "mirror"
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"{uuid4().hex}-{Path(file_name).name}"
        downloaded_path = target
        try:
            downloaded = await replied.download(file_name=str(target))
            downloaded_path = Path(downloaded) if downloaded else target
            if not downloaded_path.exists():
                raise DownloadError("Telegram did not return the media file.")
            if downloaded_path.stat().st_size > max_bytes:
                raise DownloadError(
                    f"That file is too large. The limit is {self.settings.max_upload_mb} MB."
                )
            item = DownloadItem(
                path=downloaded_path,
                title=Path(file_name).stem or "Mirrored media",
                url=f"telegram://{replied.chat.id}/{replied.id}",
                duration=None,
                audio_only=bool(
                    (mime_type or "").lower().startswith("audio/")
                    or getattr(replied, "audio", None)
                    or getattr(replied, "voice", None)
                ),
            )
            await self._send_item(
                command.chat.id,
                item,
                owner_id=owner_id,
                status_message=status,
            )
            await status.edit_text(
                "✅ <b>Mirror complete</b>\n\n"
                f"<b>{html.escape(item.title[:180])}</b>"
            )
        finally:
            downloaded_path.unlink(missing_ok=True)
            if downloaded_path.parent == target_dir:
                try:
                    target_dir.rmdir()
                except OSError:
                    pass

    async def store_command(self, _: Client, message: Message) -> None:
        source = extract_url(message.text or message.caption or "")
        if source and parse_restricted_source(source):
            await self.enqueue(
                message,
                source,
                False,
                mode="restricted",
                completion_callback=self._complete_store,
            )
            return
        replied = message.reply_to_message
        if not replied or not self._media_details(replied):
            await message.reply_text(
                "📦 <b>Permanent file store</b>\n\n"
                "Reply to a document, video, audio, photo, animation, voice "
                "message, or video note with <code>/store</code>."
            )
            return
        document = replied.document
        if document and looks_like_cookie_document(document.file_name):
            await message.reply_text(
                "🍪 Cookie files cannot be placed in the file store. "
                "Use <code>/cookies</code> to store them securely."
            )
            return
        await self._store_replied_file(message, replied)

    async def _store_replied_file(self, command: Message, replied: Message) -> None:
        archive_key = f"telegram:{replied.chat.id}:{replied.id}"
        lock = self._store_locks.setdefault(archive_key, asyncio.Lock())
        try:
            async with lock:
                await self._store_replied_file_locked(command, replied, archive_key)
        finally:
            if not lock.locked() and self._store_locks.get(archive_key) is lock:
                self._store_locks.pop(archive_key, None)

    async def _store_replied_file_locked(
        self,
        command: Message,
        replied: Message,
        archive_key: str,
    ) -> None:
        if not self.file_store or not self.app:
            await command.reply_text(
                "⏳ <b>Permanent file store is still starting</b>\n\n"
                "Please try again in a moment."
            )
            return
        media = self._media_details(replied)
        if not media:
            await command.reply_text("❌ This message does not contain supported media.")
            return
        file_name, file_size, mime_type, _ = media
        max_bytes = self.settings.max_upload_mb * 1024 * 1024
        if file_size > max_bytes:
            await command.reply_text(
                f"That file is too large. The limit is {self.settings.max_upload_mb} MB."
            )
            return
        channel_id = self.settings.bin_channel_id
        if not channel_id:
            await command.reply_text(
                "❌ Permanent storage is not configured. Set BIN_CHANNEL_ID first."
            )
            return
        try:
            existing = await self.file_store.find_archive(
                channel_id=channel_id,
                archive_key=archive_key,
                url="",
                audio_only=mime_type.lower().startswith("audio/"),
                size=file_size,
            )
            reused = existing is not None
            stored_message = existing
            if stored_message is None:
                stored_message = await self.app.copy_message(
                    channel_id,
                    replied.chat.id,
                    replied.id,
                )
                stored = await self.file_store.add(
                    channel_id=channel_id,
                    message_id=stored_message.id,
                    name=file_name,
                    title=file_name,
                    mime_type=mime_type,
                    size=file_size,
                    owner_id=command.from_user.id if command.from_user else 0,
                    archive_key=archive_key,
                )
            else:
                stored = existing
            link = await self._store_deep_link(stored.token)
            result_text = (
                "♻️ <b>Already stored — reused existing file</b>\n"
                if reused
                else "✅ <b>File stored permanently</b>\n"
            )
            await command.reply_text(
                result_text
                + f"{DIVIDER}\n"
                f"<b>{html.escape(stored.name[:180])}</b>\n\n"
                "Share this retrieval link:\n"
                f"{link}",
            )
        except Exception:
            logger.exception("Could not store replied media in bin channel")
            await command.reply_text(
                "❌ <b>Could not store this file</b>\n\n"
                "Check that the bot is an administrator in BIN_CHANNEL_ID "
                "with permission to post media, then try again."
            )

    async def _deliver_stored_file(self, message: Message, token: str) -> None:
        if not self.file_store or not self.app or not token:
            await message.reply_text("❌ This file-store link is unavailable.")
            return
        stored = await self.file_store.get(token)
        if not stored:
            await message.reply_text(
                "❌ <b>Stored file not found</b>\n\n"
                "The link may be invalid or the file index may have been removed."
            )
            return
        try:
            await self.app.copy_message(
                message.chat.id,
                stored.channel_id,
                stored.message_id,
            )
        except RPCError:
            logger.exception("Could not retrieve stored file token=%s", token)
            await message.reply_text(
                "❌ Telegram could not retrieve this stored file. "
                "The archive message may have been deleted."
            )

    async def _store_deep_link(self, token: str) -> str:
        if not self.app:
            return f"/start store_{token}"
        me = await self.app.get_me()
        username = getattr(me, "username", None)
        if username:
            return f"https://t.me/{username}?start=store_{token}"
        return f"/start store_{token}"

    async def my_files_command(self, _: Client, message: Message) -> None:
        if not self.file_store:
            await message.reply_text("⏳ <b>File store is still starting</b>")
            return
        user_id = message.from_user.id if message.from_user else 0
        files = await self.file_store.recent(owner_id=user_id, limit=10)
        await self._reply_store_list(message, files, "Your stored files")

    async def store_search_command(self, _: Client, message: Message) -> None:
        if not self.file_store:
            await message.reply_text("⏳ <b>File store is still starting</b>")
            return
        text = message.text or message.caption or ""
        user_id = message.from_user.id if message.from_user else 0
        parts = text.split(maxsplit=1)
        if user_id in self.pending_store_searches and not text.startswith("/"):
            self.pending_store_searches.discard(user_id)
            query = text.strip()
        else:
            query = parts[1].strip() if len(parts) == 2 else ""
        if not query:
            await message.reply_text(
                "Usage: <code>/store_search filename or title</code>"
            )
            return
        user_id = message.from_user.id if message.from_user else 0
        files = await self.file_store.search(query, owner_id=user_id, limit=10)
        await self._reply_store_list(message, files, f"Search: {query}")

    async def store_stats_command(self, _: Client, message: Message) -> None:
        if not self.file_store:
            await message.reply_text("⏳ <b>File store is still starting</b>")
            return
        user_id = message.from_user.id if message.from_user else 0
        stats = await self.file_store.stats(owner_id=user_id)
        await message.reply_text(
            "📊 <b>Your file-store statistics</b>\n"
            f"{DIVIDER}\n"
            f"Files: <b>{stats.count}</b>\n"
            f"Indexed size: <b>{self._format_size(stats.total_size)}</b>\n\n"
            "Stored media remains in the configured Telegram archive channel."
        )

    async def _reply_store_list(
        self,
        message: Message,
        files: list[StoredFile],
        heading: str,
    ) -> None:
        if not files:
            await message.reply_text(
                f"📭 <b>{html.escape(heading)}</b>\n\nNo stored files found."
            )
            return
        lines = [f"📦 <b>{html.escape(heading)}</b>", DIVIDER]
        for index, stored in enumerate(files, 1):
            link = await self._store_deep_link(stored.token)
            lines.append(
                f"{index}. <b>{html.escape(stored.name[:100])}</b>\n"
                f"   {link}"
            )
        await message.reply_text("\n".join(lines))

    @staticmethod
    def _format_size(size: int) -> str:
        value = float(max(0, size))
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if value < 1024 or unit == "TB":
                return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
            value /= 1024

    @staticmethod
    def _media_caption(title: str, source: str) -> str:
        """Build a safe, clickable source caption for delivered media."""
        safe_title = html.escape(title[:220])
        safe_source = html.escape(source[:700])
        parsed = urlparse(source)
        if parsed.scheme.lower() in {"http", "https"} and parsed.netloc:
            source_line = (
                f'Source: <a href="{html.escape(source[:700], quote=True)}">'
                "Open original link</a>"
            )
        else:
            source_line = f"Source: <code>{safe_source}</code>"
        return f"<b>{safe_title}</b>\n\n{source_line}"

    @staticmethod
    def _clickable_cached_caption(caption: str) -> str:
        """Upgrade captions written by older cache versions without changing titles."""
        if "<a " in caption.lower():
            return caption
        match = re.search(r"Source:\s*(?:<code>)?([^\s<\n]+)", caption, re.IGNORECASE)
        if not match:
            return caption
        source = html.unescape(match.group(1))
        parsed = urlparse(source)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            return caption
        link = (
            f'Source: <a href="{html.escape(source, quote=True)}">'
            "Open original link</a>"
        )
        return f"{caption[:match.start()]}{link}{caption[match.end():]}"

    def _upload_progress_callback(
        self,
        job: DownloadJob | None,
        title: str,
        phase: str,
        *,
        status_message: Message | None = None,
    ) -> Callable | None:
        """Return a throttled Pyrogram upload callback without making delivery fragile."""
        if not self.app or (not job and not status_message):
            return None
        state = {"time": 0.0, "percent": -1}

        async def progress(current: int, total: int) -> None:
            try:
                if total <= 0:
                    return
                percent = max(0, min(100, int(current * 100 / total)))
                now = time.monotonic()
                if (
                    percent < 100
                    and now - state["time"] < 1.2
                    and percent - state["percent"] < 3
                ):
                    return
                state.update(time=now, percent=percent)
                filled = round(percent / 100 * 12)
                bar = "▰" * filled + "▱" * (12 - filled)
                text = (
                    f"📤 <b>{html.escape(phase)}</b>\n"
                    f"{DIVIDER}\n"
                    f"<blockquote>{html.escape(title[:180])}</blockquote>\n"
                    f"{bar} <b>{percent}%</b>\n"
                    f"{self._format_size(current)} / {self._format_size(total)}"
                )
                if job and job.status_message_id:
                    await self.app.edit_message_text(
                        job.chat_id,
                        job.status_message_id,
                        text,
                    )
                elif status_message:
                    await status_message.edit_text(text)
            except Exception:
                # Telegram may reject an edit while the upload itself is still valid.
                logger.debug("Upload progress update failed", exc_info=True)

        return progress

    async def _receive_shared_file(self, message: Message) -> bool:
        user_id = message.from_user.id if message.from_user else 0
        pending = self.pending_file_uploads.get(user_id)
        if not pending:
            return False
        self.pending_file_uploads.pop(user_id, None)
        if pending.expires_at <= time.time():
            await message.reply_text(
                "⌛ <b>File link request expired</b>\n\n"
                "Send /filestream again before uploading a file."
            )
            return True
        await self._share_file(message)
        return True

    async def _share_file(
        self,
        message: Message,
        *,
        owner_id: int | None = None,
        response_to: Message | None = None,
    ) -> None:
        user_id = owner_id if owner_id is not None else (
            message.from_user.id if message.from_user else 0
        )
        reply_target = response_to or message
        media = self._media_details(message)
        if not media:
            await reply_target.reply_text(
                "❌ This message does not contain supported Telegram media."
            )
            return
        if not self.file_links:
            await reply_target.reply_text(
                (
                    "⚠️ <b>File links are temporarily unavailable</b>\n\n"
                    "Please try again later."
                    if self._file_links_unavailable
                    else "⏳ <b>File links are still starting</b>\n\n"
                    "Please send the file again in a moment."
                )
            )
            return
        file_name, file_size, mime_type, file_id = media
        max_bytes = self.settings.max_upload_mb * 1024 * 1024
        if file_size > max_bytes:
            await reply_target.reply_text(
                f"That file is too large. The limit is {self.settings.max_upload_mb} MB."
            )
            return
        shared = None
        try:
            shared = await self.file_links.add_media(
                file_id,
                file_name,
                file_size,
                owner_id=user_id,
                content_type=mime_type,
            )
            logger.info(
                "Created Telegram-native file link: name=%s size=%s expires_in=%ss",
                shared.name,
                shared.size,
                max(0, int(shared.expires_at - time.time())),
            )
            stream_url, download_url = self.file_links.urls(shared)
            await reply_target.reply_text(
                "✅ <b>Your file links are ready</b>\n"
                f"{DIVIDER}\n"
                f"<b>{html.escape(shared.name[:180])}</b>\n\n"
                f"⏱ Expires in <b>{self.settings.file_url_ttl // 3600} hour(s)</b>.\n"
                "Tap a button below to stream or download.\n"
                "Anyone with an active button link can access the file.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("▶️ Stream", url=stream_url),
                            InlineKeyboardButton("⬇️ Download", url=download_url),
                        ],
                    ]
                ),
            )
        except Exception as exc:
            logger.warning("Could not create file links: %s", exc)
            if shared and self.file_links:
                await self.file_links.remove(shared.token)
            await reply_target.reply_text(
                "❌ <b>Could not create file links</b>\n\n"
                "Please try /filestream again with the file."
            )

    async def _complete_file_links(
        self,
        job: DownloadJob,
        result: DownloadResult | None,
        error: Exception | None,
    ) -> None:
        """Turn downloaded restricted media into short-lived Telegram file links."""
        if not self.app or not self.file_links:
            await self._update_status(
                job,
                "⚠️ <b>File links are unavailable</b>\n\n"
                "The file-link service is still starting.",
            )
            self._remove_download_items(result)
            return
        if error or not result:
            await self._update_status(
                job,
                f"❌ <b>Could not retrieve Telegram content</b>\n\n"
                f"{html.escape(self._user_facing_error(job.user_id, error))}",
            )
            self._remove_download_items(result)
            asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 45))
            return
        links: list[tuple[str, str, str]] = []
        link_tokens: list[str] = []
        try:
            for item in result.items:
                if item.path.stat().st_size > self.settings.max_upload_mb * 1024 * 1024:
                    raise DownloadError(
                        f"{item.title} is over the {self.settings.max_upload_mb} MB upload limit."
                    )
                archived = await self._archive_item(
                    item,
                    owner_id=job.user_id,
                    job=job,
                )
                if archived:
                    sent = await self.app.get_messages(
                        archived.channel_id,
                        archived.message_id,
                    )
                    if not sent:
                        raise DownloadError(
                            "The archived Telegram media is no longer available."
                        )
                else:
                    sent = await self._upload_link_source(
                        job.chat_id,
                        item,
                        job=job,
                    )
                media = self._media_details(sent)
                if not media:
                    raise DownloadError("Telegram did not return a usable file reference.")
                file_name, file_size, mime_type, file_id = media
                shared = await self.file_links.add_media(
                    file_id,
                    file_name,
                    file_size,
                    owner_id=job.user_id,
                    content_type=mime_type,
                )
                stream_url, download_url = self.file_links.urls(shared)
                links.append((shared.name, stream_url, download_url))
                link_tokens.append(shared.token)
                if not archived:
                    try:
                        await sent.delete()
                    except RPCError:
                        pass
            if not links:
                raise DownloadError("The Telegram link did not contain supported media.")
            lines = ["✅ <b>Telegram file links are ready</b>", DIVIDER]
            for name, stream_url, download_url in links:
                lines.extend(
                    [
                        f"<b>{html.escape(name[:180])}</b>",
                        f"▶️ <a href=\"{html.escape(stream_url, quote=True)}\">Stream file</a>",
                        f"⬇️ <a href=\"{html.escape(download_url, quote=True)}\">Download file</a>",
                        "",
                    ]
                )
            await self._update_status(job, "\n".join(lines))
            asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 120))
        except Exception as exc:
            logger.warning("Could not create restricted file links: %s", exc)
            for token in link_tokens:
                await self.file_links.remove(token)
            await self._update_status(
                job,
                "❌ <b>Could not create Telegram file links</b>\n\n"
                "The temporary files were removed. Please try again.",
            )
            asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 45))
        finally:
            self._remove_download_items(result)

    async def _upload_link_source(
        self,
        chat_id: int,
        item: DownloadItem,
        *,
        job: DownloadJob | None = None,
    ) -> Message:
        """Upload a local item to the request chat so its Bot API file ID can be linked."""
        caption = self._media_caption(item.title, item.url)
        progress = self._upload_progress_callback(job, item.title, "Creating file link")
        if item.audio_only:
            return await self.app.send_audio(
                chat_id,
                str(item.path),
                caption=caption,
                progress=progress,
            )
        if item.path.suffix.lower() in {".mp4", ".m4v", ".mov", ".webm", ".mkv"}:
            try:
                return await self.app.send_video(
                    chat_id,
                    str(item.path),
                    caption=caption,
                    supports_streaming=True,
                    progress=progress,
                )
            except RPCError:
                pass
        return await self.app.send_document(
            chat_id,
            str(item.path),
            caption=caption,
            progress=progress,
        )

    async def _complete_store(
        self,
        job: DownloadJob,
        result: DownloadResult | None,
        error: Exception | None,
    ) -> None:
        """Archive downloaded restricted items and return permanent deep links."""
        if error or not result:
            await self._update_status(
                job,
                f"❌ <b>Could not save Telegram content</b>\n\n"
                f"{html.escape(self._user_facing_error(job.user_id, error))}",
            )
            self._remove_download_items(result)
            asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 45))
            return
        if not self.app or not self.file_store or not self.settings.bin_channel_id:
            await self._update_status(
                job,
                "❌ <b>Permanent storage is not configured</b>\n\n"
                "Set BIN_CHANNEL_ID and try again.",
            )
            self._remove_download_items(result)
            asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 45))
            return
        stored_links: list[str] = []
        try:
            for item in result.items:
                file_size = item.path.stat().st_size
                if file_size > self.settings.max_upload_mb * 1024 * 1024:
                    raise DownloadError(
                        f"{item.title} is over the {self.settings.max_upload_mb} MB upload limit."
                    )
                stored = await self._archive_item(
                    item,
                    owner_id=job.user_id,
                    job=job,
                )
                if not stored:
                    raise DownloadError("Could not archive the Telegram media.")
                stored_links.append(await self._store_deep_link(stored.token))
            await self._update_status(
                job,
                "✅ <b>Telegram content saved permanently</b>\n"
                f"{DIVIDER}\n\n"
                + "\n\n".join(stored_links),
            )
            asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 180))
        except Exception:
            logger.exception("Could not store restricted Telegram content")
            await self._update_status(
                job,
                "❌ <b>Could not save Telegram content</b>\n\n"
                "The temporary files were removed. Please try again.",
            )
            asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 45))
        finally:
            self._remove_download_items(result)

    async def _upload_archive_source(self, item: DownloadItem) -> Message:
        caption = self._media_caption(item.title, item.url)
        if item.audio_only:
            return await self.app.send_audio(
                self.settings.bin_channel_id,
                str(item.path),
                caption=caption,
            )
        try:
            return await self.app.send_video(
                self.settings.bin_channel_id,
                str(item.path),
                caption=caption,
                supports_streaming=True,
            )
        except RPCError:
            return await self.app.send_document(
                self.settings.bin_channel_id,
                str(item.path),
                caption=caption,
            )

    async def _download_restricted(
        self,
        source_url: str,
        user_id: int,
        *,
        progress: Callable[[ProgressSnapshot], Awaitable[None]] | None,
        cancel_check: Callable[[], bool] | None,
        use_archive: bool = True,
    ) -> DownloadResult:
        """Retrieve restricted Telegram media, optionally reusing the bin archive.

        Voice playback deliberately disables archive reuse: it needs a temporary
        local source for PyTgCalls and must not create, read, or upload a
        durable bin-channel copy. Download/save/file-link workflows retain the
        archive-first behavior for deduplication and persistence.
        """
        source = parse_restricted_source(source_url)
        if (
            use_archive
            and source
            and self.app
            and self.file_store
            and self.settings.bin_channel_id
            and source.count <= self.settings.restricted_max_messages
        ):
            archive_urls = [
                f"{source.url}#message={message_id}"
                for message_id in range(source.start_id, source.end_id + 1)
            ]
            stored = await self.file_store.find_archives_by_urls(
                channel_id=self.settings.bin_channel_id,
                urls=archive_urls,
            )
            if len(stored) == len(archive_urls):
                try:
                    items: list[DownloadItem] = []
                    for index, record in enumerate(stored, 1):
                        if cancel_check and cancel_check():
                            raise DownloadCancelled("Download cancelled.")
                        if progress:
                            await progress(
                                ProgressSnapshot(
                                    percent=(index - 1) * 100 / len(stored),
                                    status="archived",
                                    title=record.title,
                                    playlist_index=index,
                                    playlist_count=len(stored),
                                )
                            )
                        message = await self.app.get_messages(
                            record.channel_id,
                            record.message_id,
                        )
                        media = self._media_details(message) if message else None
                        if not message or not media:
                            raise DownloadError(
                                "An archived Telegram media message is unavailable."
                            )
                        path = await self._download_stored_media(
                            record,
                            title=record.title,
                            subdirectory="restricted-archive",
                        )
                        audio_only = (
                            (record.mime_type or "").lower().startswith("audio/")
                            or Path(record.name).suffix.lower()
                            in {".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav"}
                            or bool(getattr(message, "audio", None))
                            or bool(getattr(message, "voice", None))
                        )
                        items.append(
                            DownloadItem(
                                path=path,
                                title=record.title[:180] or Path(record.name).stem,
                                url=record.url,
                                duration=getattr(
                                    getattr(message, "video", None),
                                    "duration",
                                    None,
                                ),
                                audio_only=audio_only,
                            )
                        )
                    if progress:
                        await progress(
                            ProgressSnapshot(
                                percent=100,
                                status="complete",
                                title=f"{len(items)} archived Telegram message(s)",
                            )
                        )
                    logger.info(
                        "Reusing %s indexed bin-channel item(s) for restricted source",
                        len(items),
                    )
                    return DownloadResult(
                        items=items,
                        url=source.url,
                        item_count=len(items),
                    )
                except DownloadCancelled:
                    self._remove_download_items(
                        DownloadResult(items=items, url=source.url)
                    )
                    raise
                except Exception:
                    self._remove_download_items(
                        DownloadResult(items=items, url=source.url)
                    )
                    logger.info(
                        "Indexed archive was incomplete; falling back to restricted source"
                    )

        return await self.restricted.download(
            source_url,
            user_id,
            progress=progress,
            cancel_check=cancel_check,
        )

    async def _download_stored_media(
        self,
        stored: StoredFile,
        *,
        title: str,
        subdirectory: str,
    ) -> Path:
        if not self.app:
            raise DownloadError("Telegram is not connected.")
        target_dir = self.settings.work_dir / subdirectory / uuid4().hex
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / (Path(stored.name or title or "telegram-media").name)
        try:
            message = await self.app.get_messages(
                stored.channel_id,
                stored.message_id,
            )
            if not message or not self._media_details(message):
                raise DownloadError("The archived Telegram media is no longer available.")
            downloaded = await message.download(file_name=str(target))
            path = Path(downloaded) if downloaded else target
            if not path.exists() or not path.is_file():
                raise DownloadError("Telegram did not return the archived media file.")
            if path.stat().st_size > self.settings.max_download_bytes:
                raise DownloadError("The archived Telegram media exceeds the download limit.")
            return path
        except Exception:
            shutil.rmtree(target_dir, ignore_errors=True)
            raise

    def _is_archived_materialized(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(
                (self.settings.work_dir / "restricted-archive").resolve()
            )
            return True
        except ValueError:
            return False

    async def _download_archived_for_playback(
        self,
        stored: StoredFile,
        *,
        title: str,
    ) -> Path:
        """Materialize the durable bin-channel copy for PyTgCalls."""
        path = await self._download_stored_media(
            stored,
            title=title,
            subdirectory="voice-chat",
        )
        if path.stat().st_size > self.settings.max_upload_mb * 1024 * 1024:
            path.unlink(missing_ok=True)
            shutil.rmtree(path.parent, ignore_errors=True)
            raise DownloadError(
                f"{title} is over the {self.settings.max_upload_mb} MB upload limit."
            )
        return path

    @staticmethod
    def _remove_download_items(result: DownloadResult | None) -> None:
        if not result:
            return
        parents: set[Path] = set()
        for item in result.items:
            parents.add(item.path.parent)
            item.path.unlink(missing_ok=True)
            if item.thumbnail:
                item.thumbnail.unlink(missing_ok=True)
        for parent in parents:
            shutil.rmtree(parent, ignore_errors=True)

    @staticmethod
    def _media_details(
        message: Message,
    ) -> tuple[str, int, str | None, str] | None:
        media_names = (
            ("document", "document.bin"),
            ("video", "video.mp4"),
            ("audio", "audio.mp3"),
            ("photo", "photo.jpg"),
            ("animation", "animation.mp4"),
            ("voice", "voice.ogg"),
            ("video_note", "video-note.mp4"),
        )
        for attribute, fallback_name in media_names:
            media = getattr(message, attribute, None)
            if not media:
                continue
            name = getattr(media, "file_name", None) or fallback_name
            size = int(getattr(media, "file_size", 0) or 0)
            mime_type = getattr(media, "mime_type", None)
            if attribute == "photo":
                mime_type = mime_type or "image/jpeg"
            file_id = getattr(media, "file_id", None)
            if not file_id:
                return None
            return Path(name).name or fallback_name, size, mime_type, file_id
        return None

    async def queue_status(self, _: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else 0
        if not self.queue:
            await message.reply_text("⏳ <b>Starting your workspace…</b>\n\nTry again in a moment.")
            return
        jobs = self.queue.jobs_for(user_id)
        if not jobs:
            await message.reply_text(
                "📭 <b>Your queue is empty</b>\n\n"
                "Send a YouTube link whenever you’re ready.",
                reply_markup=self._home_keyboard(),
            )
            return
        active_ids = {job.id for job in self.queue.active_jobs()}
        lines = ["📋 <b>Your downloads</b>", DIVIDER]
        for job in jobs:
            state = "⬇️ downloading" if job.id in active_ids else "🕒 waiting"
            kind = "MP3" if job.audio_only else "video"
            lines.append(
                f"• <code>{job.id}</code>  ·  {kind}  ·  "
                f"{quality_label(job.quality, job.audio_only)}  ·  {state}"
            )
        await message.reply_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛑 Cancel all", callback_data="yt:cancel:all")],
                    [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
                ]
            ),
        )

    async def cancel_command(self, _: Client, message: Message) -> None:
        if not self.queue:
            await message.reply_text("⏳ The queue is still starting.")
            return
        user_id = message.from_user.id if message.from_user else 0
        parts = (message.text or "").split(maxsplit=1)
        job_id = parts[1].strip() if len(parts) == 2 else None
        count = self.queue.cancel(user_id, job_id)
        if count:
            target = f"job <code>{job_id}</code>" if job_id else "your downloads"
            await message.reply_text(
                f"🛑 <b>Cancellation requested</b>\n\n"
                f"{target.capitalize()} will stop at the next safe checkpoint.",
            )
        else:
            await message.reply_text(
                "🔎 <b>No matching download found</b>\n\n"
                "Use /queue to see your active job IDs.",
            )

    async def audio_command(self, _: Client, message: Message) -> None:
        url = extract_url(message.text or "")
        if url:
            await self._send_link_ack(message, url)
            await self.enqueue(message, url, audio_only=True)
        else:
            await message.reply_text(
                "🎵 <b>MP3 mode</b>\n\nUsage: <code>/audio https://youtu.be/…</code>"
            )

    async def search_command(self, _: Client, message: Message) -> None:
        text = message.text or ""
        user_id = message.from_user.id if message.from_user else 0
        parts = text.split(maxsplit=1)
        if user_id in self.pending_music_searches and not text.startswith("/"):
            self.pending_music_searches.discard(user_id)
            query = text.strip()
        else:
            query = parts[1].strip() if len(parts) == 2 else ""
        if not query:
            await message.reply_text(
                "🎧 <b>Search music</b>\n\n"
                "Usage: <code>/search Hamra pyaar</code>\n"
                "You can also use <code>/song artist title</code>."
            )
            return
        if len(query) > 120:
            await message.reply_text(
                "Please keep the search under 120 characters so results stay focused."
            )
            return
        if not self.downloader:
            await message.reply_text(
                "⏳ <b>Search is still starting</b>\n\nTry again in a moment."
            )
            return

        user_id = message.from_user.id if message.from_user else 0
        searching = await message.reply_text(
            "🔎 <b>Searching YouTube</b>\n\n"
            f"<code>{html.escape(query)}</code>\n\n"
            "Finding the best matches…"
        )
        try:
            results = await self.downloader.search(query, user_id)
        except DownloadError as exc:
            await searching.edit_text(
                f"❌ <b>Search failed</b>\n\n"
                f"{self._user_facing_error(user_id, exc)}"
            )
            asyncio.create_task(self._delete_later(message.chat.id, searching.id, 60))
            return
        except Exception:
            logger.exception("YouTube search failed")
            await searching.edit_text(
                "❌ <b>Search failed</b>\n\n"
                "YouTube search is temporarily unavailable. Please try again."
            )
            asyncio.create_task(self._delete_later(message.chat.id, searching.id, 60))
            return

        if not results:
            await searching.edit_text(
                "🔎 <b>No results found</b>\n\n"
                "Try a song title, artist name, or a shorter search."
            )
            asyncio.create_task(self._delete_later(message.chat.id, searching.id, 30))
            return

        results = results[:8]
        token = uuid4().hex[:12]
        self.pending_searches[token] = PendingSearch(
            query=query,
            results=results,
            user_id=user_id,
            chat_id=message.chat.id,
        )
        await searching.edit_text(
            self._search_results_text(query, results),
            reply_markup=self._search_keyboard(token, results),
        )
        asyncio.create_task(self._expire_search(token, searching))

    async def _expire_search(self, token: str, prompt: Message) -> None:
        await asyncio.sleep(120)
        self.pending_searches.pop(token, None)
        try:
            await prompt.delete()
        except RPCError:
            pass

    @staticmethod
    def _format_search_duration(duration: int | None) -> str:
        if duration is None or duration < 0:
            return ""
        minutes, seconds = divmod(int(duration), 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        return f"{minutes}:{seconds:02d}"

    @classmethod
    def _search_results_text(
        cls, query: str, results: list[SearchResult]
    ) -> str:
        results = results[:8]
        lines = [
            "🎧 <b>YouTube music search</b>",
            DIVIDER,
            f"Query: <code>{html.escape(query[:120])}</code>",
            "",
            "Choose a result to open the Video / MP3 download options:",
        ]
        for index, result in enumerate(results, 1):
            meta = " • ".join(
                part
                for part in (
                    result.uploader,
                    cls._format_search_duration(result.duration),
                )
                if part
            )
            lines.append(
                f"\n<b>{index}.</b> {html.escape(result.title[:180])}"
                + (f"\n   <i>{html.escape(meta[:120])}</i>" if meta else "")
            )
        return "\n".join(lines)

    @staticmethod
    def _search_keyboard(
        token: str, results: list[SearchResult]
    ) -> InlineKeyboardMarkup:
        results = results[:8]
        rows = [
            [
                InlineKeyboardButton(
                    f"{index + 1}. {result.title[:48]}",
                    callback_data=f"yt:search:{token}:{index}",
                )
            ]
            for index, result in enumerate(results)
        ]
        rows.append(
            [InlineKeyboardButton("✖️ Close", callback_data=f"yt:xsearch:{token}")]
        )
        return InlineKeyboardMarkup(rows)

    async def url_message(self, _: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else 0
        if user_id in self.pending_music_searches:
            self.pending_music_searches.discard(user_id)
            await self.search_command(_, message)
            return
        if user_id in self.pending_store_searches:
            self.pending_store_searches.discard(user_id)
            await self.store_search_command(_, message)
            return
        url = extract_url(message.text or "")
        if not url:
            return
        if parse_restricted_source(url):
            await self.enqueue(message, url, False, mode="restricted")
            return
        await self._send_link_ack(message, url)
        await self.choose_format(message, url)

    async def _send_link_ack(self, message: Message, url: str) -> Message | None:
        playlist = is_playlist_url(normalize_url(url))
        try:
            acknowledgement = await message.reply_text(
                self._link_ack_text(1, playlist),
            )
        except RPCError:
            return None
        await self._animate_link_ack(acknowledgement, playlist)
        return acknowledgement

    async def _animate_link_ack(
        self, acknowledgement: Message, playlist: bool
    ) -> None:
        try:
            await asyncio.sleep(0.65)
            await acknowledgement.edit_text(self._link_ack_text(2, playlist))
            await asyncio.sleep(0.65)
            await acknowledgement.edit_text(self._link_ack_text(3, playlist))
            await asyncio.sleep(1.4)
        except RPCError:
            pass
        finally:
            try:
                await acknowledgement.delete()
            except RPCError:
                pass

    @staticmethod
    def _link_ack_text(stage: int, playlist: bool) -> str:
        source = "YouTube playlist" if playlist else "YouTube link"
        if stage == 2:
            return (
                "2️⃣ 🧭 <b>Reading your source</b>\n\n"
                f"Checking this {source}…"
            )
        if stage == 3:
            return (
                "3️⃣ ✨ <b>Source ready</b>\n\n"
                f"Your {source} download options are opening below."
            )
        return (
            "1️⃣ 🔗 <b>Link received</b>\n\n"
            f"Preparing your {source}…"
        )

    async def choose_format(
        self, message: Message, url: str, user_id: int | None = None
    ) -> None:
        token = uuid4().hex[:12]
        owner_id = user_id or (message.from_user.id if message.from_user else 0)
        self.pending_choices[token] = PendingChoice(url, owner_id, message.chat.id)
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📹 Video", callback_data=f"yt:v:{token}"),
                    InlineKeyboardButton("🎵 Audio MP3", callback_data=f"yt:a:{token}"),
                ],
                [
                    InlineKeyboardButton("✖️ Cancel", callback_data=f"yt:x:{token}"),
                ],
            ]
        )
        prompt = await message.reply_text(
            f"🎬 <b>{BRAND}</b>\n"
            f"{DIVIDER}\n"
            "<b>Choose your download</b>\n\n"
            "📹 Video keeps the best available picture and includes a thumbnail.\n"
            "🎵 MP3 extracts audio at high quality.\n\n"
            "After choosing a format, you can select the exact quality or let "
            "me choose the best available option.\n\n"
            "For playlists, your choice applies to every available item.\n"
            "⏱ This menu expires automatically.",
            reply_markup=keyboard,
        )
        asyncio.create_task(self._expire_choice(token, prompt))

    async def _expire_choice(self, token: str, prompt: Message) -> None:
        try:
            await asyncio.sleep(120)
            self.pending_choices.pop(token, None)
            await prompt.delete()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("Could not expire download menu %s", token, exc_info=True)

    async def format_choice(self, _: Client, callback: CallbackQuery) -> None:
        data = callback.data or ""
        if data.startswith("vc:"):
            await self._vc_callback(callback)
            return
        if data.startswith("vqy:"):
            parts = data.split(":")
            if len(parts) == 4 and parts[1] == "q":
                await self._voice_quality_choice(callback, parts[2], parts[3])
            elif len(parts) == 3 and parts[1] == "x":
                await self._voice_quality_cancel(callback, parts[2])
            else:
                await callback.answer("This quality menu is no longer available.", show_alert=True)
            return
        parts = data.split(":")
        if len(parts) not in {3, 4, 5} or parts[0] != "yt":
            await callback.answer()
            return
        if len(parts) == 5 and parts[1] == "quality":
            _, _, mode, quality, token = parts
            await self._quality_choice(callback, mode, quality, token)
            return
        if len(parts) == 4 and parts[1] == "search":
            _, _, token, index = parts
            await self._search_choice(callback, token, index)
            return
        if len(parts) == 3 and parts[1] == "xsearch":
            await self._close_search(callback, parts[2])
            return
        choice, token = parts[1], parts[2]
        if choice == "help":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    self._help_text(),
                    reply_markup=self._section_nav(include_help=False),
                )
            return
        if choice == "home":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    self._welcome_text(),
                    reply_markup=self._home_keyboard(),
                )
            return
        if choice == "download":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    self._download_text(),
                    reply_markup=self._download_keyboard(),
                )
            return
        if choice == "music":
            self.pending_music_searches.add(callback.from_user.id)
            await callback.answer("Type an artist or song in this chat.")
            if callback.message:
                await callback.message.edit_text(
                    self._music_text(),
                    reply_markup=self._music_keyboard(),
                )
            return
        if choice == "files":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    self._files_text(),
                    reply_markup=self._files_keyboard(),
                )
            return
        if choice == "voice":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    self._voice_text(),
                    reply_markup=self._voice_keyboard(),
                )
            return
        if choice == "settings":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    self._settings_text(),
                    reply_markup=self._settings_keyboard(),
                )
            return
        if choice == "advanced":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    self._advanced_text(),
                    reply_markup=self._section_nav(include_help=False),
                )
            return
        if choice == "files_list":
            await self._files_list_callback(callback)
            return
        if choice == "files_search":
            self.pending_store_searches.add(callback.from_user.id)
            await callback.answer("Type a filename or title in this chat.")
            if callback.message:
                await callback.message.edit_text(
                    "🔎 <b>Search saved files</b>\n\n"
                    "Type a filename or title in this chat.\n"
                    "I’ll show matching permanent file links.",
                    reply_markup=self._files_keyboard(),
                )
            return
        if choice == "files_stats":
            await self._files_stats_callback(callback)
            return
        if choice == "voice_panel":
            await self._vc_panel_callback(callback)
            return
        if choice in {"voice_status", "voice_setup"}:
            command = "/vcstatus" if choice == "voice_status" else "/vcsetup"
            await callback.answer(f"Run {command} in the group to continue.")
            if callback.message:
                await callback.message.edit_text(
                    self._voice_text()
                    + f"\n\nNext step: send <code>{command}</code> in the target group.",
                    reply_markup=self._voice_keyboard(),
                )
            return
        if choice == "cookie_status":
            await callback.answer()
            if callback.message:
                await self._edit_cookie_status(callback.message, callback.from_user.id)
            return
        if choice == "deletecookies":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    "🗑 <b>Remove stored cookies?</b>\n\n"
                    "This will permanently delete your encrypted YouTube browser session "
                    "from this bot.",
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    "Yes, delete", callback_data="yt:delete_confirm:menu"
                                ),
                                InlineKeyboardButton(
                                    "Keep them", callback_data="yt:delete_cancel:menu"
                                ),
                            ]
                        ]
                    ),
                )
            return
        if choice == "delete_confirm":
            deleted = self.cookies.delete(callback.from_user.id)
            await callback.answer("Cookies deleted." if deleted else "No cookies stored.")
            if callback.message:
                await callback.message.edit_text(
                    (
                        "✅ <b>Cookies removed</b>\n\n"
                        "Your encrypted browser session was permanently deleted."
                        if deleted
                        else "ℹ️ <b>No cookies were stored</b>\n\nNothing needed to be removed."
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")]]
                    ),
                )
            return
        if choice == "delete_cancel":
            await callback.answer("Nothing was deleted.")
            if callback.message:
                await callback.message.edit_text(
                    self._cookie_help_text(),
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("🔐 Cookie status", callback_data="yt:cookie_status:menu")],
                            [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
                        ]
                    ),
                )
            return
        if choice == "cookies":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    self._cookie_help_text(),
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("🔐 Cookie status", callback_data="yt:cookie_status:menu")],
                            [InlineKeyboardButton("🏠 Home", callback_data="yt:home:menu")],
                        ]
                    ),
                )
            return
        if choice == "queue":
            await callback.answer()
            await self._show_queue_callback(callback)
            return
        if choice == "back":
            await callback.answer()
            if callback.message:
                await callback.message.edit_text(
                    self._welcome_text(),
                    reply_markup=self._home_keyboard(),
                )
            return
        if choice == "cancel":
            await self._cancel_callback(callback, token)
            return
        if choice == "x":
            self.pending_choices.pop(token, None)
            await callback.answer("Download menu closed.")
            if callback.message:
                try:
                    await callback.message.delete()
                except RPCError:
                    pass
            return
        if choice == "format":
            pending = self.pending_choices.get(token)
            if pending and callback.from_user.id == pending.user_id and callback.message:
                await callback.answer()
                await callback.message.edit_text(
                    self._format_prompt_text(),
                    reply_markup=self._format_keyboard(token),
                )
            return
        pending = self.pending_choices.get(token)
        if not pending:
            await callback.answer("This menu has expired. Send the link again.", show_alert=True)
            return
        if callback.from_user.id != pending.user_id:
            await callback.answer("This download menu belongs to another user.", show_alert=True)
            return
        if choice not in {"v", "a"}:
            await callback.answer("That download option is no longer available.", show_alert=True)
            return
        self.pending_choices.pop(token, None)
        self.pending_choices[token] = pending
        await callback.answer("Choose your quality…")
        if callback.message:
            await callback.message.edit_text(
                self._quality_prompt_text(choice == "a"),
                reply_markup=self._quality_keyboard(token, choice == "a"),
            )

    async def _search_choice(
        self, callback: CallbackQuery, token: str, index_text: str
    ) -> None:
        session = self.pending_searches.get(token)
        if not session:
            await callback.answer("This search has expired. Search again.", show_alert=True)
            return
        if callback.from_user.id != session.user_id:
            await callback.answer("This search belongs to another user.", show_alert=True)
            return
        try:
            index = int(index_text)
            result = session.results[index]
        except (ValueError, IndexError):
            await callback.answer("That result is no longer available.", show_alert=True)
            return

        self.pending_searches.pop(token, None)
        await callback.answer("Result selected. Choose Video or MP3.")
        if callback.message:
            selected_message = callback.message
            await callback.message.edit_text(
                "✅ <b>Selected</b>\n\n"
                f"<blockquote>{html.escape(result.title[:220])}</blockquote>\n"
                "Opening download options…"
            )
            await self.choose_format(
                selected_message,
                result.url,
                user_id=session.user_id,
            )
            try:
                await selected_message.delete()
            except RPCError:
                pass

    async def _close_search(self, callback: CallbackQuery, token: str) -> None:
        session = self.pending_searches.get(token)
        if session and callback.from_user.id != session.user_id:
            await callback.answer("This search belongs to another user.", show_alert=True)
            return
        self.pending_searches.pop(token, None)
        await callback.answer("Search closed.")
        if callback.message:
            try:
                await callback.message.delete()
            except RPCError:
                pass

    @staticmethod
    def _format_prompt_text() -> str:
        return (
            f"🎬 <b>{BRAND}</b>\n"
            f"{DIVIDER}\n"
            "<b>Choose your download</b>\n\n"
            "📹 Video includes the best available picture and thumbnail when available.\n"
            "🎵 MP3 extracts audio at high quality.\n\n"
            "⏱ This menu expires automatically."
        )

    @staticmethod
    def _format_keyboard(token: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📹 Video", callback_data=f"yt:v:{token}"),
                    InlineKeyboardButton("🎵 Audio MP3", callback_data=f"yt:a:{token}"),
                ],
                [InlineKeyboardButton("✖️ Cancel", callback_data=f"yt:x:{token}")],
            ]
        )

    @staticmethod
    def _quality_prompt_text(audio_only: bool) -> str:
        kind = "MP3 audio" if audio_only else "video"
        return (
            f"🎚 <b>Choose {kind} quality</b>\n"
            f"{DIVIDER}\n"
            "Select a target quality, or let YouTube Studio choose the best "
            "available option.\n\n"
            "Higher quality may take longer and use more data."
        )

    @staticmethod
    def _quality_keyboard(token: str, audio_only: bool) -> InlineKeyboardMarkup:
        choices = AUDIO_QUALITIES if audio_only else VIDEO_QUALITIES
        mode = "a" if audio_only else "v"
        buttons = [
            InlineKeyboardButton(
                label,
                callback_data=f"yt:quality:{mode}:{key}:{token}",
            )
            for key, label in choices
        ]
        rows = [buttons[index:index + 2] for index in range(0, len(buttons), 2)]
        rows.append(
            [
                InlineKeyboardButton("⬅️ Back", callback_data=f"yt:format:{token}"),
                InlineKeyboardButton("✖️ Cancel", callback_data=f"yt:x:{token}"),
            ]
        )
        return InlineKeyboardMarkup(rows)

    async def _quality_choice(
        self, callback: CallbackQuery, mode: str, quality: str, token: str
    ) -> None:
        pending = self.pending_choices.get(token)
        audio_only = mode == "a"
        if not pending:
            await callback.answer("This menu has expired. Send the link again.", show_alert=True)
            return
        if callback.from_user.id != pending.user_id:
            await callback.answer("This menu belongs to another user.", show_alert=True)
            return
        if mode not in {"a", "v"}:
            await callback.answer("That quality menu is no longer available.", show_alert=True)
            return
        normalized_quality = normalize_quality(quality, mode == "a")
        if normalized_quality != quality:
            await callback.answer("That quality option is no longer available.", show_alert=True)
            return
        self.pending_choices.pop(token, None)
        await callback.answer("Preparing your download…")
        callback_message = callback.message
        if callback_message:
            try:
                await callback_message.delete()
            except RPCError:
                pass
            await self.enqueue(
                callback_message,
                pending.url,
                audio_only=audio_only,
                quality=quality,
                user_id=pending.user_id,
            )

    async def _show_queue_callback(self, callback: CallbackQuery) -> None:
        if not callback.message or not self.queue:
            if callback.message:
                await callback.message.edit_text("⏳ The queue is still starting.")
            return
        jobs = self.queue.jobs_for(callback.from_user.id)
        if not jobs:
            await callback.message.edit_text(
                "📭 <b>Your queue is empty.</b>",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Back", callback_data="yt:back:menu")]]
                ),
            )
            return
        active_ids = {job.id for job in self.queue.active_jobs()}
        lines = ["📋 <b>Your downloads</b>\n"]
        for job in jobs:
            state = "⬇️ downloading" if job.id in active_ids else "🕒 waiting"
            lines.append(
                f"• <code>{job.id}</code> — "
                f"{quality_label(job.quality, job.audio_only)} — {state}"
            )
        await callback.message.edit_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛑 Cancel all", callback_data="yt:cancel:all")],
                    [InlineKeyboardButton("⬅️ Back", callback_data="yt:back:menu")],
                ]
            ),
        )

    async def _files_list_callback(self, callback: CallbackQuery) -> None:
        if not callback.message or not self.file_store:
            await callback.answer("File store is still starting.", show_alert=True)
            return
        user_id = callback.from_user.id
        files = await self.file_store.recent(owner_id=user_id, limit=10)
        if not files:
            await callback.answer("No saved files yet.")
            await callback.message.edit_text(
                "📭 <b>Your saved files</b>\n\n"
                "Reply to a Telegram media message with <code>/store</code> "
                "to save your first file.",
                reply_markup=self._files_keyboard(),
            )
            return
        lines = ["📚 <b>Your saved files</b>", DIVIDER]
        for index, stored in enumerate(files, 1):
            link = await self._store_deep_link(stored.token)
            lines.append(f"{index}. <b>{html.escape(stored.name[:100])}</b>\n{link}")
        await callback.answer()
        await callback.message.edit_text(
            "\n".join(lines),
            reply_markup=self._files_keyboard(),
        )

    async def _files_stats_callback(self, callback: CallbackQuery) -> None:
        if not callback.message or not self.file_store:
            await callback.answer("File store is still starting.", show_alert=True)
            return
        stats = await self.file_store.stats(owner_id=callback.from_user.id)
        await callback.answer()
        await callback.message.edit_text(
            "📊 <b>Your storage</b>\n"
            f"{DIVIDER}\n"
            f"Files saved: <b>{stats.count}</b>\n"
            f"Indexed size: <b>{self._format_size(stats.total_size)}</b>\n\n"
            "Your media remains safely in the configured Telegram archive.",
            reply_markup=self._files_keyboard(),
        )

    async def _vc_panel_callback(self, callback: CallbackQuery) -> None:
        if not callback.message:
            await callback.answer("This menu is no longer available.", show_alert=True)
            return
        if not await self._vc_admin_allowed(callback.message):
            await callback.answer("Admin permission required.", show_alert=True)
            return
        try:
            self.voice_chat._require_enabled()
        except RuntimeError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        chat_id = self._vc_target_chat_id(callback.message)
        await callback.answer()
        await callback.message.edit_text(
            self._vc_panel_text(chat_id),
            reply_markup=self._vc_keyboard(),
        )

    async def _cancel_callback(self, callback: CallbackQuery, job_id: str) -> None:
        if not self.queue:
            await callback.answer("Queue is still starting.", show_alert=True)
            return
        count = self.queue.cancel(
            callback.from_user.id,
            None if job_id == "all" else job_id,
        )
        await callback.answer(
            "Cancellation requested." if count else "No matching download found.",
            show_alert=not bool(count),
        )
        if callback.message and count:
            await callback.message.edit_text(
                "🛑 <b>Cancellation requested</b>\n\n"
                "The current transfer will stop at its next safe checkpoint.",
            )

    async def enqueue(
        self,
        message: Message,
        url: str,
        audio_only: bool,
        quality: str = "auto",
        user_id: int | None = None,
        mode: str = "ytdlp",
        torrent_select_files: str | None = None,
        completion_callback: Callable[
            [DownloadJob, DownloadResult | None, Exception | None],
            Awaitable[None],
        ] | None = None,
    ) -> None:
        if not self.queue or not self.cache or not self.app:
            await message.reply_text("The downloader is still starting. Try again in a moment.")
            return
        quality = normalize_quality(quality, audio_only)
        owner_id = user_id or (message.from_user.id if message.from_user else 0)
        hit = await self.cache.get(cache_key(url, audio_only, quality))
        if hit:
            # Older cache entries may be document fallbacks without a
            # thumbnail. Redownload video requests once so the upgraded
            # delivery path can send real media with preview metadata.
            if hit.get("kind") == "document":
                await self.cache.delete(cache_key(url, audio_only, quality))
                hit = None
        if hit:
            try:
                await self._send_cached(message.chat.id, hit)
                return
            except RPCError:
                await self.cache.delete(cache_key(url, audio_only, quality))
        if self.queue.jobs_for(owner_id):
            await message.reply_text(
                "⏳ <b>You already have a download queued</b>\n\n"
                "Wait for it to finish or cancel it from Activity before starting another."
            )
            return

        chat_id = message.chat.id
        status: Message | None = None
        last_update = {"time": 0.0, "value": -1.0, "item": None}

        async def progress(snapshot: ProgressSnapshot) -> None:
            if status is None:
                return
            now = time.monotonic()
            item_key = (snapshot.playlist_index, snapshot.title)
            item_changed = item_key is not None and item_key != last_update["item"]
            if (
                snapshot.percent < 100
                and not item_changed
                and now - last_update["time"] < 1.2
            ):
                return
            if (
                snapshot.percent < 100
                and not item_changed
                and snapshot.percent - last_update["value"] < 2
            ):
                return
            last_update.update(time=now, value=snapshot.percent, item=item_key)
            try:
                await status.edit_text(
                    self._status_text(snapshot, audio_only=audio_only),
                    reply_markup=cancel_markup,
                )
            except RPCError:
                return

        async def deliver_playlist_item(
            item: DownloadItem, item_number: int, total: int
        ) -> bool:
            try:
                await self._update_status(
                    job,
                    self._delivery_text(item.title, item_number, total),
                    reply_markup=cancel_markup,
                )
                await self._send_item(
                    chat_id,
                    item,
                    owner_id=job.user_id,
                    job=job,
                )
                return True
            except Exception as exc:
                logger.warning("Could not deliver playlist item %s: %s", item.title, exc)
                return False
            finally:
                item.path.unlink(missing_ok=True)
                if item.thumbnail:
                    item.thumbnail.unlink(missing_ok=True)

        job = DownloadJob(
            url=url,
            user_id=owner_id,
            chat_id=chat_id,
            audio_only=audio_only,
            callback=completion_callback or self._complete,
            quality=quality,
            progress=progress,
            item_callback=deliver_playlist_item,
            mode=mode,
            torrent_select_files=torrent_select_files,
            restricted_fetcher=self._download_restricted if mode == "restricted" else None,
        )
        cancel_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🛑 Cancel download", callback_data=f"yt:cancel:{job.id}")]]
        )
        status = await self.app.send_message(
            chat_id,
            self._status_text(audio_only=audio_only),
            reply_markup=cancel_markup,
        )
        job.status_message_id = status.id
        try:
            position = await self.queue.submit(job, reject_duplicate_user=True)
        except UserQueueBusy as exc:
            try:
                await status.delete()
            except RPCError:
                pass
            await message.reply_text(f"⏳ {html.escape(str(exc))}")
        except asyncio.QueueFull:
            await status.edit_text(
                "⚠️ <b>Download queue is busy</b>\n\n"
                "Please wait a moment and send the link again."
            )
        else:
            if position > 1:
                await status.edit_text(
                    f"🗂 <b>Added to your queue</b>\n"
                    f"{DIVIDER}\n"
                    f"{'🎵 MP3 audio' if audio_only else '📹 Video'}\n"
                    f"Quality <b>{quality_label(quality, audio_only)}</b>\n"
                    f"Position <b>#{position}</b>\n\n"
                    "It will start automatically when ready.",
                    reply_markup=cancel_markup,
                )
            else:
                await status.edit_text(
                    f"✨ <b>Your download is starting</b>\n"
                    f"{DIVIDER}\n"
                    f"{'🎵 MP3 audio' if audio_only else '📹 Video'}\n"
                    f"Quality <b>{quality_label(quality, audio_only)}</b>\n\n"
                    "Finding the best available quality…",
                    reply_markup=cancel_markup,
                )

    async def _complete(
        self, job: DownloadJob, result: DownloadResult | None, error: Exception | None
    ) -> None:
        if not self.app:
            return
        if isinstance(error, DownloadCancelled):
            await self._update_status(job, "🛑 <b>Download cancelled</b>\n\nTemporary files were removed.")
            asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 15))
            return
        if error or not result:
            text = self._user_facing_error(job.user_id, error)
            await self._update_status(job, f"❌ <b>Download failed</b>\n\n{text}")
            asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 60))
            return
        delivered = result.streamed_count
        failed = result.streamed_failures
        total_items = result.item_count or (delivered + failed + len(result.items))
        for item in result.items:
            try:
                await self._update_status(
                    job,
                    self._delivery_text(
                        item.title,
                        delivered + failed + 1,
                        total_items,
                    ),
                )
                await self._send_item(
                    job.chat_id,
                    item,
                    owner_id=job.user_id,
                    job=job,
                )
                delivered += 1
            except Exception as exc:
                failed += 1
                logger.warning("Could not deliver %s: %s", item.title, exc)
            finally:
                item.path.unlink(missing_ok=True)
                if item.thumbnail:
                    item.thumbnail.unlink(missing_ok=True)
        suffix = f"Delivered {delivered}/{total_items} item(s)."
        if failed:
            suffix += f" {failed} failed."
        if result.playlist_title:
            await self._update_status(
                job,
                "✅ <b>Playlist complete</b>\n"
                f"{DIVIDER}\n"
                f"<blockquote>{html.escape(result.playlist_title[:220])}</blockquote>\n"
                f"{suffix}",
            )
        else:
            await self._update_status(job, f"✅ <b>Complete</b>\n\n{suffix}")
        asyncio.create_task(self._delete_later(job.chat_id, job.status_message_id, 30))
        for parent in {item.path.parent for item in result.items}:
            shutil.rmtree(parent, ignore_errors=True)

    async def _send_item(
        self,
        chat_id: int,
        item: DownloadItem,
        *,
        owner_id: int = 0,
        job: DownloadJob | None = None,
        status_message: Message | None = None,
    ) -> None:
        size_mb = item.path.stat().st_size / (1024 * 1024)
        if size_mb > self.settings.max_upload_mb:
            raise DownloadError(
                f"{item.title} is {size_mb:.0f} MB, over the {self.settings.max_upload_mb} MB upload limit."
            )
        caption = self._media_caption(item.title, item.url)
        archived = await self._archive_item(
            item,
            owner_id=owner_id,
            job=job,
            status_message=status_message,
        )
        cached = await self.cache.get(cache_key(item.url, item.audio_only, item.quality))
        if cached:
            if cached.get("kind") == "document":
                await self.cache.delete(cache_key(item.url, item.audio_only, item.quality))
                cached = None
        if cached:
            try:
                await self._send_cached(chat_id, cached)
                return
            except RPCError:
                await self.cache.delete(cache_key(item.url, item.audio_only, item.quality))

        if archived:
            try:
                archived_message = await self.app.get_messages(
                    archived.channel_id,
                    archived.message_id,
                )
                archived_media = (
                    self._media_details(archived_message)
                    if archived_message
                    else None
                )
                if archived_media:
                    file_id = archived_media[3]
                    if item.audio_only:
                        await self.app.send_audio(chat_id, file_id, caption=caption)
                        kind = "audio"
                    elif getattr(archived_message, "video", None):
                        await self.app.send_video(
                            chat_id,
                            file_id,
                            caption=caption,
                            supports_streaming=True,
                            no_sound=False,
                        )
                        kind = "video"
                    else:
                        await self.app.send_document(chat_id, file_id, caption=caption)
                        kind = "document"
                    await self.cache.set(
                        cache_key(item.url, item.audio_only, item.quality),
                        {"file_id": file_id, "kind": kind, "caption": caption},
                        self.settings.cache_ttl,
                    )
                    return
            except RPCError:
                logger.info(
                    "Archived delivery unavailable; using temporary copy: title=%s",
                    item.title,
                )

        upload_progress = self._upload_progress_callback(
            job,
            item.title,
            "Uploading media",
            status_message=status_message,
        )
        if item.audio_only:
            sent = await self.app.send_audio(
                chat_id,
                str(item.path),
                caption=caption,
                thumb=str(item.thumbnail) if item.thumbnail else None,
                progress=upload_progress,
            )
            file_id = sent.audio.file_id if sent.audio else None
            kind = "audio"
        else:
            try:
                sent = await self.app.send_video(
                    chat_id,
                    str(item.path),
                    caption=caption,
                    thumb=str(item.thumbnail) if item.thumbnail else None,
                    supports_streaming=True,
                    no_sound=False,
                    progress=upload_progress,
                )
                file_id = sent.video.file_id if sent.video else None
                kind = "video"
            except RPCError:
                sent = await self.app.send_document(
                    chat_id,
                    str(item.path),
                    caption=caption,
                    progress=upload_progress,
                )
                file_id = sent.document.file_id if sent.document else None
                kind = "document"
        if file_id:
            await self.cache.set(
                cache_key(item.url, item.audio_only, item.quality),
                {"file_id": file_id, "kind": kind, "caption": caption},
                self.settings.cache_ttl,
            )

    async def _archive_item(
        self,
        item: DownloadItem,
        *,
        owner_id: int = 0,
        job: DownloadJob | None = None,
        status_message: Message | None = None,
    ) -> StoredFile | None:
        """Store every newly downloaded item in the configured Telegram bin."""
        if not self.app or not self.settings.bin_channel_id:
            return
        file_size = item.path.stat().st_size
        archive_key = self._archive_key(item)
        archive_locks = getattr(self, "_archive_locks", None)
        if archive_locks is None:
            archive_locks = {}
            self._archive_locks = archive_locks
        lock = archive_locks.setdefault(archive_key, asyncio.Lock())
        try:
            async with lock:
                return await self._archive_item_locked(
                    item,
                    owner_id=owner_id,
                    file_size=file_size,
                    archive_key=archive_key,
                    job=job,
                    status_message=status_message,
                )
        finally:
            if not lock.locked() and archive_locks.get(archive_key) is lock:
                archive_locks.pop(archive_key, None)

    async def _archive_item_locked(
        self,
        item: DownloadItem,
        *,
        owner_id: int,
        file_size: int,
        archive_key: str,
        job: DownloadJob | None = None,
        status_message: Message | None = None,
    ) -> StoredFile | None:
        file_store = getattr(self, "file_store", None)
        if file_store:
            existing = await file_store.find_archive(
                channel_id=self.settings.bin_channel_id,
                archive_key=archive_key,
                url=item.url,
                audio_only=item.audio_only,
                size=file_size,
            )
            if not existing:
                existing = await self._find_remote_archive(
                    item,
                    file_size=file_size,
                    archive_key=archive_key,
                    owner_id=owner_id,
                )
            if existing:
                logger.info(
                    "Skipping duplicate archive upload: title=%s message_id=%s",
                    item.title,
                    existing.message_id,
                )
                return existing
        caption = (
            self._media_caption(item.title, item.url)
        )
        progress = self._upload_progress_callback(
            job,
            item.title,
            "Archiving media",
            status_message=status_message,
        )
        try:
            archived_message: Message | None = None
            if item.audio_only:
                try:
                    archived_message = await self.app.send_audio(
                        self.settings.bin_channel_id,
                        str(item.path),
                        caption=caption,
                        progress=progress,
                    )
                except RPCError:
                    archived_message = await self.app.send_document(
                        self.settings.bin_channel_id,
                        str(item.path),
                        caption=caption,
                        progress=progress,
                    )
            else:
                try:
                    archived_message = await self.app.send_video(
                        self.settings.bin_channel_id,
                        str(item.path),
                        caption=caption,
                        thumb=str(item.thumbnail) if item.thumbnail else None,
                        supports_streaming=True,
                        no_sound=False,
                        progress=progress,
                    )
                except RPCError:
                    archived_message = await self.app.send_document(
                        self.settings.bin_channel_id,
                        str(item.path),
                        caption=caption,
                        progress=progress,
                    )
            if file_store and archived_message:
                media = self._media_details(archived_message)
                return await file_store.add(
                    channel_id=self.settings.bin_channel_id,
                    message_id=archived_message.id,
                    name=item.path.name,
                    title=item.title,
                    url=item.url,
                    mime_type=media[2] if media else None,
                    size=file_size,
                    owner_id=owner_id,
                    archive_key=archive_key,
                )
            return None
        except Exception:
            logger.exception(
                "Could not archive downloaded item in bin channel: title=%s",
                item.title,
            )
            return None

    async def _find_remote_archive(
        self,
        item: DownloadItem,
        *,
        file_size: int,
        archive_key: str,
        owner_id: int,
    ) -> StoredFile | None:
        """Find and index an older archive message missing from SQLite.

        Telegram search is deliberately a fallback after the local index misses.
        Exact source-caption, media-kind, and byte-size checks avoid treating a
        similarly named or differently encoded file as the same archive.
        """
        if not self.app or not self.file_store:
            return None
        try:
            async for candidate in self.app.search_messages(
                self.settings.bin_channel_id,
                query=item.url,
                limit=20,
            ):
                caption = html.unescape(
                    str(getattr(candidate, "caption", None) or "")
                )
                if item.url not in caption:
                    continue
                media = self._media_details(candidate)
                if not media or media[1] != file_size:
                    continue
                mime_type = (media[2] or "").lower()
                candidate_audio = mime_type.startswith("audio/") or bool(
                    getattr(candidate, "audio", None)
                    or getattr(candidate, "voice", None)
                )
                if candidate_audio != item.audio_only:
                    continue
                return await self.file_store.add(
                    channel_id=self.settings.bin_channel_id,
                    message_id=candidate.id,
                    name=media[0],
                    title=item.title,
                    url=item.url,
                    mime_type=media[2],
                    size=file_size,
                    owner_id=owner_id,
                    archive_key=archive_key,
                )
        except Exception:
            logger.debug(
                "Could not search older archive messages for title=%s",
                item.title,
                exc_info=True,
            )
        return None

    @staticmethod
    def _archive_key(item: DownloadItem) -> str:
        mode = "audio" if item.audio_only else "video"
        normalized = normalize_url(item.url)
        if item.url.startswith("https://t.me/") and "#message=" in item.url:
            normalized = item.url
        return f"{normalized}|mode={mode}|quality={item.quality}"

    def _user_facing_error(self, user_id: int, error: Exception | None) -> str:
        text = str(error) if isinstance(error, DownloadError) else "Download failed."
        if "automated traffic" not in text:
            return text
        health = self.cookies.health(user_id)
        if health.get("present") and health.get("valid"):
            return (
                "YouTube is rejecting this server session even with your stored cookies. "
                "Your cookies are present and valid, so uploading the same file again will not help. "
                "Export a fresh cookie file from an active YouTube browser session and retry later; "
                "some YouTube videos now also require a PO-token provider or a different network."
            )
        return (
            "YouTube blocked this request as automated traffic. "
            "Upload a JSON or Netscape browser cookie export with /cookies and try again."
        )

    async def _send_cached(
        self,
        chat_id: int,
        cached: dict[str, str],
        *,
        caption: str | None = None,
    ) -> None:
        kind = cached["kind"]
        caption = caption or self._clickable_cached_caption(cached["caption"])
        if kind == "audio":
            await self.app.send_audio(chat_id, cached["file_id"], caption=caption)
        elif kind == "document":
            await self.app.send_document(chat_id, cached["file_id"], caption=caption)
        else:
            await self.app.send_video(chat_id, cached["file_id"], caption=caption)

    @staticmethod
    def _status_text(
        snapshot: ProgressSnapshot | None = None,
        audio_only: bool = False,
    ) -> str:
        kind = "🎵 MP3" if audio_only else "📹 Video"
        if snapshot is None:
            return (
                f"⏳ <b>Preparing your {kind.lower()} download</b>\n\n"
                "Checking the request and playlist…"
            )
        percent = max(0, min(100, int(snapshot.percent)))
        filled = percent // 10
        bar = "▰" * filled + "▱" * (10 - filled)
        if snapshot.status == "complete":
            phase = "✅ Playlist prepared"
        elif snapshot.status == "finished":
            phase = "⚙️ Finalizing this item…"
        elif snapshot.status == "analyzing":
            phase = "🧠 Choosing the best downloader…"
        else:
            phase = "⬇️ Downloading"
        playlist_line = ""
        if snapshot.playlist_index and snapshot.playlist_count:
            playlist_line = (
                f"📚 <b>Playlist item {snapshot.playlist_index}/"
                f"{snapshot.playlist_count}</b>\n"
            )
        title = (
            f"\n<blockquote>{html.escape(snapshot.title[:180])}</blockquote>"
            if snapshot.title
            else ""
        )
        details: list[str] = []
        if snapshot.total:
            details.append(
                f"📦 {Bot._format_size(snapshot.downloaded)} / "
                f"{Bot._format_size(snapshot.total)}"
            )
        elif snapshot.downloaded:
            details.append(f"📦 {Bot._format_size(snapshot.downloaded)}")
        if snapshot.speed:
            details.append(f"⚡ {Bot._format_rate(snapshot.speed)}")
        if snapshot.eta is not None and snapshot.status != "finished":
            details.append(f"⏱ ETA {Bot._format_duration(snapshot.eta)}")
        telemetry = f"\n{' • '.join(details)}" if details else ""
        return (
            f"{phase} <b>{kind}</b>\n"
            f"{DIVIDER}\n"
            f"{playlist_line}"
            f"{bar} <b>{percent}%</b>\n\n"
            f"{title}{telemetry}\n\n"
            "You can cancel this download at any time."
        )

    @staticmethod
    def _format_rate(value: float) -> str:
        units = ("B/s", "KB/s", "MB/s", "GB/s")
        amount = float(value)
        unit = units[0]
        for unit in units:
            if amount < 1024 or unit == units[-1]:
                break
            amount /= 1024
        return f"{amount:.1f} {unit}"

    @staticmethod
    def _format_duration(seconds: int) -> str:
        seconds = max(0, int(seconds))
        minutes, remaining = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes:02d}m"
        if minutes:
            return f"{minutes}m {remaining:02d}s"
        return f"{remaining}s"

    @staticmethod
    def _delivery_text(title: str, item_number: int, total: int) -> str:
        safe_title = html.escape(title[:240])
        item_label = f"Item {item_number} of {total}" if total > 1 else "Your media is ready"
        return (
            "📬 <b>Finishing delivery</b>\n"
            f"{DIVIDER}\n"
            f"<blockquote>{safe_title}</blockquote>\n"
            f"{item_label}\n\n"
            "Preparing it for Telegram…"
        )

    async def _update_status(
        self,
        job: DownloadJob,
        text: str,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> None:
        if not self.app or not job.status_message_id:
            return
        try:
            await self.app.edit_message_text(
                job.chat_id,
                job.status_message_id,
                text,
                reply_markup=reply_markup,
            )
        except RPCError:
            pass

    async def _delete_later(
        self, chat_id: int, message_id: int | None, delay: int
    ) -> None:
        if not self.app or not message_id:
            return
        await asyncio.sleep(delay)
        try:
            await self.app.delete_messages(chat_id, message_id)
        except RPCError:
            pass

    async def setup(self) -> None:
        self.cache = await create_cache(self.settings)
        if self.settings.mongodb_url:
            mongo_store = MongoFileStore(
                self.settings.mongodb_url,
                self.settings.mongodb_database,
            )
            try:
                await mongo_store.start()
                migrated = await mongo_store.migrate_from_sqlite(
                    self.settings.file_store_db
                )
                self.file_store = mongo_store
                self.metadata_store_name = "mongodb"
                if migrated:
                    logger.info(
                        "Migrated %s local metadata record(s) into MongoDB",
                        migrated,
                    )
                logger.info(
                    "MongoDB metadata store connected: database=%s",
                    self.settings.mongodb_database,
                )
            except Exception:
                logger.exception(
                    "MongoDB metadata store unavailable; using SQLite fallback"
                )
                await mongo_store.close()
        if self.file_store is None:
            self.file_store = FileStore(self.settings.file_store_db)
            await self.file_store.start()
            self.metadata_store_name = "sqlite"
            logger.info("SQLite metadata store connected")
        downloader = YTDLPDownloader(
            self.settings.work_dir,
            self.cookies,
            self.settings.pot_provider_url,
            self.settings.max_download_bytes,
        )
        self.downloader = downloader
        self.queue = DownloadQueue(
            downloader,
            workers=self.settings.workers,
            max_size=self.settings.max_queue_size,
        )
        await self.queue.start()
        self._pending_cleanup_task = asyncio.create_task(
            self._cleanup_pending_uploads()
        )
        await self.health_server.start()

    async def _cleanup_pending_uploads(self) -> None:
        try:
            while True:
                try:
                    now = time.time()
                    expired = [
                        user_id
                        for user_id, pending in self.pending_file_uploads.items()
                        if pending.expires_at <= now
                    ]
                    for user_id in expired:
                        self.pending_file_uploads.pop(user_id, None)
                    expired_auth: list[PendingRestrictedAuthorization] = []
                    async with self._restricted_auth_lock:
                        for user_id, pending in list(
                            self.pending_restricted_auth.items()
                        ):
                            if pending.expires_at <= now:
                                # Remove the exact object observed while
                                # holding the lock. A new /rauthorize update
                                # must never be removed by an older cleanup
                                # pass.
                                removed = self.pending_restricted_auth.pop(
                                    user_id, None
                                )
                                if removed is pending:
                                    expired_auth.append(pending)
                    if expired_auth:
                        async with self._restricted_auth_io_lock:
                            await self.restricted_session.cancel_login()
                        for pending in expired_auth:
                            if self.app:
                                await self.app.send_message(
                                    pending.chat_id,
                                    "⌛ <b>Authorization expired</b>\n\n"
                                    "No login data was retained. Run "
                                    "<code>/rauthorize</code> again.",
                                )
                    # Prevent unbounded growth of "expecting user input" sets.
                    # These are populated when a user opens a music/store search
                    # prompt and discarded should be automatically when they type,
                    # but they never expire if the user just closes the menu without
                    # typing. Periodic pruning is safe because the sets only gate
                    # message routing; the next button press re-adds the user.
                    if len(self.pending_music_searches) > 500:
                        self.pending_music_searches.clear()
                    if len(self.pending_store_searches) > 500:
                        self.pending_store_searches.clear()
                    # Prune voice-quality choice entries older than 10 minutes.
                    if len(self.pending_voice_choices) > 200:
                        self.pending_voice_choices.clear()
                    # Prune stale search sessions that were never closed.
                    if len(self.pending_searches) > 500:
                        self.pending_searches.clear()
                    await self._cleanup_transient_files(now)
                    await asyncio.sleep(TRANSIENT_CLEANUP_INTERVAL_SECONDS)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Pending upload cleanup iteration failed")
                    await asyncio.sleep(30)
        except asyncio.CancelledError:
            return

    async def _cleanup_transient_files(self, now: float | None = None) -> None:
        """Remove stale download work without touching durable bot state.

        Download workers create one UUID directory per operation. These paths
        are disposable; cookies, Telegram sessions, SQLite metadata, and the
        Telegram bin-channel archive are deliberately outside this janitor's
        allowlist.
        """
        current_time = now if now is not None else time.time()
        cutoff = current_time - max(
            TRANSIENT_MIN_AGE_SECONDS,
            self.settings.file_url_ttl * 2,
        )
        removed = 0
        transient_roots = (
            self.settings.work_dir,
            self.settings.work_dir / "mirror",
            self.settings.work_dir / "leech",
            self.settings.work_dir / "restricted",
            self.settings.work_dir / "restricted-archive",
            self.settings.work_dir / "voice-chat",
        )
        for root in transient_roots:
            if not root.exists() or not root.is_dir() or root.is_symlink():
                continue
            try:
                children = list(root.iterdir())
            except OSError:
                continue
            for child in children:
                if root == self.settings.work_dir:
                    # Only the downloader's UUID-named root directories are
                    # disposable. Never sweep sessions, cookies, or databases.
                    if not child.is_dir() or not TRANSIENT_ROOT_NAME_RE.fullmatch(
                        child.name
                    ):
                        continue
                try:
                    if child.is_symlink():
                        age_time = child.lstat().st_mtime
                    elif child.is_dir():
                        # A directory's own mtime usually reflects creation,
                        # not ongoing writes to a large media file. Inspect
                        # descendants so active downloads/playback remain
                        # protected while their files are still changing.
                        age_time = child.stat().st_mtime
                        for descendant in child.rglob("*"):
                            try:
                                age_time = max(age_time, descendant.stat().st_mtime)
                            except OSError:
                                continue
                    else:
                        age_time = child.stat().st_mtime
                    if age_time > cutoff:
                        continue
                    if child.is_dir() and not child.is_symlink():
                        shutil.rmtree(child, ignore_errors=True)
                    else:
                        child.unlink(missing_ok=True)
                    removed += 1
                except OSError:
                    logger.debug("Could not remove stale transient path %s", child)
        if removed:
            logger.info("Auto-cleaned %s stale transient download path(s)", removed)

    async def shutdown(self) -> None:
        if self._pending_cleanup_task:
            self._pending_cleanup_task.cancel()
            await asyncio.gather(
                self._pending_cleanup_task,
                return_exceptions=True,
            )
            self._pending_cleanup_task = None
        self._ready = False
        # Stop producers before closing the services used by their callbacks.
        # Otherwise a cancelled download can race a closed cache or file store.
        if self.queue:
            await self.queue.stop()
            self.queue = None
        await self.restricted_session.close()
        await self.voice_chat.close()
        if self.file_link_server:
            await self.file_link_server.close()
            self.file_link_server = None
        if self.file_links:
            await self.file_links.close()
            self.file_links = None
        if self.file_store:
            await self.file_store.close()
            self.file_store = None
        if self.cache:
            await self.cache.close()
            self.cache = None
        await self.health_server.close()
        self.health_server.set_request_handler(None)

    async def run(self) -> None:
        try:
            await self.setup()
            self.app = await self._start_bot_client()
            self._file_links_unavailable = False
            self.file_links = FileLinkStore(
                self.settings.file_url_base,
                self.settings.file_url_ttl,
            )
            await self.file_links.start()
            self.file_link_server = FileLinkServer(
                self.file_links,
                self.app,
                self.settings.file_url_host,
                self.settings.file_url_port,
                max_concurrent=self.settings.file_stream_concurrency,
            )
            if self.settings.file_url_port == self.settings.health_port:
                self.health_server.set_request_handler(
                    self.file_link_server.handle_request
                )
                logger.info(
                    "Telegram-native file streamer sharing health port %s via %s",
                    self.settings.health_port,
                    self.settings.file_url_base,
                )
            else:
                try:
                    await self.file_link_server.start()
                except OSError as exc:
                    logger.error(
                        "Telegram file streamer unavailable on %s:%s: %s",
                        self.settings.file_url_host,
                        self.settings.file_url_port,
                        exc,
                    )
                    self.file_link_server = None
                    await self.file_links.close()
                    self.file_links = None
                    self._file_links_unavailable = True
                else:
                    self.health_server.set_request_handler(
                        None
                    )
                    logger.info(
                        "Telegram-native file streamer listening on %s:%s via %s",
                        self.settings.file_url_host,
                        self.settings.file_url_port,
                        self.settings.file_url_base,
                    )
            try:
                await asyncio.wait_for(
                    self.app.set_bot_commands(
                        [
                            BotCommand("start", "Open the welcome menu"),
                            BotCommand("help", "Show commands and examples"),
                            BotCommand("search", "Find music by artist or song"),
                            BotCommand("ytdl", "Download a video, audio, or playlist"),
                            BotCommand("audio", "Download audio from a URL"),
                            BotCommand("queue", "Show your download queue"),
                            BotCommand("cancel", "Cancel a download"),
                            BotCommand("cookies", "Connect browser cookies securely"),
                            BotCommand("cookie_status", "Check stored cookie status"),
                            BotCommand("deletecookies", "Delete stored cookies"),
                            BotCommand("vplay", "Play audio or video in voice chat"),
                            BotCommand("vqueue", "Show the voice-chat queue"),
                            BotCommand("vcpanel", "Open interactive voice controls"),
                            BotCommand("vpause", "Pause voice-chat playback"),
                            BotCommand("vresume", "Resume voice-chat playback"),
                            BotCommand("vskip", "Skip the current voice track"),
                            BotCommand("vstop", "Stop voice-chat playback"),
                            BotCommand("vclear", "Clear the voice-chat queue"),
                            BotCommand("vseek", "Seek within the current track"),
                            BotCommand("vvolume", "Set voice-chat volume"),
                            BotCommand("vloop", "Toggle voice-chat looping"),
                            BotCommand("vcstatus", "Show voice-chat status"),
                            BotCommand("vcsetup", "Show voice-chat setup help"),
                            BotCommand("filestream", "Create a temporary file link"),
                            BotCommand("save", "Save accessible Telegram media"),
                            BotCommand("savecheck", "Check restricted-media access"),
                            BotCommand("mirror", "Mirror a supported media URL"),
                            BotCommand("leech", "Leech a supported media URL"),
                            BotCommand("store", "Save replied media permanently"),
                            BotCommand("myfiles", "List your saved files"),
                            BotCommand("store_search", "Search your saved files"),
                            BotCommand("store_stats", "Show your storage statistics"),
                        ]
                    ),
                    timeout=15,
                )
            except Exception as exc:
                logger.warning("Could not register Telegram command menu: %s", exc)
            if await self.restricted_session.start():
                identity = await self.restricted_session.identity()
                logger.info(
                    "Restricted-content user session loaded for user id=%s",
                    getattr(identity, "id", "unknown"),
                )
            try:
                await self.voice_chat.start()
            except Exception as exc:
                logger.warning(
                    "Voice chat assistant is unavailable; continuing without VC: %s",
                    self.voice_chat.user_error(exc),
                )
                self.voice_chat.enabled = False
            self._ready = True
            logger.info("ytdlbot is running")
            await asyncio.Event().wait()
        finally:
            # Keep Telegram connected while queue callbacks and optional
            # archive/file-link cleanup complete.
            app = self.app
            await self.shutdown()
            self.app = None
            if app and app.is_connected:
                await app.stop()


def main() -> None:
    print(STARTUP_BANNER, flush=True)
    settings = Settings.from_env()
    bot = Bot(settings)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("ytdlbot stopped")


if __name__ == "__main__":
    main()