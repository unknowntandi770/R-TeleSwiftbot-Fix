from __future__ import annotations

import asyncio
from email.message import Message as EmailMessage
import ipaddress
import os
import socket
import shutil
import subprocess
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener
from urllib.parse import unquote, urljoin, urlparse
from uuid import uuid4

import yt_dlp

from bot_cookies import CookieStore
from bot_quality import normalize_quality
from bot_urls import (
    google_drive_confirmation_url,
    google_drive_file_id,
    is_playlist_url,
    is_supported_url,
    is_torrent_url,
    normalize_google_drive_url,
    normalize_url,
)

@dataclass(frozen=True)
class ProgressSnapshot:
    percent: float = 0.0
    status: str = "downloading"
    title: str = ""
    speed: float | None = None
    eta: int | None = None
    downloaded: int = 0
    total: int | None = None
    playlist_index: int | None = None
    playlist_count: int | None = None


@dataclass
class DownloadItem:
    path: Path
    title: str
    url: str
    duration: int | None
    audio_only: bool
    thumbnail: Path | None = None
    quality: str = "auto"


@dataclass(frozen=True)
class SearchResult:
    title: str
    url: str
    uploader: str = ""
    duration: int | None = None
    thumbnail: str | None = None


ProgressCallback = Callable[[ProgressSnapshot], Awaitable[None]]
ItemCallback = Callable[[DownloadItem, int, int], Awaitable[bool]]
CancelCheck = Callable[[], bool]


@dataclass
class DownloadResult:
    items: list[DownloadItem]
    url: str
    playlist_title: str | None = None
    streamed_count: int = 0
    streamed_failures: int = 0
    item_count: int | None = None


class DownloadError(Exception):
    pass


class DownloadCancelled(DownloadError):
    """Raised when the user cancels a queued or active download."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class YTDLPDownloader:
    def __init__(
        self,
        work_dir: Path,
        cookie_store: CookieStore,
        pot_provider_url: str | None = None,
        max_download_bytes: int | None = None,
    ) -> None:
        self.work_dir = work_dir
        self.cookie_store = cookie_store
        self.pot_provider_url = pot_provider_url
        self.max_download_bytes = (
            max(1, int(max_download_bytes)) if max_download_bytes else None
        )
        from bot_torrent import TorrentDownloader

        self.torrent = TorrentDownloader(work_dir, self.max_download_bytes)

    @staticmethod
    def _assert_safe_public_url(url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            raise DownloadError("Only public http(s) file URLs can be mirrored.")
        if parsed.username or parsed.password:
            raise DownloadError("Links containing embedded credentials are not allowed.")
        try:
            addresses = {
                ipaddress.ip_address(info[4][0])
                for info in socket.getaddrinfo(
                    parsed.hostname,
                    parsed.port or (443 if parsed.scheme.lower() == "https" else 80),
                    type=socket.SOCK_STREAM,
                )
            }
        except (OSError, ValueError):
            raise DownloadError("The file host could not be resolved.") from None
        if any(
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_reserved
            or address.is_multicast
            or address.is_unspecified
            for address in addresses
        ):
            raise DownloadError("Private or local network links cannot be mirrored.")

    @staticmethod
    def _filename_from_headers(headers: Any, url: str) -> str:
        disposition = headers.get("Content-Disposition", "")
        name = ""
        if disposition:
            try:
                parsed = EmailMessage()
                parsed["Content-Disposition"] = disposition
                name = parsed.get_filename() or ""
            except (TypeError, ValueError):
                name = ""
        if name.lower().startswith("utf-8''"):
            name = unquote(name[7:])
        if not name:
            name = Path(urlparse(url).path).name
        name = Path(name).name.strip() or "download"
        return name[:180]

    async def download_direct(
        self,
        url: str,
        user_id: int,
        progress: ProgressCallback | None = None,
        cancel_check: CancelCheck | None = None,
    ) -> DownloadResult:
        del user_id
        original_url = url
        url = normalize_google_drive_url(url)
        self._assert_safe_public_url(url)
        output_dir = self.work_dir / "mirror" / uuid4().hex
        output_dir.mkdir(parents=True, exist_ok=True)
        loop = asyncio.get_running_loop()
        opener = build_opener(_NoRedirect)

        async def update(snapshot: ProgressSnapshot) -> None:
            if progress:
                await progress(snapshot)

        def fetch() -> DownloadItem:
            current = url
            response = None
            target: Path | None = None
            part: Path | None = None
            try:
                for _ in range(5):
                    self._assert_safe_public_url(current)
                    request = Request(
                        current,
                        headers={"User-Agent": "ytdlbot-mirror/1.0"},
                    )
                    try:
                        response = opener.open(request, timeout=30)
                    except HTTPError as exc:
                        if exc.code not in {301, 302, 303, 307, 308}:
                            raise
                        location = exc.headers.get("Location")
                        if not location:
                            raise DownloadError("The file URL returned an invalid redirect.")
                        current = urljoin(current, location)
                        self._assert_safe_public_url(current)
                        continue
                    final = response.geturl()
                    if final != current:
                        response.close()
                        response = None
                        current = urljoin(current, final)
                        continue
                    if google_drive_file_id(current):
                        content_type = response.headers.get("Content-Type", "").lower()
                        if "text/html" in content_type:
                            body = response.read(64 * 1024)
                            confirmation = google_drive_confirmation_url(current, body)
                            response.close()
                            response = None
                            if confirmation:
                                current = confirmation
                                continue
                            raise DownloadError(
                                "Google Drive did not provide a downloadable public file. "
                                "Check that the link is shared publicly."
                            )
                    break
                if response is None:
                    raise DownloadError("The file URL redirected too many times.")
                content_type = response.headers.get("Content-Type", "").split(";", 1)[
                    0
                ].strip().lower()
                size_header = response.headers.get("Content-Length")
                total = int(size_header) if size_header and size_header.isdigit() else None
                if self.max_download_bytes and total and total > self.max_download_bytes:
                    raise DownloadError(
                        "The requested file is larger than the configured download limit."
                    )
                name = self._filename_from_headers(response.headers, current)
                suffix = Path(name).suffix.lower()
                if content_type == "text/html" and suffix not in {
                    ".html",
                    ".htm",
                }:
                    raise DownloadError(
                        "This link returned a web page instead of a downloadable file. "
                        "Use the smart extractor route for supported media pages."
                    )
                target = output_dir / name
                part = target.with_name(f".{target.name}.part")
                downloaded = 0
                with part.open("wb") as handle:
                    while True:
                        if cancel_check and cancel_check():
                            raise DownloadCancelled("Download cancelled.")
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        handle.write(chunk)
                        downloaded += len(chunk)
                        if (
                            self.max_download_bytes
                            and downloaded > self.max_download_bytes
                        ):
                            raise DownloadError(
                                "The requested file is larger than the configured download limit."
                            )
                        percent = downloaded * 100 / total if total else 0.0
                        asyncio.run_coroutine_threadsafe(
                            update(
                                ProgressSnapshot(
                                    percent=min(100.0, percent),
                                    status="downloading",
                                    title=name,
                                    downloaded=downloaded,
                                    total=total,
                                )
                            ),
                            loop,
                        )
                part.replace(target)
                return DownloadItem(
                    path=target,
                    title=Path(name).stem[:180] or "Download",
                    url=original_url,
                    duration=None,
                    audio_only=(
                        content_type.startswith("audio/")
                        or suffix in {
                            ".aac",
                            ".flac",
                            ".m4a",
                            ".mp3",
                            ".oga",
                            ".ogg",
                            ".opus",
                            ".wav",
                            ".weba",
                        }
                    ),
                )
            except DownloadError:
                if target:
                    target.unlink(missing_ok=True)
                if part:
                    part.unlink(missing_ok=True)
                raise
            except Exception as exc:
                if target:
                    target.unlink(missing_ok=True)
                if part:
                    part.unlink(missing_ok=True)
                raise DownloadError(
                    "The direct file could not be downloaded. Check the URL and try again."
                ) from exc
            finally:
                if response is not None:
                    response.close()

        try:
            item = await asyncio.to_thread(fetch)
            if progress:
                await progress(
                    ProgressSnapshot(
                        percent=100.0,
                        status="complete",
                        title=item.title,
                    )
                )
            return DownloadResult(items=[item], url=original_url, item_count=1)
        except asyncio.CancelledError:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise
        except Exception:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise

    @staticmethod
    def _probe_source_kind(url: str) -> str:
        """Classify a public URL without consuming its media body."""
        current = normalize_google_drive_url(url)
        opener = build_opener(_NoRedirect)
        for _ in range(5):
            YTDLPDownloader._assert_safe_public_url(current)
            response = None
            try:
                request = Request(
                    current,
                    method="HEAD",
                    headers={"User-Agent": "ytdlbot-smart-mirror/1.0"},
                )
                try:
                    response = opener.open(request, timeout=12)
                except HTTPError as exc:
                    if exc.code not in {301, 302, 303, 307, 308}:
                        if exc.code not in {405, 501}:
                            return "extractor"
                        response = opener.open(
                            Request(
                                current,
                                headers={
                                    "Range": "bytes=0-0",
                                    "User-Agent": "ytdlbot-smart-mirror/1.0",
                                },
                            ),
                            timeout=12,
                        )
                    else:
                        location = exc.headers.get("Location")
                        if not location:
                            return "extractor"
                        current = urljoin(current, location)
                        continue
                content_type = response.headers.get("Content-Type", "").split(
                    ";", 1
                )[0].strip().lower()
                if content_type in {
                    "application/dash+xml",
                    "application/mpegurl",
                    "application/vnd.apple.mpegurl",
                    "application/x-mpegurl",
                    "audio/mpegurl",
                    "audio/x-mpegurl",
                }:
                    return "extractor"
                if google_drive_file_id(current) and content_type == "text/html":
                    response.close()
                    response = None
                    body_response = opener.open(
                        Request(
                            current,
                            headers={
                                "Range": "bytes=0-65535",
                                "User-Agent": "ytdlbot-smart-mirror/1.0",
                            },
                        ),
                        timeout=12,
                    )
                    body = body_response.read(64 * 1024)
                    confirmation = google_drive_confirmation_url(current, body)
                    body_response.close()
                    if confirmation:
                        current = confirmation
                        continue
                filename = YTDLPDownloader._filename_from_headers(
                    response.headers,
                    current,
                )
                suffix = Path(filename).suffix.lower()
                if content_type == "text/html" or content_type.startswith(
                    "application/xhtml"
                ):
                    return "extractor"
                if content_type or suffix:
                    return "direct"
                return "unknown"
            except (OSError, ValueError, URLError):
                return "unknown"
            finally:
                if response is not None:
                    response.close()
        return "extractor"

    async def download_smart(
        self,
        url: str,
        user_id: int,
        progress: ProgressCallback | None = None,
        cancel_check: CancelCheck | None = None,
    ) -> DownloadResult:
        """Choose direct transfer for files and yt-dlp for supported media pages."""
        if not is_supported_url(url):
            raise DownloadError("That does not look like a valid http(s) URL.")
        if progress:
            await progress(
                ProgressSnapshot(
                    status="analyzing",
                    title="Inspecting the link and choosing the best downloader…",
                )
            )
        kind = await asyncio.to_thread(self._probe_source_kind, url)
        if kind == "extractor":
            return await self.download(
                url,
                user_id,
                audio_only=False,
                progress=progress,
                cancel_check=cancel_check,
            )
        try:
            return await self.download_direct(
                url,
                user_id,
                progress=progress,
                cancel_check=cancel_check,
            )
        except DownloadError:
            if kind != "unknown":
                raise
            return await self.download(
                url,
                user_id,
                audio_only=False,
                progress=progress,
                cancel_check=cancel_check,
            )

    async def download_torrent(
        self,
        magnet: str,
        user_id: int,
        progress: ProgressCallback | None = None,
        cancel_check: CancelCheck | None = None,
        select_files: str | None = None,
    ) -> DownloadResult:
        del user_id
        torrent_path: Path | None = None
        if is_torrent_url(magnet):
            self._assert_safe_public_url(magnet)
            fetched = await self.download_direct(
                magnet,
                0,
                progress=progress,
                cancel_check=cancel_check,
            )
            if not fetched.items:
                raise DownloadError("The .torrent URL did not return a torrent file.")
            torrent_path = fetched.items[0].path
            source = str(torrent_path)
        else:
            source = magnet

        async def torrent_progress(data: dict[str, Any]) -> None:
            if progress:
                await progress(
                    ProgressSnapshot(
                        percent=float(data.get("percent") or 0),
                        status=str(data.get("status") or "downloading"),
                        title=str(data.get("title") or ""),
                    )
                )

        try:
            return await self.torrent.download(
                source,
                select_files=select_files,
                progress=torrent_progress,
                cancel_check=cancel_check,
            )
        finally:
            if torrent_path:
                torrent_path.unlink(missing_ok=True)
                shutil.rmtree(torrent_path.parent, ignore_errors=True)

    @staticmethod
    def _resolve_js_runtime() -> dict[str, dict[str, str]] | None:
        """Locate a Bun or Deno binary for yt-dlp's YouTube JS challenge solver.

        Honors the BUN_PATH/DENO_PATH overrides documented in
        sample_config.env and checked by the bot.py preflight report,
        instead of only ever looking on the default PATH. Without this,
        pointing BUN_PATH/DENO_PATH at a non-standard install location
        makes the preflight check report the tool as found while actual
        downloads keep failing with "no JavaScript runtime" errors.
        """
        bun_override = os.getenv("BUN_PATH", "").strip()
        if bun := shutil.which(bun_override or "bun"):
            return {"bun": {"path": bun}}
        deno_override = os.getenv("DENO_PATH", "").strip()
        if deno := shutil.which(deno_override or "deno"):
            return {"deno": {"path": deno}}
        return None

    def _options(
        self,
        output_dir: Path,
        audio_only: bool,
        cookie_path: Path | None,
        progress_hook: Callable[[dict[str, Any]], None] | None,
        quality: str = "auto",
        postprocessor_hook: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        quality = normalize_quality(quality, audio_only)
        if audio_only:
            format_selector = "bestaudio/best"
        elif quality == "auto":
            format_selector = "bestvideo*+bestaudio/best"
        else:
            format_selector = (
                f"bestvideo*[height<={quality}]+bestaudio/"
                f"best[height<={quality}]"
            )
        options: dict[str, Any] = {
            "noplaylist": False,
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
            "format": format_selector,
            "outtmpl": str(output_dir / "%(title).180B [%(id)s].%(ext)s"),
            "restrictfilenames": True,
            "windowsfilenames": True,
            "merge_output_format": "mp4",
            "postprocessors": [],
            # Performance: download fragments in parallel where supported
            "concurrent_fragment_downloads": 4,
            # Reliability: retry fragments and full downloads on transient errors
            "retries": 5,
            "fragment_retries": 10,
            "socket_timeout": 30,
            # Throughput: larger HTTP read chunks reduce syscall overhead
            "http_chunk_size": 10 * 1024 * 1024,
            # Skip unavailable playlist items instead of aborting the whole list
            "ignoreerrors": "only_download",
        }
        if self.max_download_bytes:
            options["max_filesize"] = self.max_download_bytes
        if audio_only:
            options["postprocessors"] = [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": quality if quality != "auto" else "192",
                }
            ]
        if cookie_path:
            options["cookiefile"] = str(cookie_path)
        # Let yt-dlp fetch a matching EJS challenge-solver bundle from GitHub
        # if the locally pinned yt-dlp-ejs package ever falls behind what the
        # installed yt-dlp release expects. This is a resilience fallback and
        # does not depend on whether a PO-token provider is configured.
        options["remote_components"] = ["ejs:github"]
        if self.pot_provider_url:
            options["extractor_args"] = {
                "youtubepot-bgutilhttp": {
                    "base_url": [self.pot_provider_url],
                }
            }
        # yt-dlp 2026.07 requires Deno >= 2.3, while the managed runtime in
        # this environment can be older. Prefer the supported Bun runtime for
        # YouTube's JavaScript challenge solver.
        if js_runtimes := self._resolve_js_runtime():
            options["js_runtimes"] = js_runtimes
        if progress_hook:
            options["progress_hooks"] = [progress_hook]
        if postprocessor_hook:
            options["postprocessor_hooks"] = [postprocessor_hook]
        return options

    def _search_options(self, cookie_path: Path | None) -> dict[str, Any]:
        options: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
            "skip_download": True,
            "extract_flat": "in_playlist",
            "noplaylist": False,
            "playlistend": 8,
            "socket_timeout": 20,
            "retries": 3,
        }
        if cookie_path:
            options["cookiefile"] = str(cookie_path)
        options["remote_components"] = ["ejs:github"]
        if self.pot_provider_url:
            options["extractor_args"] = {
                "youtubepot-bgutilhttp": {
                    "base_url": [self.pot_provider_url],
                }
            }
        if js_runtimes := self._resolve_js_runtime():
            options["js_runtimes"] = js_runtimes
        return options

    async def search(
        self, query: str, user_id: int, limit: int = 8
    ) -> list[SearchResult]:
        query = " ".join(query.split()).strip()
        if not query:
            return []
        limit = max(1, min(8, int(limit)))
        cookie_path = self.cookie_store.path_for(user_id)

        def run() -> list[SearchResult]:
            options = self._search_options(cookie_path)
            try:
                with yt_dlp.YoutubeDL(options) as client:
                    info = client.extract_info(
                        f"ytsearch{limit}:{query}",
                        download=False,
                    )
            except yt_dlp.utils.DownloadError as exc:
                raise DownloadError(self._friendly_error(str(exc))) from exc

            results: list[SearchResult] = []
            for entry in (info.get("entries") or [])[:limit]:
                if not entry:
                    continue
                entry_id = str(entry.get("id") or "").strip()
                url = str(
                    entry.get("webpage_url")
                    or entry.get("original_url")
                    or (
                        f"https://www.youtube.com/watch?v={entry_id}"
                        if entry_id
                        else ""
                    )
                )
                if not url:
                    continue
                duration = entry.get("duration")
                try:
                    duration_value = int(duration) if duration is not None else None
                except (TypeError, ValueError):
                    duration_value = None
                results.append(
                    SearchResult(
                        title=str(entry.get("title") or "Untitled"),
                        url=url,
                        uploader=str(
                            entry.get("uploader")
                            or entry.get("channel")
                            or ""
                        ),
                        duration=duration_value,
                        thumbnail=str(entry.get("thumbnail") or "") or None,
                    )
                )
            return results

        try:
            return await asyncio.to_thread(run)
        finally:
            self.cookie_store.cleanup_materialized(cookie_path)

    @staticmethod
    def _thumbnail_url(info: dict[str, Any]) -> str | None:
        thumbnail = info.get("thumbnail")
        if isinstance(thumbnail, str) and thumbnail:
            return thumbnail
        thumbnails = info.get("thumbnails")
        if isinstance(thumbnails, list):
            for candidate in reversed(thumbnails):
                if isinstance(candidate, dict) and candidate.get("url"):
                    return str(candidate["url"])
        return None

    @staticmethod
    def _download_thumbnail(url: str | None, target: Path) -> Path | None:
        """Download a Telegram-compatible thumbnail without failing the media job."""
        if not url:
            return None
        source: Path | None = None
        response = None
        try:
            current = url
            opener = build_opener(_NoRedirect)
            for _ in range(5):
                YTDLPDownloader._assert_safe_public_url(current)
                request = Request(
                    current,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (X11; Linux x86_64) "
                            "AppleWebKit/537.36 Chrome/131 Safari/537.36"
                        )
                    },
                )
                try:
                    response = opener.open(request, timeout=15)
                    break
                except HTTPError as exc:
                    if exc.code not in {301, 302, 303, 307, 308}:
                        return None
                    location = exc.headers.get("Location")
                    if not location:
                        return None
                    response = None
                    current = urljoin(current, location)
            if response is None:
                return None
            target.parent.mkdir(parents=True, exist_ok=True)
            data = response.read(5_000_000)
            if not data:
                return None
            with NamedTemporaryFile(
                mode="wb",
                prefix="thumbnail-source-",
                suffix=".bin",
                dir=target.parent,
                delete=False,
            ) as handle:
                handle.write(data)
                source = Path(handle.name)
            converted = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-loglevel",
                    "error",
                    "-i",
                    str(source),
                    "-vf",
                    "scale=320:320:force_original_aspect_ratio=decrease",
                    "-frames:v",
                    "1",
                    "-q:v",
                    "8",
                    str(target),
                ],
                capture_output=True,
                timeout=20,
                check=False,
            )
            if converted.returncode != 0 or not target.exists():
                return None
            # Telegram's thumbnail limit is 200 KB. Retry with lower quality
            # before dropping the thumbnail entirely.
            if target.stat().st_size > 200_000:
                converted = subprocess.run(
                    [
                        "ffmpeg",
                        "-y",
                        "-loglevel",
                        "error",
                        "-i",
                        str(source),
                        "-vf",
                        "scale=240:240:force_original_aspect_ratio=decrease",
                        "-frames:v",
                        "1",
                        "-q:v",
                        "15",
                        str(target),
                    ],
                    capture_output=True,
                    timeout=20,
                    check=False,
                )
            if converted.returncode != 0 or not target.exists():
                return None
            if target.stat().st_size > 200_000:
                target.unlink(missing_ok=True)
                return None
            target.chmod(0o600)
            return target
        except Exception:
            target.unlink(missing_ok=True)
            return None
        finally:
            if response is not None:
                response.close()
            if source:
                source.unlink(missing_ok=True)

    async def download(
        self,
        url: str,
        user_id: int,
        audio_only: bool = False,
        progress: ProgressCallback | None = None,
        cancel_check: CancelCheck | None = None,
        quality: str = "auto",
        item_callback: ItemCallback | None = None,
    ) -> DownloadResult:
        if not is_supported_url(url):
            raise DownloadError("That does not look like a valid http(s) URL.")
        normalized = normalize_url(url)
        playlist = is_playlist_url(normalized)
        output_dir = self.work_dir / uuid4().hex
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            cookie_path = self.cookie_store.path_for(user_id)
        except RuntimeError as exc:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise DownloadError(
                "Your stored browser cookies cannot be read. "
                "Upload a fresh cookie file with /cookies and try again."
            ) from exc
        loop = asyncio.get_running_loop()

        async def update(snapshot: ProgressSnapshot) -> None:
            if progress:
                await progress(snapshot)

        def hook(data: dict[str, Any]) -> None:
            if cancel_check and cancel_check():
                raise DownloadCancelled("Download cancelled.")
            status = str(data.get("status") or "")
            if status not in {"downloading", "finished"}:
                return
            total = data.get("total_bytes") or data.get("total_bytes_estimate")
            downloaded = data.get("downloaded_bytes", 0)
            percent = 100.0 if status == "finished" else 0.0
            if total:
                percent = min(100.0, downloaded * 100 / total)
            info = data.get("info_dict") or {}
            playlist_index = info.get("playlist_index") or data.get("playlist_index")
            playlist_count = info.get("playlist_count") or data.get("playlist_count")
            snapshot = ProgressSnapshot(
                percent=percent,
                status=status,
                title=str(info.get("title") or ""),
                speed=data.get("speed"),
                eta=data.get("eta"),
                downloaded=int(downloaded or 0),
                total=int(total) if total else None,
                playlist_index=int(playlist_index) if playlist_index else None,
                playlist_count=int(playlist_count) if playlist_count else None,
            )
            asyncio.run_coroutine_threadsafe(update(snapshot), loop)

        streamed_ids: set[str] = set()
        streamed_count = 0
        streamed_failures = 0
        playlist_total = 0

        def deliver_finished_item(data: dict[str, Any]) -> None:
            nonlocal streamed_count, streamed_failures
            if not playlist or data.get("status") != "finished" or not item_callback:
                return
            info = data.get("info_dict") or {}
            entry_id = str(info.get("id") or "")
            path_value = info.get("filepath") or info.get("_filename")
            if not path_value:
                matches = sorted(
                    path
                    for path in output_dir.iterdir()
                    if path.is_file()
                    and entry_id in path.name
                    and not path.name.endswith((".part", ".ytdl"))
                )
                path_value = str(matches[0]) if matches else None
            if not entry_id or not path_value:
                return
            path = Path(str(path_value))
            if not path.exists() or path.suffix in {".part", ".ytdl"}:
                return
            if entry_id in streamed_ids:
                return
            # Use the order in which items are actually presented. Playlist
            # indices are sparse when yt-dlp skips failed entries.
            index = streamed_count + streamed_failures + 1
            count = int(
                info.get("playlist_count")
                or info.get("n_entries")
                or playlist_total
                or index
            )
            thumbnail = self._download_thumbnail(
                self._thumbnail_url(info),
                output_dir / f"{entry_id}.thumbnail.jpg",
            )
            item = DownloadItem(
                path=path,
                title=str(info.get("title") or path.stem),
                url=str(
                    info.get("webpage_url")
                    or f"https://www.youtube.com/watch?v={entry_id}"
                ),
                duration=info.get("duration"),
                audio_only=audio_only,
                thumbnail=thumbnail,
                quality=normalize_quality(quality, audio_only),
            )
            future = asyncio.run_coroutine_threadsafe(
                item_callback(item, index, count),
                loop,
            )
            delivered = bool(future.result())
            streamed_ids.add(entry_id)
            if delivered:
                streamed_count += 1
            else:
                streamed_failures += 1

        def run() -> tuple[dict[str, Any], Path]:
            if cancel_check and cancel_check():
                raise DownloadCancelled("Download cancelled.")
            options = self._options(
                output_dir,
                audio_only,
                cookie_path,
                hook,
                quality,
                deliver_finished_item,
            )
            options["noplaylist"] = not playlist
            if playlist:
                options["ignoreerrors"] = True
            diagnostics: list[str] = []

            class CaptureLogger:
                def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
                    return

                def info(self, message: str, *args: Any, **kwargs: Any) -> None:
                    return

                def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
                    diagnostics.append(str(message))

                def error(self, message: str, *args: Any, **kwargs: Any) -> None:
                    diagnostics.append(str(message))

            options["logger"] = CaptureLogger()
            try:
                with yt_dlp.YoutubeDL(options) as client:
                    info = client.extract_info(normalized, download=True)
                    if cancel_check and cancel_check():
                        raise DownloadCancelled("Download cancelled.")
                    prepared = client.prepare_filename(info) if not playlist else ""
            except DownloadCancelled:
                raise
            except yt_dlp.utils.DownloadError as exc:
                detail = "\n".join([str(exc), *diagnostics])
                raise DownloadError(self._friendly_error(detail)) from exc
            candidates = sorted(
                path
                for path in output_dir.iterdir()
                if path.is_file() and not path.name.endswith((".part", ".ytdl"))
            )
            if not candidates and streamed_ids:
                return info, Path()
            if not candidates:
                expected = Path(prepared)
                if expected.exists():
                    candidates = [expected]
            if not candidates:
                diagnostic_error = self._friendly_error("\n".join(diagnostics))
                if diagnostic_error != self._friendly_error(""):
                    raise DownloadError(diagnostic_error)
                if playlist:
                    discovered = len(
                        [entry for entry in (info.get("entries") or []) if entry]
                    )
                    if discovered:
                        guidance = (
                            "Upload a fresh browser cookie export with /cookies and retry."
                            if not cookie_path
                            else "Export a fresh browser cookie file with /cookies and retry."
                        )
                        raise DownloadError(
                            f"YouTube found {discovered} playlist items, but blocked every "
                            f"media download. {guidance}"
                        )
                raise DownloadError("yt-dlp completed without producing a media file.")
            return info, candidates[0]

        try:
            info, _ = await asyncio.to_thread(run)
            candidates = sorted(
                path
                for path in output_dir.iterdir()
                if path.is_file() and not path.name.endswith((".part", ".ytdl"))
            )
            entries = info.get("entries") if playlist else [info]
            entries = [entry for entry in (entries or []) if entry]
            playlist_count = (
                int(info.get("playlist_count") or info.get("n_entries") or len(entries))
                if playlist
                else None
            )
            items: list[DownloadItem] = []
            for entry in entries:
                entry_id = str(entry.get("id") or "")
                if entry_id in streamed_ids:
                    continue
                matching = [path for path in candidates if f"[{entry_id}]" in path.name]
                if matching:
                    path = matching[0]
                elif not entry_id and len(candidates) > len(items):
                    path = candidates[len(items)]
                else:
                    # yt-dlp leaves failed playlist entries as None when
                    # ignoreerrors is enabled; do not pair another item's file
                    # with the failed metadata.
                    continue
                thumbnail = await asyncio.to_thread(
                    self._download_thumbnail,
                    self._thumbnail_url(entry),
                    output_dir / f"{entry_id or len(items)}.thumbnail.jpg",
                )
                item = DownloadItem(
                        path=path,
                        title=str(entry.get("title") or path.stem),
                        url=str(entry.get("webpage_url") or f"https://www.youtube.com/watch?v={entry_id}"),
                        duration=entry.get("duration"),
                        audio_only=audio_only,
                        thumbnail=thumbnail,
                        quality=normalize_quality(quality, audio_only),
                )
                if item_callback and playlist:
                    item_number = streamed_count + streamed_failures + 1
                    delivered = await item_callback(
                        item,
                        item_number,
                        playlist_count or len(entries),
                    )
                    streamed_ids.add(entry_id)
                    if delivered:
                        streamed_count += 1
                    else:
                        streamed_failures += 1
                else:
                    items.append(item)
            if not items:
                if streamed_ids:
                    await update(
                        ProgressSnapshot(
                            percent=100.0,
                            status="complete",
                            title=str(info.get("title") or ""),
                            playlist_count=playlist_count,
                        )
                    )
                    shutil.rmtree(output_dir, ignore_errors=True)
                    return DownloadResult(
                        url=normalized,
                        items=[],
                        playlist_title=str(info.get("title")) if playlist else None,
                        streamed_count=streamed_count,
                        streamed_failures=streamed_failures,
                        item_count=playlist_count,
                    )
                if playlist and playlist_count:
                    guidance = (
                        "Export a fresh browser cookie file with /cookies and retry."
                        if cookie_path and self.cookie_store.has(user_id)
                        else "Upload a browser cookie export with /cookies and retry."
                    )
                    raise DownloadError(
                        f"YouTube recognized {playlist_count} playlist item(s), but "
                        f"blocked every media download. {guidance}"
                    )
                raise DownloadError(
                    "No playlist items could be downloaded. "
                    "YouTube may have blocked every playlist item or the stored "
                    "cookies may need refreshing."
                )
            await update(
                ProgressSnapshot(
                    percent=100.0,
                    status="complete",
                    title=str(info.get("title") or ""),
                    playlist_count=playlist_count,
                )
            )
            return DownloadResult(
                url=normalized,
                items=items,
                playlist_title=str(info.get("title")) if playlist else None,
                streamed_count=streamed_count,
                streamed_failures=streamed_failures,
                item_count=playlist_count or len(items),
            )
        except DownloadError:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise
        except asyncio.CancelledError:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise
        except Exception as exc:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise DownloadError(self._friendly_error(str(exc))) from exc
        finally:
            self.cookie_store.cleanup_materialized(cookie_path)

    @staticmethod
    def _friendly_error(message: str) -> str:
        lowered = message.lower()
        if "cookies are no longer valid" in lowered or "rotated in the browser" in lowered:
            return (
                "Your stored YouTube cookies are no longer valid. "
                "Export a fresh cookie file from the browser where YouTube is "
                "currently signed in, send it with /cookies, then retry."
            )
        if "not a bot" in lowered or "automated query" in lowered or "automated traffic" in lowered:
            return "YouTube blocked this request as automated traffic. Upload browser cookies with /cookies and try again."
        if "confirm your age" in lowered or "age-restricted" in lowered:
            return "This video is age-restricted. Upload your browser cookies with /cookies and try again."
        if "sign in to confirm" in lowered or "sign in to confirm your age" in lowered:
            return "YouTube requires sign-in for this video. Upload your browser cookies with /cookies and try again."
        if "members-only" in lowered or "members only" in lowered:
            return "This is a members-only video. Upload cookies from a subscribed account with /cookies."
        if "video unavailable" in lowered or "this video is not available" in lowered:
            return "This video is unavailable in the current region or has been removed."
        if "copyright" in lowered and "blocked" in lowered:
            return "This video is blocked due to a copyright claim and cannot be downloaded."
        if "unsupported url" in lowered or "no suitable" in lowered:
            return "This URL is not supported. Try a direct YouTube, SoundCloud, Twitter/X, or other yt-dlp supported link."
        if "private video" in lowered or "private playlist" in lowered:
            return "This video/playlist is private. Upload cookies from an account that can view it."
        if "this live event will begin" in lowered or "live event" in lowered:
            return "This is an upcoming live stream. You can download it once the broadcast has ended."
        if "is live" in lowered or "live stream" in lowered:
            return "Live streams cannot be downloaded while they are broadcasting. Try again after it ends."
        if "no video formats found" in lowered or "requested format is not available" in lowered:
            return "No downloadable format was found for this URL. The video may be restricted or require a paid account."
        if "too many requests" in lowered or "rate limit" in lowered or "ratelimit" in lowered:
            return "YouTube is rate-limiting this server. Wait a few minutes, then try again. Uploading cookies may help."
        if "connection" in lowered and ("reset" in lowered or "refused" in lowered or "timed out" in lowered):
            return "The connection to the media server was interrupted. Please try again."
        if "javascript runtime" in lowered or "js runtime" in lowered:
            return (
                "YouTube extraction needs a JavaScript runtime. Bun or Deno "
                "must be available; restart the bot and try again."
            )
        if "ffmpeg" in lowered and ("not found" in lowered or "not installed" in lowered):
            return "ffmpeg is not installed on this server. Contact the bot owner to install it."
        if "disk" in lowered and ("full" in lowered or "no space" in lowered):
            return "The server ran out of disk space. Contact the bot owner."
        # Truncate very long yt-dlp error messages that contain internal stack traces
        clean = message.strip()
        if len(clean) > 400:
            clean = clean[:400].rsplit(" ", 1)[0] + "…"
        return f"yt-dlp could not download this URL: {clean}" if clean else "yt-dlp could not download that URL. Check the link or try again later."