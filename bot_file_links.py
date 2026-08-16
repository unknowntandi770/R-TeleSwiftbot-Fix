from __future__ import annotations

import asyncio
import math
import mimetypes
import secrets
import time
from dataclasses import dataclass
from email.utils import formatdate
from pathlib import Path
import re
from urllib.parse import quote, urlsplit

from pyrogram import Client


CHUNK_SIZE = 1024 * 1024
MAX_HEADER_LINES = 64
MAX_HEADER_BYTES = 16 * 1024
WRITE_TIMEOUT_SECONDS = 30
MAX_FILE_LINK_TTL_SECONDS = 3 * 60 * 60
REQUEST_LINE_RE = re.compile(r"^(GET|HEAD) (/[^ ]*) HTTP/1\.[01]$")
HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")


@dataclass
class SharedFile:
    token: str
    file_id: str
    name: str
    content_type: str
    size: int
    owner_id: int
    expires_at: float


class FileLinkStore:
    """Short-lived Telegram file references used by the public stream server."""

    def __init__(self, base_url: str, ttl: int = 86_400) -> None:
        self.base_url = base_url.rstrip("/")
        # Public stream/download links must never outlive the product's
        # three-hour retention promise, even if an old environment still
        # contains FILE_URL_TTL=86400.
        self.ttl = max(60, min(int(ttl), MAX_FILE_LINK_TTL_SECONDS))
        self._files: dict[str, SharedFile] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def close(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            await asyncio.gather(self._cleanup_task, return_exceptions=True)
            self._cleanup_task = None
        async with self._lock:
            self._files.clear()

    async def add_media(
        self,
        file_id: str,
        name: str,
        size: int,
        owner_id: int,
        content_type: str | None = None,
    ) -> SharedFile:
        token = secrets.token_urlsafe(32)
        safe_name = Path(name or "download").name or "download"
        shared = SharedFile(
            token=token,
            file_id=file_id,
            name=safe_name,
            content_type=(
                content_type
                or mimetypes.guess_type(safe_name)[0]
                or "application/octet-stream"
            ),
            size=max(0, int(size)),
            owner_id=owner_id,
            expires_at=time.time() + self.ttl,
        )
        async with self._lock:
            self._files[token] = shared
        return shared

    async def get(self, token: str) -> SharedFile | None:
        async with self._lock:
            shared = self._files.get(token)
            if not shared:
                return None
            if shared.expires_at <= time.time():
                self._files.pop(token, None)
                return None
            return shared

    async def remove(self, token: str) -> bool:
        async with self._lock:
            return self._files.pop(token, None) is not None

    def urls(self, shared: SharedFile) -> tuple[str, str]:
        stream = f"{self.base_url}/file/{quote(shared.token)}/stream"
        download = f"{self.base_url}/file/{quote(shared.token)}/download"
        return stream, download

    async def _cleanup_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(60)
                now = time.time()
                async with self._lock:
                    expired = [
                        token
                        for token, shared in self._files.items()
                        if shared.expires_at <= now
                    ]
                    for token in expired:
                        self._files.pop(token, None)
        except asyncio.CancelledError:
            return


class FileLinkServer:
    """Range-aware HTTP server that streams directly from Telegram."""

    def __init__(
        self,
        store: FileLinkStore,
        telegram: Client,
        host: str,
        port: int,
        max_concurrent: int = 8,
    ) -> None:
        self.store = store
        self.telegram = telegram
        self.host = host
        self.port = port
        self.server: asyncio.AbstractServer | None = None
        self._stream_slots = asyncio.Semaphore(max(1, max_concurrent))

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            limit=16 * 1024,
        )

    async def close(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request = await asyncio.wait_for(reader.readline(), timeout=10)
            if len(request) > 4096:
                raise ValueError("request line is too large")
            request_line = request.decode("latin-1").rstrip("\r\n")
            match = REQUEST_LINE_RE.fullmatch(request_line)
            if not match:
                await self._respond(
                    writer, "405 Method Not Allowed", {"Allow": "GET, HEAD"}
                )
                return
            method = match.group(1)
            target = match.group(2)
            headers: dict[str, str] = {}
            header_lines = 0
            header_bytes = len(request)
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=10)
                header_lines += 1
                header_bytes += len(line)
                if header_lines > MAX_HEADER_LINES or header_bytes > MAX_HEADER_BYTES:
                    raise ValueError("request headers are too large")
                if line in {b"\r\n", b"\n", b""}:
                    break
                if b":" not in line:
                    raise ValueError("malformed request header")
                key, value = line.decode("latin-1").split(":", 1)
                key = key.strip()
                if not HEADER_NAME_RE.fullmatch(key):
                    raise ValueError("malformed request header name")
                headers[key.lower()] = value.strip()
            if not await self.handle_request(method, target, headers, writer):
                await self._respond(writer, "404 Not Found")
        except (asyncio.TimeoutError, UnicodeError, ConnectionError, ValueError):
            try:
                await self._respond(writer, "400 Bad Request")
            except ConnectionError:
                pass
        except Exception:
            try:
                await self._respond(writer, "502 Bad Gateway")
            except ConnectionError:
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def handle_request(
        self,
        method: str,
        target: str,
        headers: dict[str, str],
        writer: asyncio.StreamWriter,
    ) -> bool:
        """Handle a parsed request, returning whether it belongs to this server."""
        if method not in {"GET", "HEAD"}:
            await self._respond(writer, "405 Method Not Allowed", {"Allow": "GET, HEAD"})
            return True
        parsed_target = urlsplit(target)
        segments = [segment for segment in parsed_target.path.split("/") if segment]
        if len(segments) != 3 or segments[0] != "file":
            return False
        mode = segments[2]
        if mode not in {"stream", "download"}:
            await self._respond(writer, "404 Not Found")
            return True
        shared = await self.store.get(segments[1])
        if not shared:
            await self._respond(writer, "404 Not Found")
            return True
        async with self._stream_slots:
            await self._send_file(
                writer,
                shared,
                mode,
                headers.get("range"),
                head_only=method == "HEAD",
            )
        return True

    async def _send_file(
        self,
        writer: asyncio.StreamWriter,
        shared: SharedFile,
        mode: str,
        range_header: str | None,
        head_only: bool = False,
    ) -> None:
        if shared.size <= 0:
            await self._respond(writer, "416 Range Not Satisfiable")
            return
        selected_range = self._range(range_header, shared.size)
        if selected_range is None:
            await self._respond(
                writer,
                "416 Range Not Satisfiable",
                {"Content-Range": f"bytes */{shared.size}"},
            )
            return
        start, end, status = selected_range
        length = end - start + 1
        headers = {
            "Content-Type": shared.content_type,
            "Content-Length": str(length),
            "Content-Disposition": self._content_disposition(shared, mode),
            "Accept-Ranges": "bytes",
            # The token is the authorization boundary. Do not let an
            # intermediary cache a successful response beyond token expiry.
            "Cache-Control": "private, no-store, max-age=0",
            "Last-Modified": formatdate(shared.expires_at, usegmt=True),
            "Expires": formatdate(shared.expires_at, usegmt=True),
        }
        if status == "206 Partial Content":
            headers["Content-Range"] = f"bytes {start}-{end}/{shared.size}"
        await self._respond(writer, status, headers)
        if head_only:
            return

        offset = start // CHUNK_SIZE
        trim_start = start % CHUNK_SIZE
        chunks = math.ceil((trim_start + length) / CHUNK_SIZE)
        sent = 0
        chunk_index = 0
        stream = self.telegram.stream_media(
            shared.file_id,
            limit=chunks,
            offset=offset,
        )
        try:
            async for chunk in stream:
                if chunk_index == 0 and trim_start:
                    chunk = chunk[trim_start:]
                remaining = length - sent
                if remaining <= 0:
                    break
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]
                if chunk:
                    writer.write(chunk)
                    # A client that stops reading must not retain a Telegram
                    # media stream and one server task indefinitely.
                    await asyncio.wait_for(
                        writer.drain(),
                        timeout=WRITE_TIMEOUT_SECONDS,
                    )
                    sent += len(chunk)
                chunk_index += 1
        finally:
            close = getattr(stream, "aclose", None)
            if close:
                await close()

    @staticmethod
    def _content_disposition(shared: SharedFile, mode: str) -> str:
        disposition = "inline" if mode == "stream" else "attachment"
        ascii_name = "".join(
            character if 32 <= ord(character) < 127 and character not in {'"', "\\"} else "_"
            for character in shared.name
        )[:120] or "download"
        filename = quote(shared.name, safe="")
        return (
            f'{disposition}; filename="{ascii_name}"; '
            f"filename*=UTF-8''{filename}"
        )

    @staticmethod
    def _range(
        range_header: str | None, size: int
    ) -> tuple[int, int, str] | None:
        if not range_header:
            return 0, size - 1, "200 OK"
        if not range_header.startswith("bytes="):
            return None
        value = range_header[6:].split(",", 1)[0].strip()
        try:
            left, right = value.split("-", 1)
            if left:
                start = int(left)
                end = int(right) if right else size - 1
            else:
                suffix_length = int(right)
                if suffix_length <= 0:
                    raise ValueError
                start = max(0, size - suffix_length)
                end = size - 1
            if start < 0 or start >= size or end < start:
                raise ValueError
            return start, min(end, size - 1), "206 Partial Content"
        except (ValueError, TypeError):
            return None

    @staticmethod
    async def _respond(
        writer: asyncio.StreamWriter,
        status: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        response_headers = {"Connection": "close", "Content-Length": "0"}
        if headers:
            response_headers.update(headers)
        payload = f"HTTP/1.1 {status}\r\n".encode("latin-1")
        payload += b"".join(
            f"{key}: {value}\r\n".encode("latin-1")
            for key, value in response_headers.items()
        )
        payload += b"\r\n"
        writer.write(payload)
        await writer.drain()