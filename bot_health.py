from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any, Awaitable


HealthSnapshot = Callable[[], dict[str, Any]]
RequestHandler = Callable[
    [str, str, dict[str, str], asyncio.StreamWriter],
    Awaitable[bool],
]


class HealthServer:
    """Tiny dependency-free HTTP probe server for any hosting platform.

    The bot has no web framework requirement, but hosting platforms still need
    a reliable liveness/readiness contract. Keeping this server separate from
    the Telegram-native file streamer also means health checks never consume a
    media stream slot.
    """

    def __init__(
        self,
        host: str,
        port: int,
        snapshot: HealthSnapshot,
        request_handler: RequestHandler | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.snapshot = snapshot
        self.request_handler = request_handler
        self.server: asyncio.AbstractServer | None = None

    def set_request_handler(self, request_handler: RequestHandler | None) -> None:
        self.request_handler = request_handler

    async def start(self) -> None:
        if self.server:
            return
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            limit=16 * 1024,
        )

    async def close(self) -> None:
        if not self.server:
            return
        self.server.close()
        await self.server.wait_closed()
        self.server = None

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request = await asyncio.wait_for(reader.readline(), timeout=5)
            parts = request.decode("latin-1").strip().split(None, 2)
            if len(parts) != 3:
                await self._respond(writer, 400, {"status": "bad_request"}, head_only=True)
                return
            method, target, version = parts
            if method not in {"GET", "HEAD"} or version not in {"HTTP/1.0", "HTTP/1.1"}:
                await self._respond(writer, 405, {"status": "method_not_allowed"}, head_only=True)
                return
            headers: dict[str, str] = {}
            header_bytes = len(request)
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5)
                header_bytes += len(line)
                if header_bytes > 16 * 1024:
                    raise ValueError("request headers are too large")
                if line in {b"", b"\r\n", b"\n"}:
                    break
                if b":" not in line:
                    raise ValueError("malformed request header")
                key, value = line.decode("latin-1").split(":", 1)
                headers[key.strip().lower()] = value.strip()
            if (
                self.request_handler
                and target.split("?", 1)[0].startswith("/file/")
                and await self.request_handler(method, target, headers, writer)
            ):
                return
            if target.split("?", 1)[0] == "/healthz":
                status_code = 200
                payload = {"status": "ok"}
            elif target.split("?", 1)[0] == "/readyz":
                payload = self.snapshot()
                status_code = 200 if payload.get("ready") else 503
            else:
                status_code = 404
                payload = {"status": "not_found"}
            await self._respond(writer, status_code, payload, head_only=method == "HEAD")
        except (
            asyncio.IncompleteReadError,
            asyncio.TimeoutError,
            ValueError,
            UnicodeError,
            ConnectionError,
            BrokenPipeError,
        ):
            try:
                await self._respond(writer, 400, {"status": "bad_request"}, head_only=True)
            except (ConnectionError, BrokenPipeError):
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, BrokenPipeError):
                pass

    @staticmethod
    async def _respond(
        writer: asyncio.StreamWriter,
        status_code: int,
        payload: dict[str, Any],
        *,
        head_only: bool = False,
    ) -> None:
        reason = {
            200: "OK",
            400: "Bad Request",
            404: "Not Found",
            405: "Method Not Allowed",
            503: "Service Unavailable",
        }.get(status_code, "Unknown")
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers = (
            f"HTTP/1.1 {status_code} {reason}\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Cache-Control: no-store\r\n"
            "Connection: close\r\n\r\n"
        ).encode("latin-1")
        writer.write(headers)
        if not head_only:
            writer.write(body)
        await writer.drain()