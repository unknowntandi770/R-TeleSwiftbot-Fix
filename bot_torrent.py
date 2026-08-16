from __future__ import annotations

import asyncio
import re
import shutil
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

from bot_downloader import DownloadCancelled, DownloadError, DownloadItem, DownloadResult
from bot_urls import is_magnet_url, is_torrent_url


ProgressCallback = Callable[[dict[str, Any]], Awaitable[None]]
CancelCheck = Callable[[], bool]

_PERCENT_RE = re.compile(r"(\d{1,3})%")
_SELECT_RE = re.compile(r"\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*")


class TorrentDownloader:
    """Bounded aria2c adapter for magnet downloads.

    aria2 owns the BitTorrent protocol and writes only into a per-job
    directory. The bot remains responsible for Telegram delivery and cleanup.
    """

    def __init__(
        self,
        work_dir: Path,
        max_download_bytes: int | None = None,
    ) -> None:
        self.work_dir = work_dir
        self.max_download_bytes = (
            max(1, int(max_download_bytes)) if max_download_bytes else None
        )

    async def download(
        self,
        magnet: str,
        *,
        select_files: str | None = None,
        progress: ProgressCallback | None = None,
        cancel_check: CancelCheck | None = None,
    ) -> DownloadResult:
        local_torrent = Path(magnet).expanduser()
        if (
            not is_magnet_url(magnet)
            and not is_torrent_url(magnet)
            and not (
                local_torrent.is_file()
                and local_torrent.suffix.lower() == ".torrent"
            )
        ):
            raise DownloadError(
                "Leech expects a magnet link or a public .torrent URL."
            )
        if not shutil.which("aria2c"):
            raise DownloadError(
                "Torrent support is unavailable because aria2c is not installed."
            )
        if select_files and not _SELECT_RE.fullmatch(select_files):
            raise DownloadError("File selection must look like 1,3-5.")

        output_dir = self.work_dir / "leech" / uuid4().hex
        output_dir.mkdir(parents=True, exist_ok=True)
        command = [
            "aria2c",
            "--dir",
            str(output_dir),
            "--seed-time=0",
            # Give slow-seeded torrents up to 5 min to find peers
            "--bt-stop-timeout=300",
            "--file-allocation=none",
            "--follow-torrent=true",
            "--summary-interval=1",
            "--console-log-level=notice",
            "--enable-color=false",
            "--max-tries=5",
            "--retry-wait=3",
            "--connect-timeout=15",
            "--timeout=60",
            "--bt-tracker-connect-timeout=15",
            "--bt-tracker-interval=30",
            # Parallel connections per server for HTTP/FTP sources
            "--max-connection-per-server=8",
            "--split=8",
            "--min-split-size=5M",
            # Increase concurrent download streams within the torrent
            "--bt-max-open-files=32",
            "--max-concurrent-downloads=5",
        ]
        if select_files:
            command.append(f"--select-file={select_files}")
        command.append(magnet)
        process: asyncio.subprocess.Process | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            while True:
                if cancel_check and cancel_check():
                    await self._stop_process(process)
                    raise DownloadCancelled("Download cancelled.")
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), 1)
                except asyncio.TimeoutError:
                    if process.returncode is not None:
                        break
                    continue
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if self.max_download_bytes:
                    current_size = sum(
                        path.stat().st_size
                        for path in output_dir.rglob("*")
                        if path.is_file()
                    )
                    if current_size > self.max_download_bytes:
                        await self._stop_process(process)
                        raise DownloadError(
                            "The torrent exceeded the configured download limit."
                        )
                if progress:
                    match = _PERCENT_RE.search(text)
                    await progress(
                        {
                            "percent": min(100.0, float(match.group(1)))
                            if match
                            else 0.0,
                            "status": "downloading",
                            "title": text[-160:],
                        }
                    )
            return_code = await process.wait()
            if return_code != 0:
                raise DownloadError(
                    "aria2 could not fetch this torrent. Check the magnet link, "
                    "tracker availability, and available disk space."
                )
            files = sorted(
                path
                for path in output_dir.rglob("*")
                if path.is_file()
                and not path.name.endswith((".aria2", ".torrent"))
                and not path.name.startswith(".")
            )
            if not files:
                raise DownloadError(
                    "The torrent completed without producing a downloadable file."
                )
            if self.max_download_bytes:
                total_size = sum(path.stat().st_size for path in files)
                if total_size > self.max_download_bytes:
                    raise DownloadError(
                        "The torrent is larger than the configured download limit."
                    )
            items = [
                DownloadItem(
                    path=path,
                    title=path.name,
                    url=magnet,
                    duration=None,
                    audio_only=False,
                )
                for path in files
            ]
            if progress:
                await progress(
                    {
                        "percent": 100.0,
                        "status": "complete",
                        "title": items[0].title,
                    }
                )
            return DownloadResult(items=items, url=magnet, item_count=len(items))
        except asyncio.CancelledError:
            if process and process.returncode is None:
                await self._stop_process(process)
            shutil.rmtree(output_dir, ignore_errors=True)
            raise
        except Exception:
            if process and process.returncode is None:
                await self._stop_process(process)
            shutil.rmtree(output_dir, ignore_errors=True)
            raise

    @staticmethod
    async def _stop_process(process: asyncio.subprocess.Process) -> None:
        """Stop aria2 promptly even if it is stuck shutting down."""
        if process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()