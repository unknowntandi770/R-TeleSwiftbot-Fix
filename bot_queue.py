from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from uuid import uuid4

from bot_downloader import (
    CancelCheck,
    DownloadCancelled,
    DownloadError,
    DownloadResult,
    ItemCallback,
    ProgressCallback,
    YTDLPDownloader,
)

logger = logging.getLogger(__name__)

JobCallback = Callable[["DownloadJob", DownloadResult | None, Exception | None], Awaitable[None]]
RestrictedFetcher = Callable[
    [str, int, ProgressCallback | None, CancelCheck | None],
    Awaitable[DownloadResult],
]


class UserQueueBusy(RuntimeError):
    """Raised when a user already has a queued or active download."""


@dataclass
class DownloadJob:
    url: str
    user_id: int
    chat_id: int
    audio_only: bool
    callback: JobCallback
    quality: str = "auto"
    progress: ProgressCallback | None = None
    item_callback: ItemCallback | None = None
    mode: str = "ytdlp"
    torrent_select_files: str | None = None
    restricted_fetcher: RestrictedFetcher | None = None
    status_message_id: int | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    # Short IDs have a realistic collision chance once a bot has processed
    # enough jobs. Keep the full UUID while still displaying it compactly in
    # Telegram messages.
    id: str = field(default_factory=lambda: uuid4().hex)


# Maximum seconds a single download job is allowed to run before the bot
# forcibly cancels it via the job's cancel_event. 4 hours covers very large
# files; adjust down for public bots with strict resource budgets.
DEFAULT_JOB_TIMEOUT_SECONDS = 4 * 60 * 60


class DownloadQueue:
    def __init__(
        self,
        downloader: YTDLPDownloader,
        workers: int,
        max_size: int,
        job_timeout_seconds: int = DEFAULT_JOB_TIMEOUT_SECONDS,
    ) -> None:
        self.downloader = downloader
        self.jobs: asyncio.Queue[DownloadJob] = asyncio.Queue(maxsize=max_size)
        self.workers = workers
        self.job_timeout_seconds = max(60, int(job_timeout_seconds))
        self._tasks: list[asyncio.Task[None]] = []
        self._active: dict[str, DownloadJob] = {}
        self._known: dict[str, DownloadJob] = {}
        self._state_lock = asyncio.Lock()
        self._started = False
        self._stopping = False

    async def start(self) -> None:
        async with self._state_lock:
            if self._tasks:
                return
            self._stopping = False
            self._started = True
            self._tasks = [
                asyncio.create_task(self._worker(i), name=f"download-worker-{i}")
                for i in range(self.workers)
            ]

    async def stop(self) -> None:
        async with self._state_lock:
            self._stopping = True
            for job in list(self._known.values()):
                job.cancel_event.set()
            tasks = list(self._tasks)
            self._tasks.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        drained: list[DownloadJob] = []
        while not self.jobs.empty():
            try:
                drained.append(self.jobs.get_nowait())
            except asyncio.QueueEmpty:
                break
            else:
                self.jobs.task_done()
        for job in drained:
            # Jobs that never reached a worker still need their callback so the
            # Telegram status message can be finalized instead of hanging.
            try:
                await job.callback(
                    job,
                    None,
                    DownloadCancelled("Download cancelled during shutdown."),
                )
            except Exception:
                logger.exception(
                    "Shutdown cancellation callback crashed for job %s", job.id
                )
        self._active.clear()
        self._known.clear()
        self._started = False

    async def submit(
        self,
        job: DownloadJob,
        *,
        reject_duplicate_user: bool = False,
    ) -> int:
        async with self._state_lock:
            if not self._started or self._stopping:
                raise RuntimeError("The download queue is not accepting new jobs.")
            if reject_duplicate_user and any(
                known.user_id == job.user_id for known in self._known.values()
            ):
                raise UserQueueBusy(
                    "You already have a download queued. Wait for it to finish or cancel it first."
                )
            # Reserve the job before enqueueing it. This prevents two concurrent
            # Telegram updates from both passing the per-user duplicate check.
            self._known[job.id] = job
            try:
                self.jobs.put_nowait(job)
            except Exception:
                self._known.pop(job.id, None)
                raise
            return self.jobs.qsize()

    def position(self) -> int:
        return self.jobs.qsize()

    def active_for(self, user_id: int) -> bool:
        return any(job.user_id == user_id for job in self._active.values())

    def jobs_for(self, user_id: int) -> list[DownloadJob]:
        return [
            job
            for job in self._known.values()
            if job.user_id == user_id
        ]

    def active_jobs(self) -> list[DownloadJob]:
        return list(self._active.values())

    def cancel(self, user_id: int, job_id: str | None = None) -> int:
        matched = 0
        for job in self._known.values():
            if job.user_id != user_id:
                continue
            if job_id and job.id.lower() != job_id.lower():
                continue
            if not job.cancel_event.is_set():
                job.cancel_event.set()
                matched += 1
        return matched

    async def _timeout_watcher(self, job: DownloadJob) -> None:
        """Soft-cancel a job that runs past the configured timeout.

        Sets cancel_event so yt-dlp's progress hook picks it up on the next
        chunk boundary without forcibly killing the download thread.
        """
        try:
            await asyncio.sleep(self.job_timeout_seconds)
            if not job.cancel_event.is_set():
                logger.warning(
                    "Job %s timed out after %ss; requesting cancellation",
                    job.id,
                    self.job_timeout_seconds,
                )
                job.cancel_event.set()
        except asyncio.CancelledError:
            return

    async def _worker(self, worker_id: int) -> None:
        del worker_id
        while True:
            job: DownloadJob | None = None
            timeout_task: asyncio.Task[None] | None = None
            try:
                job = await self.jobs.get()
                self._active[job.id] = job
                if job.cancel_event.is_set():
                    await job.callback(job, None, DownloadCancelled("Download cancelled."))
                    continue
                # Start a timeout watcher that soft-cancels the job if it runs
                # too long. This prevents stuck workers from blocking the queue.
                timeout_task = asyncio.create_task(
                    self._timeout_watcher(job), name=f"timeout-{job.id}"
                )
                if job.mode == "direct":
                    result = await self.downloader.download_direct(
                        job.url,
                        job.user_id,
                        progress=job.progress,
                        cancel_check=job.cancel_event.is_set,
                    )
                elif job.mode == "smart":
                    result = await self.downloader.download_smart(
                        job.url,
                        job.user_id,
                        progress=job.progress,
                        cancel_check=job.cancel_event.is_set,
                    )
                elif job.mode == "torrent":
                    result = await self.downloader.download_torrent(
                        job.url,
                        job.user_id,
                        progress=job.progress,
                        cancel_check=job.cancel_event.is_set,
                        select_files=job.torrent_select_files,
                    )
                elif job.mode == "restricted":
                    if not job.restricted_fetcher:
                        raise DownloadError(
                            "Restricted Telegram retrieval is not configured."
                        )
                    result = await job.restricted_fetcher(
                        job.url,
                        job.user_id,
                        job.progress,
                        job.cancel_event.is_set,
                    )
                else:
                    result = await self.downloader.download(
                        job.url,
                        job.user_id,
                        audio_only=job.audio_only,
                        progress=job.progress,
                        cancel_check=job.cancel_event.is_set,
                        quality=job.quality,
                        item_callback=job.item_callback,
                    )
            except asyncio.CancelledError as exc:
                # A worker is cancelled during shutdown. Preserve normal
                # callback semantics for the active job before re-raising so
                # callers do not keep a permanent "downloading" message.
                if job is not None:
                    try:
                        await job.callback(
                            job,
                            None,
                            DownloadCancelled("Download cancelled during shutdown."),
                        )
                    except Exception:
                        logger.exception(
                            "Shutdown cancellation callback crashed for job %s",
                            job.id,
                        )
                raise
            except Exception as exc:
                if job is not None:
                    try:
                        await job.callback(job, None, exc)
                    except Exception:
                        logger.exception(
                            "Download failure callback crashed for job %s", job.id
                        )
            else:
                if job is not None:
                    try:
                        await job.callback(job, result, None)
                    except Exception:
                        logger.exception(
                            "Download success callback crashed for job %s", job.id
                        )
            finally:
                # Always cancel the timeout watcher once the job is done.
                if timeout_task is not None and not timeout_task.done():
                    timeout_task.cancel()
                    await asyncio.gather(timeout_task, return_exceptions=True)
                if job is not None:
                    self._active.pop(job.id, None)
                    self._known.pop(job.id, None)
                    self.jobs.task_done()