from __future__ import annotations

import asyncio
from email.message import Message as EmailMessage
from html import unescape
import ipaddress
import logging
import re
import socket
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
from urllib.error import HTTPError, URLError

import yt_dlp
from pyrogram import Client
from pytgcalls import PyTgCalls
from pytgcalls.exceptions import (
    NoAudioSourceFound,
    NoVideoSourceFound,
    NotInCallError,
)
from pytgcalls.types import ChatUpdate, GroupCallConfig, MediaStream, StreamEnded

from bot_cookies import CookieStore
from bot_urls import (
    google_drive_confirmation_url,
    google_drive_file_id,
    normalize_google_drive_url,
)
from bot_quality import normalize_stream_quality

logger = logging.getLogger("ytdlbot.voice_chat")


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


@dataclass
class VoiceTrack:
    title: str
    source: str
    stream_url: str
    requester_id: int
    duration: int | None = None
    cleanup_path: Path | None = None
    video: bool = False
    audio_url: str | None = None
    video_url: str | None = None
    audio_headers: dict[str, str] = field(default_factory=dict)
    video_headers: dict[str, str] = field(default_factory=dict)
    stream_quality: str = "original"


@dataclass
class VoiceChatState:
    queue: list[VoiceTrack]
    current: VoiceTrack | None = None
    paused: bool = False
    loop: bool = False
    volume: int = 100
    position: int = 0
    started_at: float | None = None


class VoiceChatController:
    """Optional Telegram voice-chat playback using a user assistant session."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_path: Path,
        work_dir: Path,
        cookie_store: CookieStore | None = None,
        pot_provider_url: str | None = None,
        session_string: str | None = None,
        max_queue_size: int = 32,
    ) -> None:
        self.enabled = False
        self._api_id = api_id
        self._api_hash = api_hash
        self._session_path = session_path
        self._work_dir = work_dir
        self._session_string = (session_string or "").strip()
        self._cookie_store = cookie_store
        self._pot_provider_url = pot_provider_url
        self._max_queue_size = max(1, max_queue_size)
        self.assistant: Client | None = None
        self.calls: PyTgCalls | None = None
        self.assistant_is_bot: bool | None = None
        self.assistant_name: str | None = None
        self._states: dict[int, VoiceChatState] = {}
        self._play_tasks: dict[int, asyncio.Task[None]] = {}
        self._end_events: dict[int, asyncio.Event] = {}
        self._start_waiters: dict[int, asyncio.Future[None]] = {}
        self._control_events: dict[int, asyncio.Event] = {}
        self._skip_requests: set[int] = set()
        self._owned_calls: set[int] = set()
        self._chat_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._lock = asyncio.Lock()

    @property
    def setup_hint(self) -> str:
        return (
            "Voice chat streaming is not configured yet.\n\n"
            "Telegram requires a real user account for voice chats. "
            "Generate a session string with "
            "<code>python generate_vc_session.py</code>, save it as "
            "the private <code>VC_SESSION_STRING</code> secret, and restart "
            "the bot. A local phone-login session file remains supported as "
            "a fallback."
        )

    @property
    def session_file(self) -> Path:
        return self._session_path.with_suffix(".session")

    @property
    def ready(self) -> bool:
        return bool(self.calls and self.assistant and self.enabled)

    @property
    def authorized_client(self) -> Client | None:
        """Expose the voice user client only to voice-specific callers."""
        if (
            self.enabled
            and self.calls
            and self.assistant
            and self.assistant.is_connected
            and not self.assistant_is_bot
        ):
            return self.assistant
        return None

    async def start(self) -> None:
        if self.ready:
            return
        try:
            if self._session_string:
                self._validate_session_string(self._session_string)
                self.assistant = self._new_client()
                await self.assistant.start()
                await self._activate_assistant()
                return
            if not self.session_file.exists():
                logger.info("Voice chat streaming disabled: phone login has not completed")
                return
            probe = self._new_client()
            try:
                authorized = await probe.connect()
            except Exception as exc:
                logger.warning(
                    "Voice chat session could not be checked; run the phone login "
                    "script again: %s",
                    exc,
                )
                return
            finally:
                if probe.is_connected:
                    await probe.disconnect()
            if not authorized:
                logger.info(
                    "Voice chat streaming disabled: local session is not authorized; "
                    "run vc_phone_login.py"
                )
                return
            self.assistant = self._new_client()
            await self.assistant.start()
            await self._activate_assistant()
        except BaseException:
            await self._rollback_start()
            raise

    async def _rollback_start(self) -> None:
        assistant = self.assistant
        self.calls = None
        self.assistant = None
        self.enabled = False
        self.assistant_is_bot = None
        self.assistant_name = None
        if assistant and assistant.is_connected:
            try:
                await assistant.stop()
            except Exception:
                pass

    def _new_client(self) -> Client:
        if self._session_string:
            return Client(
                "ytdlbot-vc-assistant-string",
                api_id=self._api_id,
                api_hash=self._api_hash,
                session_string=self._session_string,
                in_memory=True,
                skip_updates=True,
            )
        return Client(
            self._session_path.name,
            api_id=self._api_id,
            api_hash=self._api_hash,
            workdir=str(self._session_path.parent),
            skip_updates=True,
        )

    @staticmethod
    def _validate_session_string(value: str) -> None:
        if len(value) < 50 or not re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", value):
            raise RuntimeError(
                "VC_SESSION_STRING is malformed. Generate a fresh Pyrogram "
                "user session string with generate_vc_session.py."
            )

    async def _activate_assistant(self) -> None:
        if not self.assistant:
            raise RuntimeError(self.setup_hint)
        me = await self.assistant.get_me()
        self.assistant_is_bot = bool(getattr(me, "is_bot", False))
        self.assistant_name = (
            getattr(me, "username", None)
            or getattr(me, "first_name", None)
            or str(getattr(me, "id", "unknown"))
        )
        if self.assistant_is_bot:
            await self.assistant.stop()
            self.assistant = None
            raise RuntimeError(
                "The saved login belongs to a Telegram bot account. "
                "Use a real user account; bot accounts cannot join voice chats."
            )
        self.calls = PyTgCalls(self.assistant)
        self.calls.add_handler(self.handle_update)
        await self.calls.start()
        self.enabled = True
        logger.info(
            "Voice chat streaming enabled with assistant user id=%s",
            getattr(me, "id", "unknown"),
        )

    async def close(self) -> None:
        for task in self._play_tasks.values():
            task.cancel()
        if self._play_tasks:
            await asyncio.gather(*self._play_tasks.values(), return_exceptions=True)
        self._play_tasks.clear()
        if self.calls:
            for chat_id in list(self._states):
                try:
                    await self._leave_call(
                        chat_id,
                        close=chat_id in self._owned_calls,
                    )
                except Exception:
                    pass
            self.calls = None
        if self.assistant and self.assistant.is_connected:
            await self.assistant.stop()
        self.assistant = None
        self.enabled = False
        self.assistant_is_bot = None
        self.assistant_name = None
        self._states.clear()
        self._end_events.clear()
        self._start_waiters.clear()
        self._control_events.clear()
        self._skip_requests.clear()
        self._owned_calls.clear()
        self._chat_locks.clear()

    async def add(
        self,
        chat_id: int,
        source: str,
        requester_id: int,
        *,
        video: bool = False,
        stream_quality: str = "original",
    ) -> tuple[VoiceTrack, int]:
        self._require_enabled()
        await self._recover_stale_state(chat_id)
        track = await self._resolve(
            source,
            requester_id,
            video=video,
            stream_quality=stream_quality,
        )
        position, start_now = await self._enqueue(
            chat_id,
            track,
            return_start=True,
        )
        if start_now:
            try:
                await asyncio.wait_for(self._start_waiters[chat_id], timeout=30)
            except asyncio.TimeoutError as exc:
                waiter = self._start_waiters.pop(chat_id, None)
                if waiter and not waiter.done():
                    waiter.cancel()
                raise RuntimeError(
                    "Telegram did not join the voice chat within 30 seconds. "
                    "Run /vcstatus and check the assistant membership and "
                    "video-chat permissions."
                ) from exc
        return track, position

    async def add_file(
        self,
        chat_id: int,
        path: Path,
        title: str,
        requester_id: int,
        *,
        video: bool = False,
        stream_quality: str = "original",
    ) -> tuple[VoiceTrack, int]:
        self._require_enabled()
        if not path.exists() or not path.is_file():
            raise ValueError("The replied Telegram file is no longer available.")
        track = VoiceTrack(
            title=title[:180],
            source=str(path),
            stream_url=str(path),
            requester_id=requester_id,
            cleanup_path=path,
            video=video,
            audio_url=str(path),
            video_url=str(path) if video else None,
            # Telegram files and direct local media already have a fixed
            # source resolution. A quality cap would require transcoding,
            # which is intentionally outside the live voice-chat path.
            stream_quality="original",
        )
        await self._recover_stale_state(chat_id)
        position, start_now = await self._enqueue(chat_id, track, return_start=True)
        if start_now:
            try:
                await asyncio.wait_for(self._start_waiters[chat_id], timeout=30)
            except asyncio.TimeoutError as exc:
                waiter = self._start_waiters.pop(chat_id, None)
                if waiter and not waiter.done():
                    waiter.cancel()
                raise RuntimeError(
                    "Telegram did not join the voice chat within 30 seconds. "
                    "Run /vcstatus and check the assistant membership and "
                    "video-chat permissions."
                ) from exc
        return track, position

    async def pause(self, chat_id: int) -> bool:
        self._require_enabled()
        if not self.calls:
            return False
        try:
            paused = await self.calls.pause(chat_id)
        except NotInCallError as exc:
            await self._recover_disconnected_call(chat_id)
            raise RuntimeError(
                "The assistant is no longer connected to the voice chat. "
                "Check the assistant membership and video-chat permissions, "
                "then use /vplay again."
            ) from exc
        if paused:
            async with self._lock:
                if chat_id in self._states:
                    self._states[chat_id].paused = True
        return paused

    async def resume(self, chat_id: int) -> bool:
        self._require_enabled()
        if not self.calls:
            return False
        try:
            resumed = await self.calls.resume(chat_id)
        except NotInCallError as exc:
            await self._recover_disconnected_call(chat_id)
            raise RuntimeError(
                "The assistant is no longer connected to the voice chat. "
                "Check the assistant membership and video-chat permissions, "
                "then use /vplay again."
            ) from exc
        if resumed:
            async with self._lock:
                if chat_id in self._states:
                    self._states[chat_id].paused = False
        return resumed

    async def skip(self, chat_id: int) -> bool:
        self._require_enabled()
        async with self._lock:
            state = self._states.get(chat_id)
            if not state or not state.current:
                return False
            self._skip_requests.add(chat_id)
        if self.calls:
            try:
                            await self._leave_call(chat_id)
            except Exception:
                pass
        event = self._end_events.get(chat_id)
        if event:
            event.set()
        return True

    async def clear_queue(self, chat_id: int) -> int:
        self._require_enabled()
        async with self._lock:
            state = self._states.get(chat_id)
            if not state:
                return 0
            queued = list(state.queue)
            state.queue.clear()
        for track in queued:
            self._cleanup_track(track)
        return len(queued)

    async def set_volume(self, chat_id: int, volume: int) -> bool:
        self._require_enabled()
        if not 1 <= volume <= 200:
            raise ValueError("Volume must be between 1 and 200.")
        state = self._states.get(chat_id)
        if not state or not state.current or not self.calls:
            return False
        try:
            await self.calls.change_volume_call(chat_id, volume)
        except NotInCallError as exc:
            await self._recover_disconnected_call(chat_id)
            raise RuntimeError(
                "The assistant is no longer connected to the voice chat. "
                "Check the assistant membership and video-chat permissions, "
                "then use /vplay again."
            ) from exc
        async with self._lock:
            state = self._states.get(chat_id)
            if state and state.current:
                state.volume = volume
        return True

    async def set_loop(self, chat_id: int, enabled: bool | None = None) -> bool:
        self._require_enabled()
        async with self._lock:
            state = self._states.get(chat_id)
            if not state or not state.current:
                return False
            state.loop = not state.loop if enabled is None else enabled
            return state.loop

    async def seek(self, chat_id: int, seconds: int) -> bool:
        self._require_enabled()
        if seconds < 0:
            raise ValueError("Seek position cannot be negative.")
        async with self._lock:
            state = self._states.get(chat_id)
            if not state or not state.current or not self.calls:
                return False
            track = state.current
        if track.duration:
            seconds = min(seconds, max(0, track.duration - 1))
        try:
            await self.calls.play(
                chat_id,
                self._video_stream(track, seconds)
                if track.video
                else self._audio_stream(track, seconds),
                config=GroupCallConfig(auto_start=True),
            )
            await self.calls.unmute(chat_id)
            await self._wait_for_native_connection(
                chat_id,
                attempts=4,
                delay=0.25,
            )
        except NotInCallError as exc:
            await self._recover_disconnected_call(chat_id)
            raise RuntimeError(
                "The assistant is no longer connected to the voice chat. "
                "Check the assistant membership and video-chat permissions, "
                "then use /vplay again."
            ) from exc
        async with self._lock:
            state = self._states.get(chat_id)
            if state and state.current is track:
                state.position = seconds
                state.started_at = time.monotonic()
        self._control_events.setdefault(chat_id, asyncio.Event()).set()
        return True

    @staticmethod
    def _audio_stream(track: VoiceTrack, seconds: int = 0) -> MediaStream:
        """Build a real audio-only stream for PyTgCalls.

        Passing a direct YouTube URL lets PyTgCalls auto-detect video and
        audio.  That is unreliable for expiring YouTube URLs: Telegram can
        show the assistant in the call while NTgCalls has no microphone
        source.  Force the audio path and ignore video for voice playback.
        """
        seek = f"-ss {seconds}" if seconds > 0 else None
        audio_path = unescape(track.audio_url or track.stream_url)
        return MediaStream(
            media_path=audio_path,
            audio_path=audio_path,
            audio_flags=MediaStream.Flags.REQUIRED,
            video_flags=MediaStream.Flags.IGNORE,
            headers=track.audio_headers,
            ffmpeg_parameters=seek,
        )

    @staticmethod
    def _video_stream(track: VoiceTrack, seconds: int = 0) -> MediaStream:
        if not track.video:
            raise ValueError("This track does not contain a video stream.")
        seek = f"-ss {seconds}" if seconds > 0 else None
        video_path = unescape(track.video_url or track.stream_url)
        audio_path = unescape(track.audio_url or track.stream_url)
        # A progressive file/URL already contains both tracks. Passing it a
        # second time as ``audio_path`` makes PyTgCalls start two independent
        # FFmpeg readers; with remote/signed media this can leave the call
        # connected while the camera source silently fails. Let the video
        # input provide both tracks whenever both URLs point to the same
        # source. Separate audio/video inputs remain supported for DASH-style
        # extractors.
        shared_media = video_path == audio_path
        # PyTgCalls exposes one header map for both ffmpeg inputs. Do not
        # merge headers from unrelated signed URLs: a Referer or cookie
        # belonging to one stream can invalidate the other stream. Common
        # headers remain useful (usually User-Agent and Origin).
        shared_headers = {
            key: value
            for key, value in track.audio_headers.items()
            if track.video_headers.get(key) == value
        }
        return MediaStream(
            media_path=video_path,
            audio_path=None if shared_media else audio_path,
            audio_flags=MediaStream.Flags.REQUIRED,
            video_flags=MediaStream.Flags.REQUIRED,
            headers=(
                track.video_headers or track.audio_headers
                if shared_media
                else shared_headers
            ),
            ffmpeg_parameters=seek,
        )

    def current_position(self, chat_id: int) -> int | None:
        state = self._states.get(chat_id)
        if not state or not state.current:
            return None
        if state.started_at is None:
            return state.position
        elapsed = max(0, int(time.monotonic() - state.started_at))
        if state.current.duration:
            return min(state.current.duration, state.position + elapsed)
        return state.position + elapsed

    async def stop(self, chat_id: int, *, close_call: bool = True) -> bool:
        """Stop playback and close the group's voice/video chat.

        Cancel is a full teardown action so it does not leave a silent or stale
        group call behind. Internal callers can preserve an existing call by
        passing ``close_call=False``.
        """
        self._require_enabled()
        state = self._states.pop(chat_id, None)
        self._skip_requests.add(chat_id)
        task = self._play_tasks.pop(chat_id, None)
        if task and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        waiter = self._start_waiters.pop(chat_id, None)
        if waiter and not waiter.done():
            waiter.cancel()
        self._end_events.pop(chat_id, None)
        self._control_events.pop(chat_id, None)
        self._skip_requests.discard(chat_id)
        if self.calls:
            try:
                await self._leave_call(
                    chat_id,
                    close=close_call or chat_id in self._owned_calls,
                )
            except Exception:
                pass
        self._owned_calls.discard(chat_id)
        if state:
            if state.current:
                self._cleanup_track(state.current)
            for track in state.queue:
                self._cleanup_track(track)
        return bool(state)

    def status(self, chat_id: int) -> VoiceChatState | None:
        return self._states.get(chat_id)

    async def _leave_call(self, chat_id: int, *, close: bool = False) -> None:
        """Leave playback and close only calls created by this controller."""
        if not self.calls:
            return
        try:
            await self.calls.leave_call(chat_id, close=close)
        except TypeError:
            # Keep compatibility with simple test doubles and older PyTgCalls.
            await self.calls.leave_call(chat_id)

    async def _call_exists(self, chat_id: int) -> bool:
        """Check Telegram state before PyTgCalls potentially creates a call."""
        if not self.calls:
            return True
        mtproto = getattr(self.calls, "_app", None)
        getter = getattr(mtproto, "get_input_call", None)
        if not callable(getter):
            return True
        try:
            return (await getter(chat_id)) is not None
        except Exception:
            # Do not claim ownership when Telegram state cannot be inspected.
            return True

    async def _enqueue(
        self,
        chat_id: int,
        track: VoiceTrack,
        *,
        return_start: bool = False,
    ) -> tuple[int, bool] | int:
        async with self._chat_locks[chat_id]:
            async with self._lock:
                state = self._states.setdefault(chat_id, VoiceChatState(queue=[]))
                if len(state.queue) >= self._max_queue_size:
                    raise ValueError(
                        f"Voice chat queue is full (maximum {self._max_queue_size} upcoming tracks)."
                    )
                start_now = state.current is None and not state.queue
                position = len(state.queue) + (1 if state.current else 0)
                state.queue.append(track)
                if chat_id not in self._play_tasks or self._play_tasks[chat_id].done():
                    if start_now:
                        self._start_waiters[chat_id] = (
                            asyncio.get_running_loop().create_future()
                        )
                    self._play_tasks[chat_id] = asyncio.create_task(
                        self._worker(chat_id)
                    )
        return (position, start_now) if return_start else position

    async def _recover_stale_state(self, chat_id: int) -> None:
        state = self._states.get(chat_id)
        if not state or not state.current or not self.calls:
            return
        active_task = self._play_tasks.get(chat_id)
        if active_task and not active_task.done():
            # A worker may have assigned the current track but still be inside
            # PyTgCalls.play() joining the existing voice chat. Checking time()
            # during that window would incorrectly clear a healthy startup.
            return
        try:
            await self.calls.time(chat_id)
            return
        except Exception:
            logger.info("Clearing stale voice-chat state for %s", chat_id)
        self._states.pop(chat_id, None)
        self._skip_requests.add(chat_id)
        task = self._play_tasks.pop(chat_id, None)
        if task and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        for track in [state.current, *state.queue]:
            if track:
                self._cleanup_track(track)

    async def _recover_disconnected_call(self, chat_id: int) -> None:
        """Drop local playback state after Telegram/PyTgCalls loses the call."""
        state = self._states.pop(chat_id, None)
        self._skip_requests.add(chat_id)
        task = self._play_tasks.get(chat_id)
        if task and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._play_tasks.pop(chat_id, None)
        if state:
            if state.current:
                self._cleanup_track(state.current)
            for track in state.queue:
                self._cleanup_track(track)
        event = self._end_events.get(chat_id)
        if event:
            event.set()
        control = self._control_events.get(chat_id)
        if control:
            control.set()

    async def _wait_for_native_connection(
        self,
        chat_id: int,
        *,
        attempts: int = 8,
        delay: float = 0.4,
    ) -> None:
        """Wait for NTgCalls to expose a connection after Telegram joins."""
        if not self.calls:
            raise RuntimeError(self.setup_hint)
        last_error: NotInCallError | None = None
        for attempt in range(max(1, attempts)):
            try:
                await self.calls.time(chat_id)
                return
            except NotInCallError as exc:
                last_error = exc
                if attempt + 1 < attempts:
                    await asyncio.sleep(delay)
        if last_error:
            raise last_error
        raise RuntimeError(
            "The assistant joined Telegram but did not establish an audio "
            "connection. Start a fresh voice chat and try /vplay again."
        )

    @staticmethod
    def _cleanup_track(track: VoiceTrack) -> None:
        if track.cleanup_path:
            track.cleanup_path.unlink(missing_ok=True)
            try:
                track.cleanup_path.parent.rmdir()
            except OSError:
                pass

    @staticmethod
    def _redact_error(text: str) -> str:
        return re.sub(r"https?://\S+", "[redacted-url]", " ".join(text.split()))

    @staticmethod
    def _can_refresh_media_source(track: VoiceTrack) -> bool:
        """Return whether yt-dlp can obtain a fresher URL for this track."""
        source = track.source.strip().lower()
        if not source.startswith(("http://", "https://")):
            # Search requests are resolved by yt-dlp too, so they can be
            # refreshed when their signed playback URL expires.
            return True
        try:
            host = (urlparse(source).hostname or "").rstrip(".")
        except ValueError:
            return False
        return host == "youtu.be" or host == "youtube.com" or host.endswith(
            ".youtube.com"
        )

    async def _refresh_media_source(self, track: VoiceTrack) -> VoiceTrack:
        refreshed = await self._resolve(
            track.source,
            track.requester_id,
            video=track.video,
            stream_quality=track.stream_quality,
        )
        logger.info(
            "Refreshed expired voice media source for requester %s (video=%s)",
            track.requester_id,
            track.video,
        )
        return refreshed

    async def chat_diagnostics(self, chat_id: int) -> dict[str, Any]:
        self._require_enabled()
        if not self.assistant:
            raise RuntimeError(self.setup_hint)
        me = await self.assistant.get_me()
        chat = await self.assistant.get_chat(chat_id)
        member = await self.assistant.get_chat_member(chat_id, me.id)
        member_status = getattr(member.status, "name", str(member.status)).lower()
        state = self._states.get(chat_id)
        native_connected = False
        if state and state.current and self.calls:
            try:
                await self.calls.time(chat_id)
                native_connected = True
            except NotInCallError:
                native_connected = False
        return {
            "assistant_id": me.id,
            "assistant_name": (
                getattr(me, "username", None)
                or getattr(me, "first_name", None)
                or str(me.id)
            ),
            "assistant_is_bot": bool(getattr(me, "is_bot", False)),
            "chat_title": getattr(chat, "title", None) or str(chat_id),
            "member_status": member_status,
            "connected": native_connected,
            "has_voice_controller": bool(self.calls),
        }

    async def _worker(self, chat_id: int) -> None:
        while True:
            state = self._states.get(chat_id)
            if not state:
                return
            should_leave = False
            async with self._lock:
                if not state.queue:
                    state.current = None
                    should_leave = True
                else:
                    state.current = state.queue.pop(0)
                    state.paused = False
                    state.position = 0
                    state.started_at = time.monotonic()
            if should_leave:
                if self.calls:
                    try:
                        await self._leave_call(
                            chat_id,
                            close=chat_id in self._owned_calls,
                        )
                    except Exception:
                        pass
                self._owned_calls.discard(chat_id)
                await self._cleanup_idle_chat(chat_id, state)
                return
            track = state.current
            try:
                if not self.calls:
                    return
                self._end_events[chat_id] = asyncio.Event()
                if not await self._call_exists(chat_id):
                    self._owned_calls.add(chat_id)
                connected = False
                for attempt in range(3):
                    try:
                        media_stream = (
                            self._video_stream(track)
                            if track.video
                            else self._audio_stream(track)
                        )
                        logger.info(
                            "Starting voice media for chat %s: video=%s "
                            "separate_inputs=%s (attempt %s)",
                            chat_id,
                            track.video,
                            bool(
                                track.video
                                and track.audio_url
                                and track.video_url
                                and track.audio_url != track.video_url
                            ),
                            attempt + 1,
                        )
                        await self.calls.play(
                            chat_id,
                            media_stream,
                            # PyTgCalls creates the group call when none exists,
                            # then joins it with the assistant user.
                            config=GroupCallConfig(auto_start=True),
                        )
                        # PyTgCalls joins muted by default on many Telegram
                        # clients. Explicitly unmute after play() succeeds.
                        await self.calls.unmute(chat_id)
                        # Do not report "Now playing" until NTgCalls confirms
                        # that a native media connection exists.
                        await self._wait_for_native_connection(chat_id)
                        logger.info(
                            "Voice chat media connection ready for chat %s "
                            "(attempt %s)",
                            chat_id,
                            attempt + 1,
                        )
                        connected = True
                        break
                    except (NoVideoSourceFound, NoAudioSourceFound) as exc:
                        if attempt >= 2 or not self._can_refresh_media_source(track):
                            raise
                        logger.warning(
                            "PyTgCalls could not read the %s track for chat %s; "
                            "refreshing the media URL (attempt %s): %s",
                            "video" if isinstance(exc, NoVideoSourceFound) else "audio",
                            chat_id,
                            attempt + 1,
                            self._redact_error(str(exc)),
                        )
                        try:
                            track = await self._refresh_media_source(track)
                        except Exception:
                            logger.warning(
                                "Could not refresh failed voice media source for chat %s",
                                chat_id,
                                exc_info=True,
                            )
                            raise exc
                        state.current = track
                        await asyncio.sleep(0.5)
                        continue
                    except NotInCallError:
                        if attempt == 2:
                            raise
                        logger.warning(
                            "Voice chat connection dropped during startup for "
                            "chat %s; retrying once",
                            chat_id,
                        )
                        try:
                            await self._leave_call(chat_id)
                        except Exception:
                            pass
                        await asyncio.sleep(1)
                if not connected:
                    raise RuntimeError(
                        "The assistant could not establish a voice-chat media connection."
                    )
                waiter = self._start_waiters.pop(chat_id, None)
                if waiter and not waiter.done():
                    waiter.set_result(None)
                await self._wait_until_stream_ends(chat_id)
                skipped = chat_id in self._skip_requests
                self._skip_requests.discard(chat_id)
                if state.loop and not skipped and self._states.get(chat_id) is state:
                    state.queue.insert(0, track)
            except asyncio.CancelledError:
                waiter = self._start_waiters.pop(chat_id, None)
                if waiter and not waiter.done():
                    waiter.cancel()
                raise
            except Exception as exc:
                waiter = self._start_waiters.pop(chat_id, None)
                if waiter and not waiter.done():
                    waiter.set_exception(exc)
                logger.error("Voice chat playback failed for chat %s: %s", chat_id, self._redact_error(str(exc)))
                # A failed current track must not strand upcoming tracks.
                # Keep the state object and let the worker advance normally.
                state = self._states.get(chat_id)
                if state and state.queue and self._states.get(chat_id) is state:
                    state.current = None
                    state.position = 0
                    state.started_at = None
                    continue
                try:
                    await self._leave_call(
                        chat_id,
                        close=chat_id in self._owned_calls,
                    )
                except Exception:
                    logger.debug("Could not leave failed voice call %s", chat_id, exc_info=True)
                self._owned_calls.discard(chat_id)
                return
            finally:
                state = self._states.get(chat_id)
                if state:
                    state.current = None
                    state.position = 0
                    state.started_at = None
                self._end_events.pop(chat_id, None)
                self._cleanup_track(track) if not (
                    state and state.loop and track in state.queue
                ) else None
                if state and not state.current and not state.queue:
                    await self._cleanup_idle_chat(chat_id, state)

    async def _cleanup_idle_chat(self, chat_id: int, state: VoiceChatState) -> None:
        lock = self._chat_locks.get(chat_id)
        if lock is None:
            return
        async with lock:
            async with self._lock:
                if (
                    self._states.get(chat_id) is not state
                    or state.current is not None
                    or state.queue
                ):
                    return
                self._states.pop(chat_id, None)
                self._end_events.pop(chat_id, None)
                self._control_events.pop(chat_id, None)
                waiter = self._start_waiters.pop(chat_id, None)
                if waiter and not waiter.done():
                    waiter.cancel()
                if self._play_tasks.get(chat_id) is asyncio.current_task():
                    self._play_tasks.pop(chat_id, None)
                self._skip_requests.discard(chat_id)
            self._chat_locks.pop(chat_id, None)

    async def _wait_until_stream_ends(self, chat_id: int) -> None:
        state = self._states.get(chat_id)
        duration = state.current.duration if state and state.current else None
        event = self._end_events.get(chat_id)
        if not event:
            return
        control = self._control_events.setdefault(chat_id, asyncio.Event())
        while True:
            remaining = (
                max(1, duration - self.current_position(chat_id) + 10)
                if duration
                else None
            )
            end_task = asyncio.create_task(event.wait())
            control_task = asyncio.create_task(control.wait())
            try:
                done, _ = await asyncio.wait(
                    {end_task, control_task},
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                end_task.cancel()
                control_task.cancel()
                await asyncio.gather(end_task, control_task, return_exceptions=True)
            if end_task in done:
                return
            if control_task in done:
                control.clear()
                if event.is_set():
                    return
                continue
            return

    async def handle_update(self, _: Any, update: Any) -> None:
        if isinstance(update, StreamEnded):
            event = self._end_events.get(update.chat_id)
            if event:
                event.set()
        elif isinstance(update, ChatUpdate) and update.status & ChatUpdate.Status.LEFT_CALL:
            logger.warning("Voice chat connection ended for chat %s", update.chat_id)
            await self._recover_disconnected_call(update.chat_id)

    async def _resolve(
        self,
        source: str,
        requester_id: int,
        *,
        video: bool = False,
        stream_quality: str = "original",
    ) -> VoiceTrack:
        query = source.strip()
        stream_quality = normalize_stream_quality(stream_quality)
        if not query:
            raise ValueError("Provide a media URL or search query.")
        explicit_url = bool(re.match(r"^https?://", query, re.IGNORECASE))
        if explicit_url:
            query = normalize_google_drive_url(query)
            await asyncio.to_thread(self._assert_safe_public_url, query)
        if not re.match(r"^https?://", query, re.IGNORECASE):
            query = f"ytsearch1:{query}"
        direct = self._direct_stream_fallback(query, requester_id, video=video)
        if direct:
            return direct
        if explicit_url:
            probed = await asyncio.to_thread(
                self._probe_public_media_url,
                query,
                requester_id,
                video,
            )
            if probed:
                return probed
            if google_drive_file_id(query):
                raise ValueError(
                    "The Google Drive link is public but does not contain a "
                    "playable audio/video file. Use /mirror for ZIPs and other "
                    "documents."
                )
        loop = asyncio.get_running_loop()

        cookie_path = (
            self._cookie_store.path_for(requester_id)
            if self._cookie_store
            else None
        )

        def extract() -> dict[str, Any]:
            options = {
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
                "format": (
                    (
                        (
                            (
                                f"bestvideo*[height<={stream_quality}]"
                                "[vcodec^=avc1]+bestaudio[acodec^=mp4a]/"
                                f"best[height<={stream_quality}]"
                                "[vcodec^=avc1][acodec^=mp4a]/"
                                f"bestvideo*[height<={stream_quality}]"
                                "[vcodec!=none]+bestaudio/"
                                f"best[height<={stream_quality}]"
                                "[acodec!=none][vcodec!=none]"
                            )
                            if stream_quality != "original"
                            else (
                                "bestvideo*[vcodec^=avc1]+bestaudio[acodec^=mp4a]/"
                                "best[vcodec^=avc1][acodec^=mp4a]/"
                                "bestvideo*[vcodec!=none]+bestaudio/"
                                "best[acodec!=none][vcodec!=none]"
                            )
                        )
                        + "/best"
                    )
                    if video
                    else "bestaudio/best"
                ),
                "skip_download": True,
                "cachedir": False,
                "socket_timeout": 30,
                "retries": 2,
            }
            if cookie_path:
                options["cookiefile"] = str(cookie_path)
            if self._pot_provider_url:
                options["remote_components"] = ["ejs:github"]
                options["extractor_args"] = {
                    "youtubepot-bgutilhttp": {
                        "base_url": [self._pot_provider_url],
                    }
                }
            import shutil

            if bun := shutil.which("bun"):
                options["js_runtimes"] = {"bun": {"path": bun}}
            elif deno := shutil.which("deno"):
                options["js_runtimes"] = {"deno": {"path": deno}}
            try:
                with yt_dlp.YoutubeDL(options) as client:
                    info = client.extract_info(query, download=False)
                    if info and info.get("entries"):
                        info = next(
                            (entry for entry in info["entries"] if entry),
                            None,
                        )
                    return info or {}
            finally:
                if self._cookie_store:
                    self._cookie_store.cleanup_materialized(cookie_path)

        info = await loop.run_in_executor(None, extract)
        stream_url = str(info.get("url") or "")
        requested_formats = [
            item
            for item in (info.get("requested_formats") or [])
            if isinstance(item, dict)
        ]
        info_has_audio = (
            "acodec" not in info
            or info.get("acodec") not in {None, "", "none"}
        )
        info_has_video = (
            "vcodec" not in info
            or info.get("vcodec") not in {None, "", "none"}
        )
        audio_url = next(
            (
                str(item.get("url"))
                for item in requested_formats
                if item.get("url") and item.get("acodec") not in {None, "none"}
            ),
            stream_url if info_has_audio else "",
        )
        video_url = next(
            (
                str(item.get("url"))
                for item in requested_formats
                if item.get("url") and item.get("vcodec") not in {None, "none"}
            ),
            stream_url if video and info_has_video else None,
        )
        if not audio_url or (video and not video_url):
            raise ValueError(
                "Could not resolve a playable video and audio stream."
                if video
                else "Could not resolve a playable audio stream."
            )

        def stream_headers(item: dict[str, Any] | None) -> dict[str, str]:
            if not item:
                return {}
            raw_headers = item.get("http_headers")
            if not isinstance(raw_headers, dict):
                return {}
            return {
                str(name): str(value)
                for name, value in raw_headers.items()
                if isinstance(name, str)
                and isinstance(value, (str, int, float))
                and "\r" not in str(value)
                and "\n" not in str(value)
            }

        audio_format = next(
            (item for item in requested_formats if item.get("url") == audio_url),
            None,
        )
        video_format = next(
            (item for item in requested_formats if item.get("url") == video_url),
            None,
        )
        fallback_headers = stream_headers(info)
        return VoiceTrack(
            title=str(info.get("title") or source)[:180],
            source=source,
            stream_url=stream_url,
            requester_id=requester_id,
            duration=int(info["duration"]) if info.get("duration") else None,
            video=video,
            audio_url=audio_url,
            video_url=video_url,
            audio_headers=stream_headers(audio_format) or fallback_headers,
            video_headers=stream_headers(video_format) or fallback_headers,
            stream_quality=stream_quality,
        )

    @staticmethod
    def _assert_safe_public_url(source: str) -> None:
        parsed = urlparse(source)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Use a public HTTP(S) media link or a YouTube URL.")
        if parsed.username or parsed.password:
            raise ValueError("Links containing embedded credentials are not allowed.")
        host = parsed.hostname.lower().rstrip(".")
        if host in {"localhost", "localhost.localdomain"}:
            raise ValueError("Local network links cannot be played in voice chat.")
        try:
            addresses = {
                result[4][0]
                for result in socket.getaddrinfo(
                    host,
                    parsed.port or (443 if parsed.scheme.lower() == "https" else 80),
                    type=socket.SOCK_STREAM,
                )
            }
        except (OSError, ValueError):
            # Let yt-dlp or the media probe report an ordinary DNS failure.
            addresses = set()
        for raw_address in addresses:
            address = ipaddress.ip_address(raw_address)
            if (
                address.is_private
                or address.is_loopback
                or address.is_link_local
                or address.is_reserved
                or address.is_multicast
                or address.is_unspecified
            ):
                raise ValueError("Private or local network links cannot be played.")

    @classmethod
    def _probe_public_media_url(
        cls,
        source: str,
        requester_id: int,
        video: bool,
    ) -> VoiceTrack | None:
        """Identify safe extensionless public media URLs without downloading them."""
        parsed = urlparse(source)
        if not parsed.hostname:
            return None
        # Known extractors should receive the URL directly; a HEAD request to
        # YouTube/social pages is both unnecessary and often misleading.
        host = parsed.hostname.lower().rstrip(".")
        if any(
            host == domain or host.endswith("." + domain)
            for domain in (
                "youtube.com",
                "youtu.be",
                "googlevideo.com",
                "soundcloud.com",
                "vimeo.com",
                "twitch.tv",
                "dailymotion.com",
            )
        ):
            return None
        current = source
        opener = build_opener(_NoRedirect)
        response = None
        try:
            for _ in range(3):
                request = Request(
                    current,
                    method="HEAD",
                    headers={"User-Agent": "ytdlbot-voice/1.0"},
                )
                try:
                    response = opener.open(request, timeout=8)
                    break
                except HTTPError as exc:
                    if exc.code in {301, 302, 303, 307, 308} and exc.headers.get(
                        "Location"
                    ):
                        from urllib.parse import urljoin

                        current = urljoin(current, exc.headers["Location"])
                        cls._assert_safe_public_url(current)
                        continue
                    if exc.code not in {405, 501}:
                        return None
                    request = Request(
                        current,
                        method="GET",
                        headers={
                            "Range": "bytes=0-0",
                            "User-Agent": "ytdlbot-voice/1.0",
                        },
                    )
                    response = opener.open(request, timeout=8)
                    break
                except (OSError, URLError):
                    return None
                content_type = response.headers.get("Content-Type", "").split(
                    ";", 1
                )[0].strip().lower()
                if google_drive_file_id(current) and content_type == "text/html":
                    response.close()
                    response = None
                    request = Request(
                        current,
                        headers={
                            "Range": "bytes=0-65535",
                            "User-Agent": "ytdlbot-voice/1.0",
                        },
                    )
                    response = opener.open(request, timeout=8)
                    body = response.read(64 * 1024)
                    confirmation = google_drive_confirmation_url(current, body)
                    response.close()
                    response = None
                    if not confirmation:
                        return None
                    current = confirmation
                    continue
                break
            if response is None:
                return None
            content_type = response.headers.get("Content-Type", "").split(";", 1)[
                0
            ].strip().lower()
            filename = cls._response_filename(response.headers)
            if not cls._is_playable_content_type(content_type, video, filename):
                return None
            title = filename or unquote(Path(urlparse(current).path).name)
            title = title or "Public media stream"
            return VoiceTrack(
                title=title[:180],
                source=source,
                stream_url=current,
                requester_id=requester_id,
                video=video,
                audio_url=current,
                video_url=current if video else None,
            )
        except (OSError, ValueError, URLError):
            return None
        finally:
            if response is not None:
                response.close()

    @staticmethod
    def _response_filename(headers: Any) -> str:
        disposition = str(headers.get("Content-Disposition", ""))
        if not disposition:
            return ""
        try:
            parsed = EmailMessage()
            parsed["Content-Disposition"] = disposition
            return Path(parsed.get_filename() or "").name
        except (TypeError, ValueError):
            return ""

    @staticmethod
    def _is_playable_content_type(
        content_type: str,
        video: bool,
        filename: str = "",
    ) -> bool:
        if content_type.startswith("audio/"):
            return not video
        if content_type.startswith("video/"):
            return True
        if content_type in {
            "application/dash+xml",
            "application/mpegurl",
            "application/vnd.apple.mpegurl",
            "application/x-mpegurl",
        }:
            return True
        suffix = Path(filename.lower()).suffix
        video_extensions = {
            ".3g2", ".3gp", ".avi", ".flv", ".m2ts", ".m4v", ".mkv",
            ".mov", ".mp4", ".mpeg", ".mpg", ".m3u", ".m3u8", ".mpd",
            ".ogv", ".ts", ".webm", ".wmv",
        }
        audio_extensions = {
            ".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav",
        }
        if suffix in video_extensions:
            return True
        return not video and suffix in audio_extensions

    @staticmethod
    def _direct_stream_fallback(
        source: str,
        requester_id: int,
        *,
        video: bool,
    ) -> VoiceTrack | None:
        """Accept an explicit public media URL when no extractor is needed.

        This is intentionally limited to obvious media/stream extensions and
        public hosts. YouTube and other supported sites continue through
        yt-dlp, while local/private URLs are never handed to the assistant.
        """
        parsed = urlparse(source)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            return None
        if parsed.username or parsed.password:
            return None
        host = parsed.hostname.lower().rstrip(".")
        if host in {"localhost", "localhost.localdomain"}:
            return None
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None
        if address and (
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_reserved
            or address.is_multicast
            or address.is_unspecified
        ):
            return None
        path = parsed.path.lower()
        if not path.endswith(
            (
                ".3g2",
                ".3gp",
                ".avi",
                ".flv",
                ".m3u8",
            ".m3u",
                ".mpd",
                ".mp4",
                ".m4v",
                ".webm",
                ".mkv",
                ".mov",
                ".m2ts",
                ".mjpeg",
                ".mjpg",
                ".mpeg",
                ".mpg",
                ".ogv",
                ".ts",
                ".wmv",
                ".mp3",
                ".m4a",
                ".aac",
                ".ogg",
                ".opus",
                ".wav",
                ".flac",
            )
        ):
            return None
        audio_only_extensions = {
            ".aac",
            ".flac",
            ".m4a",
            ".oga",
            ".mp3",
            ".ogg",
            ".opus",
            ".wav",
            ".weba",
        }
        if video and Path(path).suffix.lower() in audio_only_extensions:
            return None
        title = unquote(Path(parsed.path).name) or "Direct stream"
        return VoiceTrack(
            title=title[:180],
            source=source,
            stream_url=source,
            requester_id=requester_id,
            video=video,
            audio_url=source,
            video_url=source if video else None,
        )

    def _require_enabled(self) -> None:
        if not self.enabled or not self.calls:
            raise RuntimeError(self.setup_hint)

    @staticmethod
    def user_error(exc: Exception) -> str:
        text = " ".join(str(exc).split()).strip()
        name = type(exc).__name__
        if name == "NoActiveGroupCall":
            return (
                "Telegram could not create the group video chat. "
                "Make sure the assistant is a group member with permission "
                "to manage video chats."
            )
        if "BOT_METHOD_INVALID" in text.upper() or "CREATEGROUPCALL" in text.upper():
            return (
                "Telegram rejected automatic video-chat creation. "
                "Promote the assistant and grant it permission to manage "
                "video chats, then retry /vplay."
            )
        if name == "NotInCallError":
            return "The assistant is not connected to this voice chat."
        if name == "NoVideoSourceFound":
            return (
                "The video stream could not be read by FFmpeg after automatic "
                "refresh attempts. Retry /vplay video; a fresh source will be "
                "selected."
            )
        if name == "NoAudioSourceFound":
            return (
                "The audio stream could not be read by FFmpeg after automatic "
                "refresh attempts. Retry /vplay; a fresh source will be selected."
            )
        if name in {"ChatAdminRequired", "RPCError"} or "ADMIN" in text.upper():
            return "The assistant needs permission to manage voice chats in this group."
        if "USER_NOT_PARTICIPANT" in text.upper() or "CHAT_ID_INVALID" in text.upper():
            return "The assistant account is not a member of this group or channel."
        if "SIGN IN" in text.upper() or "BOT CHECK" in text.upper():
            return (
                "YouTube blocked video extraction. Add a fresh YouTube cookies "
                "file with /cookies, then retry /vplay video."
            )
        if "COULD NOT RESOLVE" in text.upper() or "NO PLAYABLE" in text.upper():
            return "YouTube did not return a playable audio/video stream."
        if name == "RuntimeError":
            return text
        return f"{name}: {text[:300]}" if text else name