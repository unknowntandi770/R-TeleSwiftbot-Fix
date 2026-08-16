from __future__ import annotations

VIDEO_QUALITIES: tuple[tuple[str, str], ...] = (
    ("auto", "⚡ Best available"),
    ("2160", "2160p 4K"),
    ("1440", "1440p HD"),
    ("1080", "1080p Full HD"),
    ("720", "720p HD"),
    ("480", "480p"),
    ("360", "360p"),
)

STREAM_VIDEO_QUALITIES: tuple[tuple[str, str], ...] = (
    ("original", "🔝 Original / source maximum"),
    ("2160", "2160p 4K maximum"),
    ("1440", "1440p HD maximum"),
    ("1080", "1080p Full HD maximum"),
    ("720", "720p HD maximum"),
    ("480", "480p maximum"),
)

AUDIO_QUALITIES: tuple[tuple[str, str], ...] = (
    ("auto", "⚡ Recommended · 192 kbps"),
    ("320", "320 kbps"),
    ("256", "256 kbps"),
    ("192", "192 kbps"),
    ("128", "128 kbps"),
)


def normalize_quality(value: str | None, audio_only: bool) -> str:
    allowed = {key for key, _ in (AUDIO_QUALITIES if audio_only else VIDEO_QUALITIES)}
    candidate = str(value or "auto").lower().strip()
    return candidate if candidate in allowed else "auto"


def quality_label(value: str | None, audio_only: bool) -> str:
    normalized = normalize_quality(value, audio_only)
    choices = AUDIO_QUALITIES if audio_only else VIDEO_QUALITIES
    return next(label for key, label in choices if key == normalized)


def normalize_stream_quality(value: str | None) -> str:
    allowed = {key for key, _ in STREAM_VIDEO_QUALITIES}
    candidate = str(value or "original").lower().strip()
    return candidate if candidate in allowed else "original"


def stream_quality_label(value: str | None) -> str:
    normalized = normalize_stream_quality(value)
    return next(label for key, label in STREAM_VIDEO_QUALITIES if key == normalized)