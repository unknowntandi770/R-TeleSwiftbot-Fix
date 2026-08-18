from __future__ import annotations

from html import unescape
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

URL_RE = re.compile(r"https?://[^\s<>]+", re.IGNORECASE)
MAGNET_RE = re.compile(r"magnet:\?[^\s<>]+", re.IGNORECASE)
DRIVE_FILE_RE = re.compile(
    r"^/file/d/(?P<file_id>[A-Za-z0-9_-]+)(?:/|$)",
    re.IGNORECASE,
)


def extract_url(text: str) -> str | None:
    match = URL_RE.search(text or "")
    if not match:
        return None
    candidate = match.group(0).rstrip(".,!?)]}>\"'")
    parsed = urlparse(candidate)
    if not parsed.netloc:
        return None
    return candidate


def extract_source(text: str) -> str | None:
    """Extract either an HTTP(S) URL or a BitTorrent magnet URI."""
    value = text or ""
    matches = list(URL_RE.finditer(value)) + list(MAGNET_RE.finditer(value))
    if not matches:
        return None
    match = min(matches, key=lambda item: item.start())
    candidate = match.group(0).rstrip(".,!?)]}>\"'")
    if candidate.lower().startswith("magnet:?"):
        parsed = urlparse(candidate)
        if parsed.scheme.lower() != "magnet" or not parsed.query:
            return None
        return candidate
    parsed = urlparse(candidate)
    return candidate if parsed.netloc else None


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    ignored_query_keys = {
        "feature",
        "si",
        "utm_campaign",
        "utm_medium",
        "utm_source",
        "utm_term",
        "utm_content",
    }
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in ignored_query_keys
    ]
    return urlunparse(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", urlencode(query), "")
    )


def google_drive_file_id(url: str) -> str | None:
    """Extract a public Google Drive file id from common share URL shapes."""
    parsed = urlparse(url.strip().strip("\"'"))
    host = (parsed.hostname or "").lower().rstrip(".")
    if host not in {
        "drive.google.com",
        "docs.google.com",
        "drive.usercontent.google.com",
    }:
        return None
    match = DRIVE_FILE_RE.match(parsed.path)
    if match:
        return match.group("file_id")
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    file_id = query.get("id", "")
    return file_id if re.fullmatch(r"[A-Za-z0-9_-]+", file_id) else None


def normalize_google_drive_url(url: str) -> str:
    """Use Drive's file endpoint instead of the HTML viewer page."""
    file_id = google_drive_file_id(url)
    if not file_id:
        return url.strip().strip("\"'")
    return (
        "https://drive.usercontent.google.com/download?"
        f"id={file_id}&export=download"
    )


def google_drive_confirmation_url(url: str, body: bytes) -> str | None:
    """Build Drive's virus-scan confirmation URL from a bounded HTML response."""
    file_id = google_drive_file_id(url)
    if not file_id or b"Virus scan warning" not in body:
        return None
    text = body.decode("utf-8", "replace")
    fields = {
        unescape(name): unescape(value)
        for name, value in re.findall(
            r'<input[^>]+name=["\']([^"\']+)["\'][^>]+value=["\']([^"\']*)["\']',
            text,
            re.IGNORECASE,
        )
    }
    if fields.get("id") != file_id or fields.get("confirm") != "t":
        return None
    query = {
        "id": file_id,
        "export": "download",
        "confirm": "t",
    }
    if fields.get("uuid"):
        query["uuid"] = fields["uuid"]
    return "https://drive.usercontent.google.com/download?" + urlencode(query)


def is_supported_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def is_magnet_url(source: str) -> bool:
    parsed = urlparse(source.strip())
    return parsed.scheme.lower() == "magnet" and bool(parsed.query)


def is_supported_source(source: str) -> bool:
    return is_supported_url(source) or is_magnet_url(source)


def is_youtube_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().rstrip(".")
    return host == "youtu.be" or host == "youtube.com" or host.endswith(
        ".youtube.com"
    ) or host.endswith(".youtube-nocookie.com")


def is_playlist_url(url: str) -> bool:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    return "list" in query and (
        parsed.netloc.lower().endswith("youtube.com")
        or parsed.netloc.lower().endswith("youtube-nocookie.com")
    )


def is_stream_manifest(url: str) -> bool:
    """Return whether a URL is an HLS or MPEG-DASH manifest."""
    path = urlparse(url.strip().strip("\"'")).path.lower()
    return path.endswith((".m3u8", ".m3u", ".mpd"))


def is_torrent_url(url: str) -> bool:
    """Return whether an HTTP(S) URL points to a torrent metainfo file."""
    parsed = urlparse(url.strip().strip("\"'"))
    return parsed.scheme.lower() in {"http", "https"} and (
        parsed.path.lower().endswith(".torrent")
        or dict(parse_qsl(parsed.query, keep_blank_values=True))
        .get("filename", "")
        .lower()
        .endswith(".torrent")
    )


def is_torrent_source(source: str) -> bool:
    return is_magnet_url(source) or is_torrent_url(source)


def source_kind(source: str) -> str:
    """Classify a user source before choosing the safest production pipeline."""
    value = source.strip().strip("\"'")
    if is_torrent_source(value):
        return "torrent"
    if is_youtube_url(value):
        return "youtube"
    if google_drive_file_id(value):
        return "drive"
    if is_stream_manifest(value):
        return "manifest"
    if is_supported_url(value):
        return "direct"
    return "search"