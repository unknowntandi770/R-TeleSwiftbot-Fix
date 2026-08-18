"""Unit tests for bot_urls.py — pure functions, no network/Telegram deps."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import bot_urls as u


class TestExtractUrl(unittest.TestCase):
    def test_finds_plain_url(self):
        self.assertEqual(u.extract_url("check https://example.com/x now"), "https://example.com/x")

    def test_strips_trailing_punctuation(self):
        self.assertEqual(u.extract_url("see https://example.com/x."), "https://example.com/x")
        self.assertEqual(u.extract_url("(https://example.com/x)"), "https://example.com/x")

    def test_no_url_returns_none(self):
        self.assertIsNone(u.extract_url("no links here"))

    def test_empty_text_returns_none(self):
        self.assertIsNone(u.extract_url(""))
        self.assertIsNone(u.extract_url(None))


class TestExtractSource(unittest.TestCase):
    def test_finds_magnet(self):
        magnet = "magnet:?xt=urn:btih:abcdef1234567890"
        self.assertEqual(u.extract_source(f"grab this {magnet} please"), magnet)

    def test_prefers_earliest_match(self):
        text = "first https://a.com then magnet:?xt=urn:btih:aaa"
        self.assertEqual(u.extract_source(text), "https://a.com")

    def test_malformed_magnet_without_query_rejected(self):
        self.assertIsNone(u.extract_source("magnet:?"))

    def test_no_source_returns_none(self):
        self.assertIsNone(u.extract_source("nothing to see"))


class TestNormalizeUrl(unittest.TestCase):
    def test_strips_tracking_params(self):
        result = u.normalize_url("https://Example.com/Path/?utm_source=x&keep=1")
        self.assertIn("keep=1", result)
        self.assertNotIn("utm_source", result)

    def test_lowercases_scheme_and_host(self):
        result = u.normalize_url("HTTPS://EXAMPLE.com/path")
        self.assertTrue(result.startswith("https://example.com"))

    def test_strips_trailing_slash(self):
        result = u.normalize_url("https://example.com/path/")
        self.assertNotIn("path/", result.split("?")[0].rstrip("/") + "X")  # sanity guard
        self.assertEqual(result, "https://example.com/path")


class TestGoogleDrive(unittest.TestCase):
    def test_extracts_file_id_from_path_form(self):
        url = "https://drive.google.com/file/d/1AbCdEfGhIjK/view?usp=sharing"
        self.assertEqual(u.google_drive_file_id(url), "1AbCdEfGhIjK")

    def test_extracts_file_id_from_query_form(self):
        url = "https://drive.google.com/open?id=1AbCdEfGhIjK"
        self.assertEqual(u.google_drive_file_id(url), "1AbCdEfGhIjK")

    def test_rejects_non_drive_host(self):
        self.assertIsNone(u.google_drive_file_id("https://not-drive.example.com/file/d/123/view"))

    def test_rejects_id_with_invalid_characters(self):
        url = "https://drive.google.com/open?id=1AbC;rm -rf"
        self.assertIsNone(u.google_drive_file_id(url))

    def test_normalize_falls_back_to_stripped_input_when_no_id(self):
        self.assertEqual(u.normalize_google_drive_url("  \"https://example.com/x\"  "), "https://example.com/x")

    def test_normalize_builds_download_endpoint(self):
        url = "https://drive.google.com/file/d/ABC123/view"
        result = u.normalize_google_drive_url(url)
        self.assertEqual(result, "https://drive.usercontent.google.com/download?id=ABC123&export=download")

    def test_confirmation_url_requires_matching_id_and_confirm_flag(self):
        file_id = "ABC123"
        url = f"https://drive.google.com/file/d/{file_id}/view"
        body = (
            b"Virus scan warning "
            b'<input type="hidden" name="id" value="ABC123">'
            b'<input type="hidden" name="confirm" value="t">'
            b'<input type="hidden" name="uuid" value="xyz">'
        )
        result = u.google_drive_confirmation_url(url, body)
        self.assertIn("confirm=t", result)
        self.assertIn("uuid=xyz", result)

    def test_confirmation_url_none_without_scan_warning(self):
        url = "https://drive.google.com/file/d/ABC123/view"
        self.assertIsNone(u.google_drive_confirmation_url(url, b"nothing relevant"))


class TestClassification(unittest.TestCase):
    def test_is_supported_url(self):
        self.assertTrue(u.is_supported_url("https://example.com"))
        self.assertFalse(u.is_supported_url("ftp://example.com"))
        self.assertFalse(u.is_supported_url("not a url"))

    def test_is_magnet_url(self):
        self.assertTrue(u.is_magnet_url("magnet:?xt=urn:btih:aaa"))
        self.assertFalse(u.is_magnet_url("magnet:?"))
        self.assertFalse(u.is_magnet_url("https://example.com"))

    def test_is_youtube_url_variants(self):
        for url in (
            "https://www.youtube.com/watch?v=x",
            "https://youtu.be/x",
            "https://m.youtube.com/watch?v=x",
            "https://www.youtube-nocookie.com/embed/x",
        ):
            self.assertTrue(u.is_youtube_url(url), url)
        self.assertFalse(u.is_youtube_url("https://notyoutube.com/watch?v=x"))

    def test_is_playlist_url(self):
        self.assertTrue(u.is_playlist_url("https://www.youtube.com/watch?v=x&list=PL123"))
        self.assertFalse(u.is_playlist_url("https://www.youtube.com/watch?v=x"))
        self.assertFalse(u.is_playlist_url("https://example.com?list=PL123"))

    def test_is_stream_manifest(self):
        self.assertTrue(u.is_stream_manifest("https://example.com/stream.m3u8"))
        self.assertTrue(u.is_stream_manifest("https://example.com/stream.mpd"))
        self.assertFalse(u.is_stream_manifest("https://example.com/video.mp4"))

    def test_is_torrent_url(self):
        self.assertTrue(u.is_torrent_url("https://example.com/file.torrent"))
        self.assertTrue(u.is_torrent_url("https://example.com/dl?filename=x.torrent"))
        self.assertFalse(u.is_torrent_url("https://example.com/file.mp4"))

    def test_source_kind_priority(self):
        self.assertEqual(u.source_kind("magnet:?xt=urn:btih:aaa"), "torrent")
        self.assertEqual(u.source_kind("https://youtu.be/x"), "youtube")
        self.assertEqual(u.source_kind("https://drive.google.com/file/d/ABC123/view"), "drive")
        self.assertEqual(u.source_kind("https://example.com/stream.m3u8"), "manifest")
        self.assertEqual(u.source_kind("https://example.com/video.mp4"), "direct")


if __name__ == "__main__":
    unittest.main()
