import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cryptography.fernet import Fernet

import bot_cookies as c

NETSCAPE_SAMPLE = (
    b"# Netscape HTTP Cookie File\n"
    b".youtube.com\tTRUE\t/\tTRUE\t9999999999\tSID\tabc123\n"
    b".youtube.com\tTRUE\t/\tTRUE\t1\tEXPIRED\tabc123\n"
)

JSON_SAMPLE = (
    b'[{"domain": ".youtube.com", "name": "SID", "value": "abc123", '
    b'"path": "/", "secure": true, "expirationDate": 9999999999}]'
)


class TestNormalizeCookieExport(unittest.TestCase):
    def test_rejects_empty(self):
        with self.assertRaises(ValueError):
            c.normalize_cookie_export(b"   ")

    def test_rejects_oversized(self):
        with self.assertRaises(ValueError):
            c.normalize_cookie_export(b"x" * 2_000_001)

    def test_rejects_binary_content(self):
        with self.assertRaises(ValueError):
            c.normalize_cookie_export(b"# Netscape\x00binary")

    def test_accepts_valid_netscape_file(self):
        result = c.normalize_cookie_export(NETSCAPE_SAMPLE)
        self.assertTrue(result.endswith(b"\n"))
        self.assertIn(b"youtube.com", result)

    def test_rejects_malformed_netscape_rows(self):
        malformed = b"# Netscape HTTP Cookie File\ntoo\tfew\tfields\n"
        with self.assertRaises(ValueError):
            c.normalize_cookie_export(malformed)

    def test_converts_json_export_to_netscape(self):
        result = c.normalize_cookie_export(JSON_SAMPLE)
        self.assertTrue(result.startswith(b"# Netscape"))
        fields = result.splitlines()[1].split(b"\t")
        self.assertEqual(len(fields), 7)
        self.assertEqual(fields[0], b".youtube.com")
        self.assertEqual(fields[5], b"SID")
        self.assertEqual(fields[6], b"abc123")

    def test_json_wrapped_in_cookies_key(self):
        payload = b'{"cookies": [{"domain": "youtube.com", "name": "X", "value": "y"}]}'
        result = c.normalize_cookie_export(payload)
        self.assertIn(b"X\ty", result)

    def test_rejects_invalid_json_and_non_netscape_text(self):
        with self.assertRaises(ValueError):
            c.normalize_cookie_export(b"not json and not netscape")

    def test_rejects_json_with_no_usable_records(self):
        with self.assertRaises(ValueError):
            c.normalize_cookie_export(b"[{\"domain\": \"\", \"name\": \"\"}]")


class TestLooksLikeCookieDocument(unittest.TestCase):
    def test_matches_cookie_in_filename(self):
        self.assertTrue(c.looks_like_cookie_document("my_cookies.txt"))

    def test_matches_txt_or_json_extension(self):
        self.assertTrue(c.looks_like_cookie_document("export.json"))
        self.assertTrue(c.looks_like_cookie_document("random.txt"))

    def test_rejects_unrelated_filename(self):
        self.assertFalse(c.looks_like_cookie_document("video.mp4"))

    def test_handles_none(self):
        self.assertFalse(c.looks_like_cookie_document(None))


class TestCookieStore(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.key = Fernet.generate_key()
        self.store = c.CookieStore(Path(self.tmpdir.name) / "cookies", self.key)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_directory_created_with_restricted_permissions(self):
        mode = self.store.directory.stat().st_mode & 0o777
        self.assertEqual(mode, 0o700)

    def test_save_and_retrieve_roundtrip(self):
        self.store.save(42, NETSCAPE_SAMPLE)
        self.assertTrue(self.store.has(42))
        materialized = self.store.path_for(42)
        self.assertIsNotNone(materialized)
        self.assertIn(b"youtube.com", materialized.read_bytes())
        self.store.cleanup_materialized(materialized)
        self.assertFalse(materialized.exists())

    def test_stored_file_is_encrypted_on_disk(self):
        self.store.save(42, NETSCAPE_SAMPLE)
        raw = self.store._path(42).read_bytes()
        self.assertNotIn(b"youtube.com", raw)

    def test_path_for_missing_user_returns_none(self):
        self.assertIsNone(self.store.path_for(999))

    def test_delete_removes_stored_cookie(self):
        self.store.save(42, NETSCAPE_SAMPLE)
        self.assertTrue(self.store.delete(42))
        self.assertFalse(self.store.has(42))
        self.assertFalse(self.store.delete(42))  # second delete is a no-op

    def test_wrong_key_cannot_decrypt(self):
        self.store.save(42, NETSCAPE_SAMPLE)
        other_store = c.CookieStore(self.store.directory, Fernet.generate_key())
        with self.assertRaises(RuntimeError):
            other_store.path_for(42)

    def test_health_reports_expired_and_domains(self):
        self.store.save(42, NETSCAPE_SAMPLE)
        health = self.store.health(42)
        self.assertTrue(health["present"])
        self.assertEqual(health["records"], 2)
        self.assertEqual(health["expired"], 1)
        self.assertIn(".youtube.com", health["youtube_domains"])

    def test_health_for_missing_user(self):
        health = self.store.health(999)
        self.assertFalse(health["present"])
        self.assertEqual(health["records"], 0)


class TestCookieStoreSyncBack(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.key = Fernet.generate_key()
        self.store = c.CookieStore(Path(self.tmpdir.name) / "cookies", self.key)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_sync_back_persists_updated_cookie_contents(self):
        self.store.save(42, NETSCAPE_SAMPLE)
        materialized = self.store.path_for(42)
        # Simulate yt-dlp rewriting the cookiejar with a rotated value.
        rotated = NETSCAPE_SAMPLE.replace(b"abc123", b"rotated456")
        materialized.write_bytes(rotated)
        changed = self.store.sync_back(42, materialized)
        self.assertTrue(changed)
        self.store.cleanup_materialized(materialized)
        refreshed = self.store.path_for(42)
        self.assertIn(b"rotated456", refreshed.read_bytes())
        self.store.cleanup_materialized(refreshed)

    def test_sync_back_is_noop_when_unchanged(self):
        self.store.save(42, NETSCAPE_SAMPLE)
        materialized = self.store.path_for(42)
        changed = self.store.sync_back(42, materialized)
        self.assertFalse(changed)
        self.store.cleanup_materialized(materialized)

    def test_sync_back_handles_missing_path(self):
        self.assertFalse(self.store.sync_back(42, None))

    def test_sync_back_handles_deleted_materialized_file(self):
        self.store.save(42, NETSCAPE_SAMPLE)
        materialized = self.store.path_for(42)
        materialized.unlink()
        self.assertFalse(self.store.sync_back(42, materialized))

    def test_sync_back_ignores_unreadable_garbage(self):
        self.store.save(42, NETSCAPE_SAMPLE)
        materialized = self.store.path_for(42)
        materialized.write_bytes(b"not a cookie file at all")
        self.assertFalse(self.store.sync_back(42, materialized))
        self.store.cleanup_materialized(materialized)

    def test_sync_back_creates_entry_for_new_user(self):
        # A user with no prior stored cookies can still gain one from a
        # materialized file (defensive path; not hit in normal flow since
        # path_for() returns None when nothing is stored).
        self.tmpdir2 = tempfile.TemporaryDirectory()
        scratch = Path(self.tmpdir2.name) / "scratch.cookies.txt"
        scratch.write_bytes(NETSCAPE_SAMPLE)
        try:
            self.assertTrue(self.store.sync_back(99, scratch))
            self.assertTrue(self.store.has(99))
        finally:
            self.tmpdir2.cleanup()


if __name__ == "__main__":
    unittest.main()
