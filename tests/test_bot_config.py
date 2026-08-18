import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import bot_config as cfg


REQUIRED_ENV = {
    "API_ID": "123456",
    "API_HASH": "a" * 32,
    "BOT_TOKEN": "123456:AAA-fake-token",
}


class EnvTestCase(unittest.TestCase):
    """Base class that snapshots/restores os.environ around each test."""

    def setUp(self):
        self._original_env = dict(os.environ)
        # Start from a clean slate for the bot-relevant variables so leftover
        # host env vars in the sandbox can't leak between tests.
        for key in list(os.environ):
            if key in REQUIRED_ENV or key.startswith(
                ("WORK_DIR", "COOKIE", "SESSION_SECRET", "MAX_", "FILE_URL",
                 "BIN_CHANNEL_ID", "VC_", "ADMIN_ID", "MONGODB_", "REDIS_URL",
                 "ENABLE_REDIS", "PORT", "PUBLIC_URL", "HEALTH_", "RESTRICTED_")
            ):
                os.environ.pop(key, None)
        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["WORK_DIR"] = self._tmpdir.name

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._original_env)
        self._tmpdir.cleanup()


class TestAsBool(unittest.TestCase):
    def test_truthy_values(self):
        for v in ("1", "true", "True", "yes", "on", "ON"):
            self.assertTrue(cfg._as_bool(v, default=False), v)

    def test_falsy_values(self):
        for v in ("0", "false", "no", "off", "garbage"):
            self.assertFalse(cfg._as_bool(v, default=True), v)

    def test_empty_uses_default(self):
        self.assertTrue(cfg._as_bool("", default=True))
        self.assertFalse(cfg._as_bool("", default=False))


class TestAsInt(EnvTestCase):
    def test_uses_default_when_unset(self):
        self.assertEqual(cfg._as_int("SOME_MISSING_VAR", 42), 42)

    def test_parses_valid_int(self):
        os.environ["MAX_QUEUE_SIZE"] = "10"
        self.assertEqual(cfg._as_int("MAX_QUEUE_SIZE", 1), 10)

    def test_rejects_non_numeric(self):
        os.environ["MAX_QUEUE_SIZE"] = "not-a-number"
        with self.assertRaises(RuntimeError):
            cfg._as_int("MAX_QUEUE_SIZE", 1)

    def test_enforces_minimum(self):
        os.environ["MAX_QUEUE_SIZE"] = "-5"
        with self.assertRaises(RuntimeError):
            cfg._as_int("MAX_QUEUE_SIZE", 1, minimum=0)


class TestOptionalChatId(EnvTestCase):
    def test_unset_returns_none(self):
        self.assertIsNone(cfg._optional_chat_id("BIN_CHANNEL_ID"))

    def test_valid_negative_channel_id(self):
        os.environ["BIN_CHANNEL_ID"] = "-1001234567890"
        self.assertEqual(cfg._optional_chat_id("BIN_CHANNEL_ID"), -1001234567890)

    def test_zero_is_rejected(self):
        os.environ["BIN_CHANNEL_ID"] = "0"
        with self.assertRaises(RuntimeError):
            cfg._optional_chat_id("BIN_CHANNEL_ID")

    def test_out_of_range_is_rejected(self):
        os.environ["BIN_CHANNEL_ID"] = "99999999999999"
        with self.assertRaises(RuntimeError):
            cfg._optional_chat_id("BIN_CHANNEL_ID")

    def test_non_numeric_is_rejected(self):
        os.environ["BIN_CHANNEL_ID"] = "not-an-id"
        with self.assertRaises(RuntimeError):
            cfg._optional_chat_id("BIN_CHANNEL_ID")


class TestOptionalPositiveInt(EnvTestCase):
    def test_unset_returns_none(self):
        self.assertIsNone(cfg._optional_positive_int("ADMIN_ID"))

    def test_positive_value_accepted(self):
        os.environ["ADMIN_ID"] = "123"
        self.assertEqual(cfg._optional_positive_int("ADMIN_ID"), 123)

    def test_zero_or_negative_rejected(self):
        for v in ("0", "-1"):
            os.environ["ADMIN_ID"] = v
            with self.assertRaises(RuntimeError):
                cfg._optional_positive_int("ADMIN_ID")


class TestSettingsFromEnv(EnvTestCase):
    def test_missing_required_var_raises(self):
        with self.assertRaises(RuntimeError):
            cfg.Settings.from_env()

    def test_minimal_valid_config_builds_settings(self):
        os.environ.update(REQUIRED_ENV)
        settings = cfg.Settings.from_env()
        self.assertEqual(settings.api_id, 123456)
        self.assertEqual(settings.api_hash, REQUIRED_ENV["API_HASH"])
        self.assertEqual(settings.bot_token, REQUIRED_ENV["BOT_TOKEN"])
        # Defaults
        self.assertEqual(settings.max_queue_size, 32)
        self.assertEqual(settings.workers, 2)
        self.assertFalse(settings.enable_redis)

    def test_invalid_api_id_raises(self):
        os.environ.update(REQUIRED_ENV)
        os.environ["API_ID"] = "not-numeric"
        with self.assertRaises(RuntimeError):
            cfg.Settings.from_env()

    def test_zero_api_id_raises(self):
        os.environ.update(REQUIRED_ENV)
        os.environ["API_ID"] = "0"
        with self.assertRaises(RuntimeError):
            cfg.Settings.from_env()

    def test_file_url_ttl_is_capped_at_three_hours(self):
        os.environ.update(REQUIRED_ENV)
        os.environ["FILE_URL_TTL"] = str(999999)
        settings = cfg.Settings.from_env()
        self.assertEqual(settings.file_url_ttl, cfg.MAX_FILE_LINK_TTL_SECONDS)

    def test_invalid_file_url_base_raises(self):
        os.environ.update(REQUIRED_ENV)
        os.environ["FILE_URL_BASE"] = "not-a-url"
        with self.assertRaises(RuntimeError):
            cfg.Settings.from_env()

    def test_cookie_secret_is_persisted_across_calls(self):
        """A generated fallback cookie key must survive a second load,
        otherwise previously-encrypted cookies would become undecryptable
        after every restart."""
        os.environ.update(REQUIRED_ENV)
        first = cfg.Settings.from_env()
        second = cfg.Settings.from_env()
        self.assertEqual(first.cookie_key, second.cookie_key)

    def test_prepare_directories_creates_work_and_cookie_dirs(self):
        os.environ.update(REQUIRED_ENV)
        settings = cfg.Settings.from_env()
        settings.prepare_directories()
        self.assertTrue(settings.work_dir.is_dir())
        self.assertTrue(settings.cookie_dir.is_dir())


if __name__ == "__main__":
    unittest.main()
