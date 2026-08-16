import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import bot_quality as q


class TestNormalizeQuality(unittest.TestCase):
    def test_valid_video_quality_passes_through(self):
        self.assertEqual(q.normalize_quality("1080", audio_only=False), "1080")

    def test_valid_audio_quality_passes_through(self):
        self.assertEqual(q.normalize_quality("320", audio_only=True), "320")

    def test_invalid_value_falls_back_to_auto(self):
        self.assertEqual(q.normalize_quality("9999", audio_only=False), "auto")
        self.assertEqual(q.normalize_quality("bogus", audio_only=True), "auto")

    def test_none_falls_back_to_auto(self):
        self.assertEqual(q.normalize_quality(None, audio_only=False), "auto")

    def test_case_and_whitespace_normalized(self):
        self.assertEqual(q.normalize_quality("  1080  ", audio_only=False), "1080")

    def test_audio_quality_not_valid_for_video(self):
        # "320" is a valid audio bitrate but not a video resolution key
        self.assertEqual(q.normalize_quality("320", audio_only=False), "auto")


class TestQualityLabel(unittest.TestCase):
    def test_known_video_quality_label(self):
        self.assertEqual(q.quality_label("1080", audio_only=False), "1080p Full HD")

    def test_unknown_quality_falls_back_to_auto_label(self):
        self.assertEqual(q.quality_label("nonsense", audio_only=False), "⚡ Best available")


class TestStreamQuality(unittest.TestCase):
    def test_valid_stream_quality_passes_through(self):
        self.assertEqual(q.normalize_stream_quality("1080"), "1080")

    def test_invalid_stream_quality_falls_back_to_original(self):
        self.assertEqual(q.normalize_stream_quality("bogus"), "original")

    def test_none_falls_back_to_original(self):
        self.assertEqual(q.normalize_stream_quality(None), "original")

    def test_stream_quality_label_roundtrip(self):
        self.assertEqual(q.stream_quality_label("2160"), "2160p 4K maximum")
        self.assertEqual(q.stream_quality_label("nonsense"), "🔝 Original / source maximum")


if __name__ == "__main__":
    unittest.main()
