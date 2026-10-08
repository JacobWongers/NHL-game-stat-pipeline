import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from pipelines.expected.upload_to_gcs import upload_file_to_gcs


class TestUploadToGcs(unittest.TestCase):
    @patch("pipelines.expected.batch.upload_to_gcs.storage.Client")
    def test_uploads_file_to_expected_object(self, client_class):
        blob = Mock()
        client_class.return_value.bucket.return_value.blob.return_value = blob

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "stats.jsonl"
            file_path.write_text("{}\n", encoding="utf-8")

            uri = upload_file_to_gcs(file_path, "nhl_streaming_expected", "expected/hockey_reference")

        self.assertEqual(uri, "gs://nhl_streaming_expected/expected/hockey_reference/stats.jsonl")
        client_class.return_value.bucket.assert_called_once_with("nhl_streaming_expected")
        client_class.return_value.bucket.return_value.blob.assert_called_once_with(
            "expected/hockey_reference/stats.jsonl"
        )
        blob.upload_from_filename.assert_called_once_with(
            str(file_path), content_type="application/x-ndjson"
        )


if __name__ == "__main__":
    unittest.main()