import unittest
from unittest.mock import Mock

from google.cloud import bigquery

from pipelines.expected.load_to_bigquery import load_gcs_jsonl_to_bigquery


class TestLoadToBigquery(unittest.TestCase):
    def test_loads_gcs_jsonl_into_bigquery(self):
        client = Mock()
        job = Mock()
        job.output_rows = 64
        client.load_table_from_uri.return_value = job

        rows_loaded = load_gcs_jsonl_to_bigquery(
            "gs://nhl_streaming_expected/expected/hockey_reference/stats.jsonl",
            "project.dataset.raw_skater_stats",
            client=client,
        )

        self.assertEqual(rows_loaded, 64)
        client.load_table_from_uri.assert_called_once()
        source_uri, table_id = client.load_table_from_uri.call_args.args
        self.assertEqual(source_uri, "gs://nhl_streaming_expected/expected/hockey_reference/stats.jsonl")
        self.assertEqual(table_id, "project.dataset.raw_skater_stats")
        job.result.assert_called_once_with()

        load_config = client.load_table_from_uri.call_args.kwargs["job_config"]
        self.assertEqual(load_config.source_format, bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
        self.assertEqual(load_config.write_disposition, bigquery.WriteDisposition.WRITE_APPEND)
        self.assertEqual([field.name for field in load_config.schema], [
            "player_name",
            "player_url",
            "team_abbrev",
            "season",
            "games_played",
            "goals",
            "assists",
            "shots",
        ])

    def test_rejects_non_gcs_uri(self):
        with self.assertRaisesRegex(ValueError, "gs://"):
            load_gcs_jsonl_to_bigquery("https://example.com/stats.jsonl", "project.dataset.table")


if __name__ == "__main__":
    unittest.main()
