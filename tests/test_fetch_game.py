import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipelines.actual.ingestion.fetch_game import fetch_play_by_play, save_game


class TestFetchGame(unittest.TestCase):
	def mock_response(self, payload):
		response = io.BytesIO(json.dumps(payload).encode())
		context = patch("pipelines.actual.ingestion.fetch_game.urllib.request.urlopen")
		mock_urlopen = context.start()
		mock_urlopen.return_value.__enter__.return_value = response
		self.addCleanup(context.stop)
		return mock_urlopen

	def test_fetch_play_by_play_returns_api_data(self):
		payload = {"plays": [{"eventId": 1}]}
		mock_urlopen = self.mock_response(payload)

		data, game_id = fetch_play_by_play(2025030213)

		self.assertEqual(data, payload)
		self.assertEqual(game_id, 2025030213)
		mock_urlopen.assert_called_once()

	@patch("pipelines.actual.ingestion.fetch_game.input", return_value="2025030213")
	def test_invalid_game_id_can_be_retried(self, _mock_input):
		payload = {"plays": []}
		self.mock_response(payload)

		data, game_id = fetch_play_by_play(0)

		self.assertEqual(data, payload)
		self.assertEqual(game_id, 2025030213)

	@patch("pipelines.actual.ingestion.fetch_game.input", return_value="2025030213")
	def test_non_integer_game_id_can_be_retried(self, _mock_input):
		payload = {"plays": []}
		self.mock_response(payload)

		data, game_id = fetch_play_by_play("fffff")

		self.assertEqual(data, payload)
		self.assertEqual(game_id, 2025030213)

	@patch("pipelines.actual.ingestion.fetch_game.fetch_play_by_play")
	def test_save_game_writes_jsonl_without_saving_json(self, mock_fetch):
		payload = {"plays": [{"eventId": 1, "typeDescKey": "goal"}]}
		mock_fetch.return_value = (payload, 2025030213)

		with tempfile.TemporaryDirectory() as temp_dir:
			output_path = save_game(2025030213, temp_dir)

			self.assertEqual(output_path, Path(temp_dir) / "play_by_play_2025030213.jsonl")
			self.assertFalse(output_path.with_suffix(".json").exists())
			rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
			self.assertEqual(len(rows), 1)
			self.assertEqual(rows[0]["game_id"], 2025030213)
			self.assertEqual(rows[0]["event_id"], 1)
			self.assertEqual(rows[0]["event_type"], "goal")


if __name__ == "__main__":
	unittest.main()
