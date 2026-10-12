import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from pipelines.actual.producer.producer import game_time_seconds, load_events, stream_events


class TestProducer(unittest.TestCase):
	def test_stream_events_builds_producer_from_supplied_config(self):
		kafka_config = {
			"bootstrap.servers": "broker.example:9092",
			"security.protocol": "SASL_SSL",
			"sasl.mechanisms": "PLAIN",
			"sasl.username": "test-key",
			"sasl.password": "test-secret",
		}

		with patch("pipelines.actual.producer.producer.Producer") as producer_class:
			stream_events(
				[{"game_id": 99, "event_id": 1, "period": 1, "time_in_period": "00:00"}],
				"plays",
				kafka_config=kafka_config,
			)

		producer_class.assert_called_once_with(kafka_config)

	def test_streams_events_with_scaled_game_time_gaps(self):
		events = [
			{"game_id": 99, "event_id": 1, "period": 1, "time_in_period": "00:00"},
			{"game_id": 99, "event_id": 2, "period": 1, "time_in_period": "00:10"},
			{"game_id": 99, "event_id": 3, "period": 1, "time_in_period": "20:00"},
			{"game_id": 99, "event_id": 4, "period": 2, "time_in_period": "00:00"},
		]
		producer = Mock()
		sleep = Mock()

		stream_events(events, "plays", producer=producer, sleep=sleep)

		self.assertEqual([call.args[0] for call in producer.produce.call_args_list], ["plays"] * 4)
		delays = [call.args[0] for call in sleep.call_args_list]
		self.assertAlmostEqual(sum(delays), 5)
		self.assertEqual(delays[-1], 0)
		self.assertEqual(producer.flush.call_count, 1)

	def test_game_clock_offsets_each_period_by_20_minutes(self):
		self.assertEqual(
			game_time_seconds({"period": 2, "time_in_period": "00:00"}),
			20 * 60,
		)
		self.assertEqual(
			game_time_seconds({"period": 3, "time_in_period": "00:00"}),
			40 * 60,
		)

	def test_load_events_rejects_a_different_game_id(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			path = Path(temp_dir) / "events.jsonl"
			path.write_text(json.dumps({"game_id": 1}) + "\n", encoding="utf-8")

			with self.assertRaisesRegex(ValueError, "game 2"):
				load_events(path, 2)


if __name__ == "__main__":
	unittest.main()