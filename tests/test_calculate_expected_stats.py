import json
import tempfile
import unittest
from pathlib import Path

from pipelines.expected.calculate_expected_stats import (
    calculate_expected_stats,
    load_player_records,
)


class TestCalculateExpectedStats(unittest.TestCase):
    def test_calculates_per_game_stats_for_every_player(self):
        records = calculate_expected_stats(
            [
                {"player_name": "Player One", "games_played": 10, "goals": 20, "assists": 30, "shots": 50},
                {"player_name": "Player Two", "games_played": 0, "goals": 3, "assists": 2, "shots": 8},
            ]
        )

        self.assertAlmostEqual(records[0]["goals_per_game"], 2.0)
        self.assertAlmostEqual(records[0]["assists_per_game"], 3.0)
        self.assertAlmostEqual(records[0]["shots_per_game"], 5.0)
        self.assertEqual(records[1]["goals_per_game"], 0.0)
        self.assertEqual(len(records), 2)

    def test_loads_jsonl_records(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / "players.jsonl"
            input_path.write_text(json.dumps({"player_name": "Player One"}) + "\n", encoding="utf-8")

            self.assertEqual(load_player_records(input_path), [{"player_name": "Player One"}])


if __name__ == "__main__":
    unittest.main()