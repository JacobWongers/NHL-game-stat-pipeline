import importlib.util
import tempfile
import unittest
from pathlib import Path


GUI_PATH = Path(__file__).parents[1] / "streamlit" / "gui.py"
GUI_SPEC = importlib.util.spec_from_file_location("nhl_streamlit_gui", GUI_PATH)
if GUI_SPEC is None or GUI_SPEC.loader is None:
    raise RuntimeError(f"Unable to load Streamlit UI module from {GUI_PATH}")
GUI = importlib.util.module_from_spec(GUI_SPEC)
GUI_SPEC.loader.exec_module(GUI)


class TestStreamlitGui(unittest.TestCase):
    def setUp(self):
        self.game_data = {
            "awayTeam": {"id": 7, "abbrev": "BUF"},
            "homeTeam": {"id": 8, "abbrev": "MTL"},
            "rosterSpots": [
                {
                    "playerId": 101,
                    "teamId": 7,
                    "firstName": {"default": "Away"},
                    "lastName": {"default": "Skater"},
                    "positionCode": "C",
                    "sweaterNumber": 10,
                    "headshot": "https://assets.nhle.com/mugs/BUF/101.png",
                },
                {
                    "playerId": 102,
                    "teamId": 8,
                    "firstName": {"default": "Home"},
                    "lastName": {"default": "Skater"},
                    "positionCode": "D",
                    "sweaterNumber": 20,
                },
                {
                    "playerId": 103,
                    "teamId": 8,
                    "firstName": {"default": "Home"},
                    "lastName": {"default": "Goalie"},
                    "positionCode": "G",
                },
            ],
        }
        self.events = [
            {
                "game_id": 2025030214,
                "event_type": "goal",
                "details": {
                    "scoringPlayerId": 101,
                    "assist1PlayerId": 102,
                    "assist2PlayerId": None,
                },
            },
            {
                "game_id": 2025030214,
                "event_type": "shot-on-goal",
                "details": {"shootingPlayerId": 101},
            },
            {
                "game_id": 2025030214,
                "event_type": "goal",
                "details": {
                    "scoringPlayerId": 102,
                    "assist1PlayerId": None,
                    "assist2PlayerId": None,
                },
            },
        ]

    def test_actual_stats_are_aggregated_from_extracted_events(self):
        summary = GUI.build_game_summary(self.game_data, self.events)

        self.assertEqual(
            [(team["abbreviation"], team["goals"], team["shots"]) for team in summary["teams"]],
            [("BUF", 1, 2), ("MTL", 1, 1)],
        )
        players = {player["id"]: player for player in summary["players"]}
        self.assertEqual(
            (players["101"]["goals"], players["101"]["assists"], players["101"]["shots"]),
            (1, 0, 2),
        )
        self.assertEqual(
            (players["102"]["goals"], players["102"]["assists"], players["102"]["shots"]),
            (1, 1, 1),
        )
        self.assertNotIn("103", players)

    def test_loads_events_from_saved_extracted_jsonl(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            previous_dir = GUI.EXTRACTED_DIR
            GUI.EXTRACTED_DIR = Path(temp_dir)
            try:
                path = GUI.save_event_rows(2025030214, self.events)
                loaded = GUI.load_event_rows(path, 2025030214)
            finally:
                GUI.EXTRACTED_DIR = previous_dir

        self.assertEqual(loaded, self.events)

    def test_rejects_extracted_events_for_a_different_game_id(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "events.jsonl"
            path.write_text('{"game_id":2025030212}\n', encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "game 2025030214"):
                GUI.load_event_rows(path, 2025030214)

    def test_rejects_an_empty_extracted_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "events.jsonl"
            path.write_text("\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "contains no play-by-play events"):
                GUI.load_event_rows(path, 2025030214)


if __name__ == "__main__":
    unittest.main()
