import importlib.util
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


GUI_PATH = Path(__file__).parents[1] / "streamlit" / "gui.py"
GUI_SPEC = importlib.util.spec_from_file_location("nhl_streamlit_gui", GUI_PATH)
if GUI_SPEC is None or GUI_SPEC.loader is None:
    raise RuntimeError(f"Unable to load Streamlit UI module from {GUI_PATH}")
GUI = importlib.util.module_from_spec(GUI_SPEC)
GUI_SPEC.loader.exec_module(GUI)


class TestStreamlitGui(unittest.TestCase):
    def test_game_stats_and_team_totals_come_from_gold_records(self):
        game_data = {
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
        gold_records = [
            {
                "player_id": 101,
                "player_name": "Away Skater",
                "position": "C",
                "sweater_number": 10,
                "goals": 2,
                "assists": 1,
                "shots": 5,
            },
            {
                "player_id": 102,
                "player_name": "Home Skater",
                "position": "D",
                "sweater_number": 20,
                "goals": 1,
                "assists": 3,
                "shots": 4,
            },
        ]

        summary = GUI.build_gold_game_summary(game_data, gold_records)

        self.assertEqual(
            [(team["abbreviation"], team["goals"], team["shots"]) for team in summary["teams"]],
            [("BUF", 2, 5), ("MTL", 1, 4)],
        )
        away_skater = next(player for player in summary["players"] if player["id"] == "101")
        self.assertEqual(
            (away_skater["goals"], away_skater["assists"], away_skater["shots"]),
            (2, 1, 5),
        )
        self.assertEqual(len(summary["players"]), 2)

    def test_query_filters_gold_rows_by_game_id(self):
        cursor = Mock()
        cursor.description = [("game_id",), ("player_id",), ("goals",)]
        cursor.fetchall.return_value = [(2025030214, 101, 2)]
        connection = Mock()
        connection.cursor.return_value = cursor

        records = GUI._query_gold_records(connection, 2025030214, "catalog.schema.nhl_events_gold")

        self.assertEqual(records, [{"game_id": 2025030214, "player_id": 101, "goals": 2}])
        query, parameters = cursor.execute.call_args.args
        self.assertIn("FROM `catalog`.`schema`.`nhl_events_gold`", query)
        self.assertIn("WHERE game_id = :game_id", query)
        self.assertEqual(parameters, {"game_id": 2025030214})
        cursor.close.assert_called_once_with()

    @patch("databricks.sql.connect")
    def test_waits_for_active_players_and_stable_gold_results(self, mock_connect):
        cursor = Mock()
        cursor.description = [
            ("game_id",),
            ("player_id",),
            ("player_name",),
            ("position",),
            ("sweater_number",),
            ("headshot",),
            ("goals",),
            ("assists",),
            ("shots",),
        ]
        cursor.fetchall.return_value = [
            (2025030214, 101, "Away Skater", "C", 10, "", 1, 0, 3)
        ]
        connection = Mock()
        connection.cursor.return_value = cursor
        mock_connect.return_value = connection

        records = GUI.wait_for_gold_records(
            2025030214,
            "catalog.schema.nhl_events_gold",
            "workspace.cloud.databricks.com",
            "/sql/warehouse",
            "test-token",
            {"101"},
            poll_interval_seconds=0,
        )

        self.assertEqual(records[0]["player_id"], 101)
        self.assertEqual(cursor.execute.call_count, 6)
        connection.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
