import json
import tempfile
import unittest
from pathlib import Path

from pipelines.actual.ingestion.fetch_game import extract_game_roster
from pipelines.expected.hockey_reference.fetch_expected_stats import (
    build_expected_baseline_for_game,
    parse_team_page_stats,
)


class TestHockeyReferenceFetch(unittest.TestCase):
    def test_extract_game_roster_filters_skaters(self):
        payload = {
            "rosterSpots": [
                {"playerId": 1, "firstName": {"default": "Connor"}, "lastName": {"default": "McDavid"}, "teamId": 22, "positionCode": "C"},
                {"playerId": 2, "firstName": {"default": "Goalie"}, "lastName": {"default": "Name"}, "teamId": 22, "positionCode": "G"},
                {"playerId": 3, "firstName": {"default": "Sid"}, "lastName": {"default": "Crosby"}, "teamId": 5, "positionCode": "C"},
            ]
        }

        roster = extract_game_roster(payload)

        self.assertEqual(len(roster), 2)
        self.assertEqual(roster[0]["player_id"], 1)
        self.assertEqual(roster[0]["team_id"], 22)
        self.assertEqual(roster[1]["player_id"], 3)

    def test_parse_team_page_stats_extracts_per_game_averages(self):
        html = """
        <html><body>
        <table>
        <thead><tr><th>Player</th><th>GP</th><th>G</th><th>A</th><th>S</th></tr></thead>
        <tbody>
        <tr><td><a href="/players/m/mcdavco01.html">Connor McDavid</a></td><td>39</td><td>20</td><td>35</td><td>200</td></tr>
        <tr><td><a href="/players/e/ericjov01.html">J. Another</a></td><td>50</td><td>25</td><td>30</td><td>210</td></tr>
        </tbody>
        </table>
        </body></html>
        """

        players = parse_team_page_stats(html)

        self.assertEqual(players[0]["player_name"], "Connor McDavid")
        self.assertAlmostEqual(players[0]["goals_per_game"], 20 / 39)
        self.assertAlmostEqual(players[0]["assists_per_game"], 35 / 39)
        self.assertAlmostEqual(players[0]["shots_per_game"], 200 / 39)

    def test_build_expected_baseline_for_game_uses_game_roster(self):
        payload = {
            "rosterSpots": [
                {"playerId": 1, "firstName": {"default": "Connor"}, "lastName": {"default": "McDavid"}, "teamId": 22, "positionCode": "C"},
                {"playerId": 2, "firstName": {"default": "Goalie"}, "lastName": {"default": "Name"}, "teamId": 22, "positionCode": "G"},
                {"playerId": 3, "firstName": {"default": "Sid"}, "lastName": {"default": "Crosby"}, "teamId": 5, "positionCode": "C"},
            ]
        }

        def fake_fetch(team_abbrev, season):
            self.assertIn(team_abbrev, {"EDM", "PIT"})
            self.assertEqual(season, 2024)
            return [
                {"player_name": "Connor McDavid", "goals": 20, "assists": 35, "shots": 200, "games_played": 39},
                {"player_name": "Sid Crosby", "goals": 18, "assists": 32, "shots": 180, "games_played": 45},
            ]

        baseline = build_expected_baseline_for_game(payload, season=2024, fetch_team_stats=fake_fetch)

        self.assertEqual(baseline["Connor McDavid"]["team_abbrev"], "EDM")
        self.assertAlmostEqual(baseline["Connor McDavid"]["goals_per_game"], 20 / 39)
        self.assertEqual(baseline["Sid Crosby"]["team_abbrev"], "PIT")


if __name__ == "__main__":
    unittest.main()
