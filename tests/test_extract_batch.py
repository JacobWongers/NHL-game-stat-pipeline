import unittest

from pipelines.expected.hockey_reference.extract_batch import build_bigquery_table_id, extract_team_stats


class TestExtractBatch(unittest.TestCase):
    def test_builds_matchup_and_season_bigquery_table_name(self):
        table_id = build_bigquery_table_id("project.dataset.raw_skater_stats", "veg", "pit", 2025)

        self.assertEqual(table_id, "project.dataset.VEG_PIT_2025")

    def test_extracts_stats_for_two_distinct_teams(self):
        calls = []

        def fake_fetch(team_abbrev, season, playoffs=False):
            calls.append((team_abbrev, season, playoffs))
            return [{"player_name": team_abbrev, "games_played": 1, "goals": 2, "assists": 3, "shots": 4}]

        records = extract_team_stats("mtl", "buf", 2026, fetcher=fake_fetch)

        self.assertEqual([record["team_abbrev"] for record in records], ["MTL", "BUF"])
        self.assertEqual(calls, [("MTL", 2026, False), ("BUF", 2026, False)])

    def test_rejects_the_same_team_twice(self):
        with self.assertRaisesRegex(ValueError, "different teams"):
            extract_team_stats("MTL", "mtl", 2026)


if __name__ == "__main__":
    unittest.main()