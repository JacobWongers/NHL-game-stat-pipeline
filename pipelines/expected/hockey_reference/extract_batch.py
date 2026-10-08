import argparse
import json
from pathlib import Path
from typing import Any, Callable

from pipelines.expected.load_to_bigquery import load_gcs_jsonl_to_bigquery
from pipelines.expected.upload_to_gcs import upload_file_to_gcs
from pipelines.expected.hockey_reference.fetch_expected_stats import fetch_team_stats


def extract_team_stats(
    team_one: str,
    team_two: str,
    season: int,
    playoffs: bool = False,
    fetcher: Callable[..., list[dict[str, Any]]] = fetch_team_stats,
) -> list[dict[str, Any]]:
    """Return normalized Hockey Reference records for two distinct teams."""
    team_abbrevs = [team_one.upper(), team_two.upper()]
    if team_abbrevs[0] == team_abbrevs[1]:
        raise ValueError("team_one and team_two must be different teams")

    records: list[dict[str, Any]] = []
    for team_abbrev in team_abbrevs:
        for player in fetcher(team_abbrev, season, playoffs=playoffs):
            records.append(
                {
                    "player_name": player["player_name"],
                    "player_url": player.get("player_url", ""),
                    "team_abbrev": team_abbrev,
                    "season": season,
                    "games_played": int(player.get("games_played", 0) or 0),
                    "goals": int(player.get("goals", 0) or 0),
                    "assists": int(player.get("assists", 0) or 0),
                    "shots": int(player.get("shots", 0) or 0),
                }
            )
    return records


def build_bigquery_table_id(base_table_id: str, team_one: str, team_two: str, season: int) -> str:
    """Keep the project and dataset while naming the table for the matchup and season."""
    table_parts = base_table_id.split(".")
    if len(table_parts) != 3 or not all(table_parts):
        raise ValueError("bigquery table must be qualified as project.dataset.table")

    table_name = f"{team_one.upper()}_{team_two.upper()}_{season}"
    return f"{table_parts[0]}.{table_parts[1]}.{table_name}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract normalized Hockey Reference skater stats as JSONL.")
    parser.add_argument("--season", type=int, required=True, help="Season ending year, such as 2024")
    parser.add_argument(
        "--teams",
        nargs=2,
        required=True,
        metavar=("TEAM1", "TEAM2"),
        help="Two different NHL team abbreviations, such as EDM CGY",
    )
    parser.add_argument("--playoffs", action="store_true", help="Extract playoff stats instead of regular-season stats")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/expected"),
        help="Directory for the generated JSONL file",
    )
    parser.add_argument("--bucket", required=True, help="Google Cloud Storage bucket name")
    parser.add_argument(
        "--prefix",
        default="expected/hockey_reference",
        help="GCS object prefix; the output filename is appended",
    )
    parser.add_argument(
        "--bigquery-table",
        required=True,
        help="Qualified BigQuery table used for project.dataset, such as project.dataset.raw_skater_stats",
    )
    args = parser.parse_args()

    try:
        records = extract_team_stats(args.teams[0], args.teams[1], args.season, playoffs=args.playoffs)
    except ValueError as error:
        parser.error(str(error))

    team_one, team_two = (team.upper() for team in args.teams)
    bigquery_table_id = build_bigquery_table_id(args.bigquery_table, team_one, team_two, args.season)
    output_path = args.output_dir / f"{team_one}_{team_two}_{args.season}_SKATER_STATS.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as output_file:
        for record in records:
            output_file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    print(f"Hockey Reference records -> {output_path} ({len(records)} players)")
    gcs_uri = upload_file_to_gcs(output_path, args.bucket, args.prefix)
    print(f"Uploaded -> {gcs_uri}")
    rows_loaded = load_gcs_jsonl_to_bigquery(gcs_uri, bigquery_table_id)
    print(f"Loaded {rows_loaded} rows -> {bigquery_table_id}")


if __name__ == "__main__":
    main()