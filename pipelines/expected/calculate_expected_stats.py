import argparse
import json
from pathlib import Path
from typing import Any


STAT_FIELDS = ("goals", "assists", "shots")


def load_player_records(input_path: Path) -> list[dict[str, Any]]:
    """Load player records from a JSON array/object or newline-delimited JSON file."""
    text = input_path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    if isinstance(document, list):
        return document
    if isinstance(document, dict):
        if all(isinstance(value, dict) for value in document.values()):
            return list(document.values())
        return [document]
    raise ValueError("Input JSON must contain an object, an array, or JSONL records.")


def calculate_expected_stats(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add expected goals, assists, and shots per game to every player record."""
    expected_stats = []
    for record in records:
        games_played = int(record.get("games_played", record.get("GP", 0)) or 0)
        games_played = max(games_played, 0)
        result = dict(record)
        result["games_played"] = games_played

        for stat in STAT_FIELDS:
            total = float(record.get(stat, record.get(stat[0].upper(), 0)) or 0)
            result[f"{stat}_per_game"] = total / games_played if games_played else 0.0

        expected_stats.append(result)
    return expected_stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Calculate expected per-game skater stats for the Streamlit UI.")
    parser.add_argument("input", type=Path, help="Input JSON or JSONL file containing player season stats")
    parser.add_argument(
        "--output",
        type=Path,
        help="Output JSON path (defaults to <input stem>_expected.json)",
    )
    args = parser.parse_args()

    records = load_player_records(args.input)
    expected_stats = calculate_expected_stats(records)
    output_path = args.output or args.input.with_name(f"{args.input.stem}_expected.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(expected_stats, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Expected player stats -> {output_path} ({len(expected_stats)} players)")


if __name__ == "__main__":
    main()