# an NHL play-by-play response into one JSON row per play

import argparse
import json
from pathlib import Path
from typing import Any


def extract_plays(data: Any, game_id: int | None = None) -> list[dict[str, Any]]:
	"""Return the plays from an NHL API response as flat, Kafka-ready records."""

	#validate the response structure
	if not isinstance(data, dict):
		raise ValueError("The game data must be a JSON object.")

	plays = data.get("plays")

	#require a list of plays
	if not isinstance(plays, list):
		raise ValueError("The game data must contain a plays list.")

	roster_spots = data.get("rosterSpots", [])
	if not isinstance(roster_spots, list):
		raise ValueError("The game data must contain a rosterSpots list.")

	#use the provided game id when available
	resolved_game_id = game_id if game_id is not None else data.get("id")
	rows: list[dict[str, Any]] = []
	for play in plays:
		#validate each play before flattening it
		if not isinstance(play, dict):
			raise ValueError("Every play must be a JSON object.")

		#extract nested period information
		period = play.get("periodDescriptor") or {}
		rows.append(
			{
				"game_id": resolved_game_id,
				"event_id": play.get("eventId"),
				"period": period.get("number"),
				"period_type": period.get("periodType"),
				"time_in_period": play.get("timeInPeriod"),
				"time_remaining": play.get("timeRemaining"),
				"event_type": play.get("typeDescKey"),
				"details": play.get("details", {}),
				"roster_spots": roster_spots,
			}
		)

	return rows


def load_plays(input_path: str | Path) -> list[dict[str, Any]]:
	"""Load and flatten an NHL game JSON file."""
	#load the raw game response
	path = Path(input_path)
	with path.open(encoding="utf-8") as input_file:
		data = json.load(input_file)

	#read the game id from the input filename
	game_id = path.stem.removeprefix("play_by_play_")
	try:
		parsed_game_id = int(game_id)
	except ValueError:
		parsed_game_id = None

	return extract_plays(data, parsed_game_id)


def save_extracted_plays(
	input_path: str | Path,
	output_path: str | Path,
) -> Path:
	"""Write flattened plays as newline-delimited JSON for Kafka ingestion."""
	#flatten the input response
	rows = load_plays(input_path)
	path = Path(output_path)
	#create missing output directories
	path.parent.mkdir(parents=True, exist_ok=True)
	with path.open("w", encoding="utf-8") as output_file:
		#write one compact json object per line
		for row in rows:
			output_file.write(json.dumps(row, separators=(",", ":")) + "\n")
	return path


def main() -> None:
	parser = argparse.ArgumentParser(description="Flatten NHL play-by-play JSON into JSON rows.")
	parser.add_argument("input_path", type=Path, help="NHL game JSON file to flatten")
	parser.add_argument(
		"-o",
		"--output",
		type=Path,
		help="Output JSONL path (default: data/extracted/<input filename>.jsonl)",
	)
	args = parser.parse_args()

	output_path = args.output or Path("data/extracted") / f"{args.input_path.stem}.jsonl"
	save_extracted_plays(args.input_path, output_path)
	print(f"Extracted plays -> {output_path}")


if __name__ == "__main__":
	main()
