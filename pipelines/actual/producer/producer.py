import argparse
import json
import time
from pathlib import Path
from typing import Any, Callable

from confluent_kafka import Producer


GAME_PERIOD_SECONDS = 20 * 60
GAME_SPEEDUP = GAME_PERIOD_SECONDS / 5


def game_time_seconds(event: dict[str, Any]) -> int:
	"""Return the event's position in the game clock."""
	try:
		minutes, seconds = str(event["time_in_period"]).split(":", 1)
		period = int(event["period"])
		minutes = int(minutes)
		seconds = int(seconds)
	except (KeyError, TypeError, ValueError) as error:
		event_id = event.get("event_id", "unknown")
		raise ValueError(f"Event {event_id} has an unusable game clock: {error}") from error

	if period < 1:
		raise ValueError(f"Event {event.get('event_id', 'unknown')} has an invalid period: {period}")

	return (period - 1) * GAME_PERIOD_SECONDS + minutes * 60 + seconds


def load_events(input_path: str | Path, game_id: int) -> list[dict[str, Any]]:
	"""Load JSONL events and ensure they belong to the requested game."""
	events = []
	with Path(input_path).open(encoding="utf-8") as input_file:
		for line_number, line in enumerate(input_file, 1):
			if not line.strip():
				continue
			event = json.loads(line)
			if event.get("game_id") != game_id:
				raise ValueError(f"Line {line_number} does not belong to game {game_id}.")
			events.append(event)
	return events


def load_properties(properties_path: str | Path) -> dict[str, str]:
	"""Load Kafka client settings from a Java-style properties file."""
	properties: dict[str, str] = {}
	with Path(properties_path).open(encoding="utf-8") as properties_file:
		for line_number, line in enumerate(properties_file, 1):
			line = line.strip()
			if not line or line.startswith("#"):
				continue
			if "=" not in line:
				raise ValueError(f"Invalid property on line {line_number}: {line}")
			key, value = line.split("=", 1)
			properties[key.strip()] = value.strip()
	return properties


def stream_events(
	events: list[dict[str, Any]],
	topic: str,
	properties_path: str | Path = "kafka.properties",
	producer: Producer | None = None,
	sleep: Callable[[float], None] = time.sleep,
) -> None:
	"""Publish events at a pace of five seconds per 20-minute period."""
	kafka_producer = producer or Producer(load_properties(properties_path))
	previous_time = None
	for event in events:
		current_time = game_time_seconds(event)
		if previous_time is not None:
			sleep(max(0, current_time - previous_time) / GAME_SPEEDUP)
		kafka_producer.produce(
			topic,
			key=str(event["game_id"]),
			value=json.dumps(event),
		)
		kafka_producer.poll(0)
		previous_time = current_time
	kafka_producer.flush()


def main() -> None:
	parser = argparse.ArgumentParser(description="Replay extracted NHL events to Kafka.")
	parser.add_argument("input_path", type=Path)
	parser.add_argument("game_id", type=int)
	parser.add_argument("topic")
	parser.add_argument(
		"--properties",
		type=Path,
		default=Path("kafka.properties"),
		help="Kafka client properties file",
	)
	args = parser.parse_args()

	events = load_events(args.input_path, args.game_id)
	stream_events(events, args.topic, args.properties)


if __name__ == "__main__":
	main()