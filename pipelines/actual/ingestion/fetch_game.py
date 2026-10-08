import json
import urllib.request
import urllib.error
import argparse
from pathlib import Path

try:
    from .extract_plays import extract_plays
except ImportError:
    from extract_plays import extract_plays

NHL_API_BASE = "https://api-web.nhle.com/v1/gamecenter"
DEFAULT_OUT_DIR = Path(__file__).resolve().parents[3] / "data" / "extracted"


def extract_game_roster(game_data: dict) -> list[dict]:
    """Return skaters from a game roster, excluding goalies and non-player entries."""
    if not isinstance(game_data, dict):
        raise ValueError("The game data must be a JSON object.")

    roster = game_data.get("rosterSpots", [])
    if not isinstance(roster, list):
        raise ValueError("The game data must contain a rosterSpots list.")

    skaters: list[dict] = []
    for player in roster:
        if not isinstance(player, dict):
            continue

        position_code = str(player.get("positionCode") or player.get("position") or "").upper()
        if position_code == "G":
            continue

        first_name = player.get("firstName")
        last_name = player.get("lastName")
        if isinstance(first_name, dict):
            first_name = first_name.get("default") or ""
        if isinstance(last_name, dict):
            last_name = last_name.get("default") or ""

        full_name = " ".join(part for part in [str(first_name or "").strip(), str(last_name or "").strip()] if part)
        if not full_name:
            continue

        skaters.append(
            {
                "player_id": player.get("playerId"),
                "full_name": full_name,
                "team_id": player.get("teamId"),
                "position_code": position_code,
            }
        )

    return skaters


#fetches the play by play information using the unique game id
def fetch_play_by_play(game_id: int | str) -> tuple[dict, int]:
    while True:
        try:
            game_id = int(game_id)
            if game_id <= 0:
                raise ValueError("Game ID must be a positive integer.")

            url = f"{NHL_API_BASE}/{game_id}/play-by-play"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}) #http request for the NHL API
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.load(resp)

            if not isinstance(data, dict) or "plays" not in data:
                raise ValueError("No game data was returned for that ID.")

            return data, game_id

        except (urllib.error.HTTPError, urllib.error.URLError, ValueError, json.JSONDecodeError) as e:
            print(f"Invalid or unavailable NHL game ID: {e}")
            retry = input("Enter a valid NHL game ID to try again, or press Enter to exit: ").strip()

            #if the user presses Enter
            if not retry:
                raise SystemExit("Exiting...")

            try:
                game_id = int(retry)
            except ValueError:
                print("not a valid integer game ID.")
                continue


#saves the data as newline-delimited JSON
def save_game(game_id: int, out_dir: str | Path = DEFAULT_OUT_DIR) -> Path:
    data, valid_game_id = fetch_play_by_play(game_id)
    out_path = Path(out_dir) / f"play_by_play_{valid_game_id}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = extract_plays(data, game_id=valid_game_id)
    with out_path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            output_file.write(json.dumps(row, separators=(",", ":")) + "\n")
    print(f"Saved game {valid_game_id} -> {out_path} ({len(rows)} events)")
    return out_path
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("game_id", help="NHL game ID, e.g. 2025030213")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    save_game(args.game_id, args.out_dir)