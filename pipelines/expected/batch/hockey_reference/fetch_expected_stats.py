import argparse
import inspect
import json
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

try:
    from pipelines.actual.ingestion.fetch_game import extract_game_roster
except ImportError:  # pragma: no cover - for direct script execution
    from pipelines.actual.ingestion.fetch_game import extract_game_roster

TEAM_ID_TO_ABBREV = {
    1: "NJD",
    2: "NYI",
    3: "NYR",
    4: "PHI",
    5: "PIT",
    6: "BOS",
    7: "BUF",
    8: "MTL",
    9: "OTT",
    10: "TOR",
    11: "ATL",
    12: "CAR",
    13: "FLA",
    14: "TBL",
    15: "WSH",
    16: "CHI",
    17: "DET",
    18: "NSH",
    19: "STL",
    20: "CGY",
    21: "COL",
    22: "EDM",
    23: "VAN",
    24: "ANA",
    25: "DAL",
    26: "LAK",
    27: "SJS",
    28: "VEG",
    29: "CBJ",
    30: "MIN",
    31: "WPG",
    32: "ARI",
    33: "SEA",
    34: "NJD",
}


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def infer_season_from_game_data(game_data: dict[str, Any]) -> int:
    """Return the season ending year for the given NHL game.

    NHL season codes are in the form 20252026. For game dates in Oct-Dec,
    the season is the following year; for Jan-Sep, it is the current year.
    """
    raw_season = game_data.get("season")
    if isinstance(raw_season, int) and raw_season > 1000:
        season_code = str(raw_season)
        if len(season_code) == 8 and season_code[:4].isdigit() and season_code[4:].isdigit():
            return int(season_code[4:8])

    game_date = str(game_data.get("gameDate") or game_data.get("date") or "").strip()
    if not game_date:
        return 2026

    try:
        year, month, day = (int(part) for part in game_date.split("-")[:3])
    except ValueError:
        return 2026

    if month >= 10:
        return year + 1
    return year


def strip_html(text: str) -> str:
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.I)
    text = re.sub(r"<.*?>", " ", text, flags=re.S)
    text = html_entity_decode(text)
    return " ".join(text.split())


def html_entity_decode(text: str) -> str:
    replacements = {
        "&amp;": "&",
        "&nbsp;": " ",
        "&#39;": "'",
        "&quot;": '"',
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def _safe_float(value: Any) -> float:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def fetch_team_stats(team_abbrev: str, season: int, playoffs: bool = False) -> list[dict[str, Any]]:
    """Fetch and parse a Hockey Reference team page for one season."""
    url = f"https://www.hockey-reference.com/teams/{team_abbrev}/{season}.html"
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=20) as response:
        html = response.read().decode("utf-8", errors="replace")

    return parse_team_page_stats(html, playoffs=playoffs)


def parse_team_page_stats(html: str, playoffs: bool = False) -> list[dict[str, Any]]:
    """Extract per-player season stats from a Hockey Reference team page."""
    table_id = "player_stats_post" if playoffs else "player_stats"
    table_match = re.search(
        rf'<table\b[^>]*\bid=["\']{table_id}["\'][^>]*>.*?</table>',
        html,
        flags=re.S | re.I,
    )
    if table_match:
        html = table_match.group(0)

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.S | re.I)
    players: list[dict[str, Any]] = []
    header_positions: dict[str, int] = {}

    for row in rows:
        cell_matches = re.findall(r"(<t[dh][^>]*>.*?</t[dh]>)", row, flags=re.S | re.I)
        if not cell_matches:
            continue

        cleaned_cells = [strip_html(cell) for cell in cell_matches]
        if not cleaned_cells:
            continue

        is_header_row = re.search(r"<th\b[^>]*\bscope=[\"']col[\"']", row, flags=re.I) or all(
            re.match(r"<th\b", cell, flags=re.I) for cell in cell_matches
        )
        if is_header_row:
            header_positions = {}
            for index, cell in enumerate(cell_matches):
                data_stat_match = re.search(r'data-stat=["\']([^"\']+)', cell, flags=re.I)
                header_name = data_stat_match.group(1).lower() if data_stat_match else cleaned_cells[index].lower()
                header_positions[header_name] = index
            continue

        stats: dict[str, str] = {}
        for index, cell in enumerate(cell_matches):
            data_stat_match = re.search(r'data-stat=["\']([^"\']+)', cell, flags=re.I)
            if data_stat_match:
                stats[data_stat_match.group(1).lower()] = cleaned_cells[index]

        def get_stat(*names: str) -> str:
            for name in names:
                if name in stats:
                    return stats[name]
                if name in header_positions and header_positions[name] < len(cleaned_cells):
                    return cleaned_cells[header_positions[name]]
            return ""

        player_name = get_stat("player", "name_display") or cleaned_cells[0]
        if not player_name or player_name.lower() in {"player", "rk"}:
            continue

        if any(cell.lower() in {"gp", "g", "a", "s"} for cell in cleaned_cells[1:]):
            continue

        games_text = get_stat("games", "gp")
        goals_text = get_stat("goals", "g")
        assists_text = get_stat("assists", "a")
        shots_text = get_stat("shots", "s")
        if not all((games_text, goals_text, assists_text, shots_text)):
            continue

        player_url = ""
        href_match = re.search(r"<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", row, flags=re.S | re.I)
        if href_match:
            player_url = href_match.group(1)

        games_played = int(games_text.replace(",", "")) if games_text.replace(",", "").isdigit() else 0
        goals = int(goals_text.replace(",", "")) if goals_text.replace(",", "").isdigit() else 0
        assists = int(assists_text.replace(",", "")) if assists_text.replace(",", "").isdigit() else 0
        shots = int(shots_text.replace(",", "")) if shots_text.replace(",", "").isdigit() else 0

        player_stats = {
            "player_name": player_name,
            "player_url": player_url,
            "games_played": games_played,
            "goals": goals,
            "assists": assists,
            "shots": shots,
        }

        if games_played > 0:
            player_stats["goals_per_game"] = goals / games_played
            player_stats["assists_per_game"] = assists / games_played
            player_stats["shots_per_game"] = shots / games_played
        else:
            player_stats["goals_per_game"] = 0.0
            player_stats["assists_per_game"] = 0.0
            player_stats["shots_per_game"] = 0.0

        players.append(player_stats)

    return players


def build_expected_baseline_for_game(
    game_data: dict[str, Any],
    season: int | None = None,
    fetch_team_stats_fn: Callable[..., list[dict[str, Any]]] | None = None,
    fetch_team_stats: Callable[..., list[dict[str, Any]]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Build a per-skater expected baseline for a specific NHL game roster."""
    roster = extract_game_roster(game_data)
    resolved_fetcher = fetch_team_stats_fn or fetch_team_stats
    if resolved_fetcher is None:
        raise ValueError("A fetch_team_stats callback is required to build the expected baseline.")

    if season is None:
        season = infer_season_from_game_data(game_data)

    playoffs = str(game_data.get("gameType") or game_data.get("gameTypeId") or "") == "3"
    accepts_phase = "playoffs" in inspect.signature(resolved_fetcher).parameters

    baseline_by_player: dict[str, dict[str, Any]] = {}
    per_team_stats: dict[int, dict[str, dict[str, Any]]] = {}

    for skater in roster:
        team_id = skater["team_id"]
        if team_id not in per_team_stats:
            team_abbrev = TEAM_ID_TO_ABBREV.get(team_id)
            if not team_abbrev:
                continue
            if accepts_phase:
                rows = resolved_fetcher(team_abbrev, season, playoffs=playoffs)
            else:
                rows = resolved_fetcher(team_abbrev, season)
            per_team_stats[team_id] = {normalize_name(row["player_name"]): row for row in rows}

        team_rows = per_team_stats.get(team_id, {})
        player_lookup_key = normalize_name(skater["full_name"])
        row = team_rows.get(player_lookup_key)
        if row is None:
            baseline_by_player[skater["full_name"]] = {
                "player_id": skater["player_id"],
                "team_id": skater["team_id"],
                "team_abbrev": TEAM_ID_TO_ABBREV.get(skater["team_id"], ""),
                "games_played": 0,
                "goals_per_game": 0.0,
                "assists_per_game": 0.0,
                "shots_per_game": 0.0,
            }
            continue

        games_played = int(row.get("games_played", 0) or 0)
        if games_played <= 0:
            games_played = int(row.get("GP", 0) or 0)

        goals = float(row.get("goals", row.get("G", 0)) or 0)
        assists = float(row.get("assists", row.get("A", 0)) or 0)
        shots = float(row.get("shots", row.get("S", 0)) or 0)

        goals_per_game = goals / games_played if games_played else 0.0
        assists_per_game = assists / games_played if games_played else 0.0
        shots_per_game = shots / games_played if games_played else 0.0

        if "goals_per_game" in row and row["goals_per_game"] is not None:
            goals_per_game = float(row["goals_per_game"])
        if "assists_per_game" in row and row["assists_per_game"] is not None:
            assists_per_game = float(row["assists_per_game"])
        if "shots_per_game" in row and row["shots_per_game"] is not None:
            shots_per_game = float(row["shots_per_game"])

        baseline_by_player[skater["full_name"]] = {
            "player_id": skater["player_id"],
            "team_id": skater["team_id"],
            "team_abbrev": TEAM_ID_TO_ABBREV.get(skater["team_id"], ""),
            "games_played": games_played,
            "goals_per_game": goals_per_game,
            "assists_per_game": assists_per_game,
            "shots_per_game": shots_per_game,
        }

    return baseline_by_player


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a Hockey Reference expected baseline from an NHL game roster.")
    parser.add_argument("game_path", type=Path, help="Path to a raw NHL game JSON file")
    parser.add_argument("--season", type=int, default=None, help="Optional season override. If omitted, it is inferred from the game date/season code.")
    parser.add_argument("--output", type=Path, help="Optional output path for the JSON baseline")
    args = parser.parse_args()

    with args.game_path.open("r", encoding="utf-8") as input_file:
        game_data = json.load(input_file)

    baseline = build_expected_baseline_for_game(game_data, season=args.season)
    output_path = args.output or Path("data/expected") / f"baseline_{args.game_path.stem}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(baseline, output_file, indent=2, sort_keys=True)

    print(f"Expected baseline -> {output_path} ({len(baseline)} players)")


if __name__ == "__main__":
    main()
