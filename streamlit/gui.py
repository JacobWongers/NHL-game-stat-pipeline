import html
import json
import re
import sys
import unicodedata
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse
from typing import Any

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.actual.ingestion.extract_plays import extract_plays
from pipelines.actual.ingestion.fetch_game import fetch_play_by_play
from pipelines.expected.calculate_expected_stats import calculate_expected_stats
from pipelines.expected.hockey_reference.extract_batch import extract_team_stats
from pipelines.expected.hockey_reference.fetch_expected_stats import (
    TEAM_ID_TO_ABBREV,
    infer_season_from_game_data,
)


EXTRACTED_DIR = PROJECT_ROOT / "data" / "extracted"
EXPECTED_DIR = PROJECT_ROOT / "data" / "expected"


def _localized_name(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("default", "")
    return str(value or "")


def _normalize_name(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_name = decomposed.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", ascii_name.casefold())


def build_game_summary(
    game_data: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    roster = game_data.get("rosterSpots")
    if not isinstance(roster, list):
        raise ValueError("The NHL game response does not include a roster.")
    players: dict[str, dict[str, Any]] = {}
    team_by_id: dict[str, dict[str, Any]] = {}
    game_teams: list[dict[str, Any]] = []

    for side in ("away", "home"):
        team_data = game_data.get(f"{side}Team")
        if not isinstance(team_data, dict):
            raise ValueError(f"The NHL game response does not include the {side} team.")
        team_id = str(team_data.get("id") or team_data.get("teamId") or "")
        abbreviation = str(team_data.get("abbrev") or "").upper()
        if not team_id or not abbreviation:
            raise ValueError(f"The NHL game response has incomplete {side} team details.")
        team = {
            "id": team_id,
            "abbreviation": abbreviation,
            "logo_url": f"https://assets.nhle.com/logos/nhl/svg/{abbreviation}_light.svg",
            "side": side,
            "goals": 0,
            "shots": 0,
        }
        game_teams.append(team)
        team_by_id[team_id] = team

    for roster_entry in roster:
        if not isinstance(roster_entry, dict):
            continue
        player_id = roster_entry.get("playerId")
        team_id = roster_entry.get("teamId")
        if player_id is None or team_id is None:
            continue

        team_key = str(team_id)
        headshot_url = str(roster_entry.get("headshot") or "")
        first_name = _localized_name(roster_entry.get("firstName"))
        last_name = _localized_name(roster_entry.get("lastName"))
        players[str(player_id)] = {
            "id": str(player_id),
            "team_id": team_key,
            "name": " ".join(part for part in (first_name, last_name) if part).strip(),
            "position": str(roster_entry.get("positionCode") or ""),
            "number": roster_entry.get("sweaterNumber"),
            "headshot_url": headshot_url,
            "goals": 0,
            "assists": 0,
            "shots": 0,
        }

    def add_stat(player_id: Any, stat: str) -> None:
        if player_id is None:
            return
        player = players.get(str(player_id))
        if player is not None:
            player[stat] += 1

    for event in events:
        details = event.get("details")
        if not isinstance(details, dict):
            continue
        event_type = event.get("event_type")
        if event_type == "goal":
            scorer_id = details.get("scoringPlayerId")
            add_stat(scorer_id, "goals")
            add_stat(scorer_id, "shots")
            add_stat(details.get("assist1PlayerId"), "assists")
            add_stat(details.get("assist2PlayerId"), "assists")
        elif event_type == "shot-on-goal":
            add_stat(details.get("shootingPlayerId"), "shots")

    for player in players.values():
        if player["position"] == "G":
            continue
        team = team_by_id.get(player["team_id"])
        if team is not None:
            team["goals"] += player["goals"]
            team["shots"] += player["shots"]

    skaters = [
        player for player in players.values()
        if player["position"] != "G" and player["name"]
    ]
    skaters.sort(key=lambda player: (player["name"].casefold(), player["team_id"]))
    return {
        "teams": game_teams,
        "players": skaters,
    }


def save_event_rows(game_id: int, events: list[dict[str, Any]]) -> Path:
    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
    path = EXTRACTED_DIR / f"play_by_play_{game_id}.jsonl"
    path.write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )
    return path


def load_event_rows(path: str | Path, game_id: int) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with Path(path).open(encoding="utf-8") as event_file:
        for line_number, line in enumerate(event_file, 1):
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get("game_id") != game_id:
                raise ValueError(
                    f"Extracted event on line {line_number} does not belong to game {game_id}."
                )
            events.append(event)
    if not events:
        raise ValueError(f"Extracted game {game_id} contains no play-by-play events.")
    return events


def save_expected_records(
    teams: list[str],
    season: int,
    records: list[dict[str, Any]],
) -> Path:
    EXPECTED_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPECTED_DIR / f"{teams[0]}_{teams[1]}_{season}_SKATER_STATS.jsonl"
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    return path


def find_season_stats(
    player: dict[str, Any],
    season: int | None,
    records: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if season is None:
        return None
    player_name = _normalize_name(player["name"])
    for record in records:
        if (
            record.get("team_abbrev") == player.get("team_abbreviation")
            and int(record.get("season", -1)) == season
            and _normalize_name(str(record.get("player_name") or "")) == player_name
        ):
            return record
    return None


def _show_team_logo(team: dict[str, Any], width: int = 148) -> None:
    logo_url = team["logo_url"]
    if logo_url:
        st.markdown(
            f'<img class="team-logo" src="{html.escape(logo_url, quote=True)}" '
            f'alt="{html.escape(team["abbreviation"], quote=True)} logo" '
            f'width="{width}" height="{width}">',
            unsafe_allow_html=True,
        )
    else:
        st.markdown('<div class="team-logo-placeholder">🏒</div>', unsafe_allow_html=True)


def _show_headshot(player: dict[str, Any]) -> None:
    url = player.get("headshot_url", "")
    if urlparse(url).hostname == "assets.nhle.com":
        st.markdown(
            f'<img class="player-headshot" src="{html.escape(url, quote=True)}" '
            f'alt="{html.escape(player["name"], quote=True)}">',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="player-headshot-placeholder">🏒</div>',
            unsafe_allow_html=True,
        )


def _season_value(record: dict[str, Any] | None, stat: str) -> int | float | str:
    if record is None:
        return "—"
    value = record.get(stat, 0) or 0
    return int(value) if float(value).is_integer() else float(value)


def _per_game_value(record: dict[str, Any] | None, stat: str) -> str:
    if record is None:
        return "—"
    return f"{float(record.get(f'{stat}_per_game', 0) or 0):.2f}"


def main() -> None:
    st.set_page_config(
        page_title="NHL Game Center",
        page_icon="🏒",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@500;600;700&family=Inter:wght@400;500;600;700&display=swap');
        :root { color-scheme: dark; }
        .stApp { background: #0b1724; color: #f5f8fc; }
        .block-container { max-width: 1480px; padding: 1.25rem 1.5rem 1.5rem; }
        [data-testid="stHeader"] { background: transparent; }
        [data-testid="stSelectbox"] label, [data-testid="stMetricLabel"] {
            color: #c1ccd7 !important;
        }
        h1, h2, h3, p, label, span, div { font-family: Inter, sans-serif; }
        .eyebrow { color: #65d5c3; font-size: 1rem; font-weight: 700;
            letter-spacing: .15em; text-transform: uppercase; margin-bottom: .25rem; }
        .page-title { color: #f5f8fc; font-family: 'Barlow Condensed', sans-serif;
            font-size: 3.1rem; font-weight: 700; letter-spacing: .025em;
            line-height: 1; margin: 0; text-transform: uppercase; }
        .page-subtitle { color: #d1dbe4; font-size: 1.15rem; margin-top: .4rem; }
        .panel-title { color: #f5f8fc; font-family: 'Barlow Condensed', sans-serif;
            font-size: 1.7rem; font-weight: 600; letter-spacing: .06em;
            margin: 0 0 .4rem; text-transform: uppercase; }
        .team-logo { object-fit: contain; display: block; margin: 0 auto .25rem; }
        .team-logo-placeholder, .player-headshot-placeholder {
            display: grid; place-items: center; color: #65d5c3;
            background: #15283a; border: 1px solid #334a5e; border-radius: 50%;
            margin: 0 auto .25rem;
        }
        .team-logo-placeholder { width: 148px; height: 148px; font-size: 3rem; }
        .player-headshot { width: 240px; height: 240px; object-fit: cover;
            border-radius: 50%; border: 2px solid #65d5c3; background: #15283a;
            display: block; margin: 0 auto .3rem; }
        .player-headshot-placeholder { width: 240px; height: 240px; font-size: 4rem; }
        .team-abbr { color: #f5f8fc; font-family: 'Barlow Condensed', sans-serif;
            font-size: 2rem; font-weight: 700; letter-spacing: .06em;
            text-align: center; }
        .team-side { color: #d1dbe4; font-size: 1rem; letter-spacing: .1em;
            text-align: center; text-transform: uppercase; }
        .score { color: white; font-family: 'Barlow Condensed', sans-serif;
            font-size: 4rem; font-weight: 700; line-height: 1; text-align: center; }
        .score-divider { color: #b5c2cf; font-size: 1.35rem; text-align: center; }
        .stat-label { color: #e0e7ed; font-size: 1.1rem; }
        .profile-name { color: #f5f8fc; font-family: 'Barlow Condensed', sans-serif;
            font-size: 2.4rem; font-weight: 700; line-height: 1.05; text-align: center; }
        .profile-team { color: #65d5c3; font-size: 1.1rem; font-weight: 700;
            letter-spacing: .13em; text-align: center; text-transform: uppercase; }
        .section-caption { color: #d1dbe4; font-size: 1rem; margin: .05rem 0 .35rem; }
        .stMetric { background: #142435; border: 1px solid #263b4f;
            border-radius: 10px; padding: .35rem .5rem; }
        .stMetric [data-testid="stMetricValue"] {
            color: #f5f8fc; font-size: 2.3rem; line-height: 1.1;
        }
        .stMetric [data-testid="stMetricLabel"] { color: #d1dbe4; font-size: 1.15rem; }
        [data-testid="stSelectbox"] { font-size: 1.2rem; }
        [data-testid="stVerticalBlockBorderWrapper"] { background: #0f2030;
            border-color: #2b4054 !important; border-radius: 14px; }
        [data-testid="stSelectbox"] div[data-baseweb="select"] > div {
            background: #142435; border-color: #334a5e; border-radius: 8px;
        }
        [data-testid="stAlert"] { border-radius: 10px; font-size: 1.05rem; }
        hr { border-color: #263b4f; }
        @media (max-width: 800px) {
            .block-container { padding: 1.2rem 1rem 2rem; }
            .page-title { font-size: 2.6rem; }
            .player-headshot, .player-headshot-placeholder {
                width: 200px; height: 200px;
            }
            .team-logo { width: 120px; height: 120px; }
            .team-logo-placeholder { width: 120px; height: 120px; }
            .stMetric [data-testid="stMetricValue"] { font-size: 2rem; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    header_left, header_right = st.columns([1.3, 1])
    with header_left:
        st.markdown('<div class="eyebrow">Hockey analytics</div>', unsafe_allow_html=True)
        st.markdown('<h1 class="page-title">NHL Game Center</h1>', unsafe_allow_html=True)
        st.markdown(
            '<div class="page-subtitle">Enter a game ID to view actual game stats and regular-season averages.</div>',
            unsafe_allow_html=True,
        )

    with st.form("game_id_search"):
        game_id_text = st.text_input(
            "NHL game ID",
            placeholder="Enter a 10-digit NHL game ID",
            max_chars=10,
            help="Submitting a valid ID fetches and extracts NHL play-by-play, then calculates stats locally.",
        )
        submitted = st.form_submit_button("Load game stats", type="primary")

    if submitted:
        st.session_state.pop("game_result", None)
        if not re.fullmatch(r"\d{10}", game_id_text.strip()):
            st.error("Enter a valid 10-digit NHL game ID.")
        else:
            game_id = int(game_id_text.strip())
            status: Any = None
            try:
                with st.status("Loading game stats...", expanded=True) as status:
                    status.update(label=f"Fetching NHL game {game_id} and extracting plays...")
                    game_data, valid_game_id = fetch_play_by_play(
                        game_id,
                        retry_on_error=False,
                    )
                    events = extract_plays(game_data, game_id=valid_game_id)
                    if not events:
                        raise ValueError(f"NHL game {game_id} has no play-by-play events.")
                    status.update(
                        label=f"Saving and reading {len(events)} extracted events..."
                    )
                    event_path = save_event_rows(valid_game_id, events)
                    events = load_event_rows(event_path, valid_game_id)
                    summary = build_game_summary(game_data, events)

                    season = infer_season_from_game_data(game_data)
                    away_data = game_data["awayTeam"]
                    home_data = game_data["homeTeam"]
                    team_abbreviations = [
                        str(away_data.get("abbrev") or "").upper(),
                        str(home_data.get("abbrev") or "").upper(),
                    ]
                    for index, team_data in enumerate((away_data, home_data)):
                        if not team_abbreviations[index]:
                            team_id = int(team_data.get("id") or team_data.get("teamId") or 0)
                            team_abbreviations[index] = TEAM_ID_TO_ABBREV.get(team_id, "")
                    if not all(team_abbreviations):
                        raise ValueError("Could not determine both NHL team abbreviations.")

                    status.update(
                        label=(
                            f"Fetching {season - 1}-{str(season)[-2:]} expected stats for "
                            f"{team_abbreviations[0]} and {team_abbreviations[1]}..."
                        )
                    )
                    raw_expected = extract_team_stats(
                        team_abbreviations[0],
                        team_abbreviations[1],
                        season,
                    )
                    expected_records = calculate_expected_stats(raw_expected)
                    save_expected_records(team_abbreviations, season, expected_records)

                    team_by_id = {team["id"]: team for team in summary["teams"]}
                    for player in summary["players"]:
                        player["team_abbreviation"] = team_by_id[player["team_id"]]["abbreviation"]
                    st.session_state["game_result"] = {
                        "game_id": valid_game_id,
                        "season": season,
                        "events": events,
                        "teams": summary["teams"],
                        "players": summary["players"],
                        "expected_records": expected_records,
                    }
                    status.update(
                        label=f"Game {valid_game_id} is ready.",
                        state="complete",
                        expanded=False,
                    )
            except (ValueError, OSError, URLError) as error:
                if status is not None:
                    status.update(
                        label=f"Game {game_id} could not be loaded.",
                        state="error",
                        expanded=True,
                    )
                st.error(f"Game load failed ({type(error).__name__}): {error}")

    result = st.session_state.get("game_result")
    if result is None:
        if not submitted:
            st.info(
                "Enter a game ID to fetch and extract its play-by-play. Game stats are "
                "calculated locally from the extracted event file."
            )
        return

    teams = result["teams"]
    players = result["players"]
    game_season = result["season"]
    expected_records = result["expected_records"]
    game_status = (
        "FINAL"
        if any(event.get("event_type") == "game-end" for event in result["events"])
        else "GAME FEED"
    )
    st.markdown(
        f'<div class="section-caption">Game ID: {result["game_id"]}</div>',
        unsafe_allow_html=True,
    )

    left, right = st.columns([1.08, 1], gap="large")
    with left:
        with st.container(border=True):
            st.markdown('<div class="panel-title">Game scoreboard</div>', unsafe_allow_html=True)
            away, center, home = st.columns([1, 0.65, 1])
            with away:
                _show_team_logo(teams[0])
                st.markdown(
                    f'<div class="team-abbr">{html.escape(teams[0]["abbreviation"])}</div>'
                    f'<div class="team-side">{html.escape(teams[0]["side"])}</div>',
                    unsafe_allow_html=True,
                )
            with center:
                st.markdown(
                    f'<div class="stat-label" style="text-align:center">{game_status}</div>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div class="score">{teams[0]["goals"]}'
                    f'<span class="score-divider"> &ndash; </span>{teams[1]["goals"]}</div>',
                    unsafe_allow_html=True,
                )
            with home:
                _show_team_logo(teams[1])
                st.markdown(
                    f'<div class="team-abbr">{html.escape(teams[1]["abbreviation"])}</div>'
                    f'<div class="team-side">{html.escape(teams[1]["side"])}</div>',
                    unsafe_allow_html=True,
                )

            st.divider()
            st.markdown('<div class="panel-title">Team game stats</div>', unsafe_allow_html=True)
            away_stats, home_stats = st.columns(2)
            for column, team in ((away_stats, teams[0]), (home_stats, teams[1])):
                with column:
                    st.markdown(
                        f'<div class="section-caption">{html.escape(team["abbreviation"])}'
                        f' &middot; {html.escape(team["side"])}</div>',
                        unsafe_allow_html=True,
                    )
                    goals_col, shots_col = st.columns(2)
                    goals_col.metric("Goals", team["goals"])
                    shots_col.metric("Shots", team["shots"])

    with right:
        with st.container(border=True):
            st.markdown('<div class="panel-title">Player spotlight</div>', unsafe_allow_html=True)
            player_labels = {
                f'{player["name"]}  ·  #{player["number"] or "—"}  ·  {player["team_abbreviation"]}': player
                for player in players
            }
            selected_label = st.selectbox(
                "Select a skater",
                list(player_labels),
                label_visibility="collapsed",
            )
            player = player_labels[selected_label]
            _show_headshot(player)
            st.markdown(
                f'<div class="profile-name">{html.escape(player["name"])}</div>'
                f'<div class="profile-team">{html.escape(player["team_abbreviation"])}'
                f' &nbsp;|&nbsp; #{html.escape(str(player["number"] or "—"))}</div>',
                unsafe_allow_html=True,
            )

            st.divider()
            st.markdown('<div class="panel-title">This game</div>', unsafe_allow_html=True)
            game_metrics = st.columns(3)
            for column, label, stat in zip(
                game_metrics,
                ("Goals", "Assists", "Shots"),
                ("goals", "assists", "shots"),
            ):
                column.metric(label, player[stat])

            season_record = find_season_stats(player, game_season, expected_records)
            season_text = f"{game_season - 1}-{str(game_season)[-2:]}" if game_season else "—"
            st.divider()
            st.markdown(
                f'<div class="panel-title">Season totals'
                f' <span class="section-caption">{season_text}</span></div>',
                unsafe_allow_html=True,
            )
            if season_record is None:
                st.info(
                    f"No {season_text} season baseline found for "
                    f"{player['team_abbreviation']} in `data/expected`."
                )
            totals = st.columns(3)
            for column, label, stat in zip(
                totals,
                ("Goals", "Assists", "Shots"),
                ("goals", "assists", "shots"),
            ):
                column.metric(label, _season_value(season_record, stat))

            st.markdown('<div class="panel-title" style="margin-top:1rem">Per game</div>', unsafe_allow_html=True)
            averages = st.columns(3)
            for column, label, stat in zip(
                averages,
                ("Goals", "Assists", "Shots"),
                ("goals", "assists", "shots"),
            ):
                column.metric(label, _per_game_value(season_record, stat))


if __name__ == "__main__":
    main()
