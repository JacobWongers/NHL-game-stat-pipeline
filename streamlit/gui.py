import html
import json
import re
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTRACTED_DIR = PROJECT_ROOT / "data" / "extracted"
EXPECTED_DIR = PROJECT_ROOT / "data" / "expected"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, 1):
            if not line.strip():
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError(f"{path}:{line_number} must contain a JSON object.")
            records.append(record)
    return records


@st.cache_data(ttl=30, show_spinner=False)
def load_game(path_string: str) -> list[dict[str, Any]]:
    return read_jsonl(Path(path_string))


@st.cache_data(ttl=30, show_spinner=False)
def load_expected_stats(directory_string: str) -> list[dict[str, Any]]:
    directory = Path(directory_string)
    records: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*_SKATER_STATS.jsonl")):
        records.extend(read_jsonl(path))
    return records


def _localized_name(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("default", "")
    return str(value or "")


def _team_abbreviation(headshot_url: str) -> str:
    parts = urlparse(headshot_url).path.split("/")
    if len(parts) >= 2 and re.fullmatch(r"[A-Z0-9]{2,4}", parts[-2]):
        return parts[-2]
    return ""


def _normalize_name(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_name = decomposed.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", ascii_name.casefold())


def build_game_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    roster: list[Any] = []
    for event in events:
        candidate_roster = event.get("roster_spots")
        if isinstance(candidate_roster, list):
            roster = candidate_roster
            break

    players: dict[str, dict[str, Any]] = {}
    teams: dict[str, dict[str, Any]] = {}

    for roster_entry in roster:
        if not isinstance(roster_entry, dict):
            continue
        player_id = roster_entry.get("playerId")
        team_id = roster_entry.get("teamId")
        if player_id is None or team_id is None:
            continue

        team_key = str(team_id)
        headshot_url = str(roster_entry.get("headshot") or "")
        abbreviation = _team_abbreviation(headshot_url)
        teams[team_key] = {
            "id": team_key,
            "abbreviation": abbreviation or f"TEAM {len(teams) + 1}",
            "logo_url": (
                f"https://assets.nhle.com/logos/nhl/svg/{abbreviation}_light.svg"
                if abbreviation
                else ""
            ),
            "goals": 0,
            "shots": 0,
        }

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

    if not teams:
        raise ValueError("The selected game feed does not contain a player roster.")

    side_by_team: dict[str, str] = {}
    previous_counts = {"awaySOG": 0, "homeSOG": 0, "awayScore": 0, "homeScore": 0}
    for event in events:
        details = event.get("details")
        if not isinstance(details, dict):
            continue
        event_type = event.get("event_type")
        owner_id = details.get("eventOwnerTeamId")
        owner_key = str(owner_id) if owner_id is not None else ""

        if event_type == "shot-on-goal":
            for count_key, side in (("awaySOG", "away"), ("homeSOG", "home")):
                current = int(details.get(count_key, previous_counts[count_key]) or 0)
                if current > previous_counts[count_key] and owner_key in teams:
                    side_by_team[owner_key] = side
                previous_counts[count_key] = max(previous_counts[count_key], current)
            if owner_key in teams:
                teams[owner_key]["shots"] += 1
            shooter = details.get("shootingPlayerId")
            if str(shooter) in players:
                players[str(shooter)]["shots"] += 1

        if event_type == "goal":
            for count_key, side in (("awayScore", "away"), ("homeScore", "home")):
                current = int(details.get(count_key, previous_counts[count_key]) or 0)
                if current > previous_counts[count_key] and owner_key in teams:
                    side_by_team[owner_key] = side
                previous_counts[count_key] = max(previous_counts[count_key], current)

            if owner_key in teams:
                teams[owner_key]["goals"] += 1
                teams[owner_key]["shots"] += 1
            scorer = details.get("scoringPlayerId")
            if str(scorer) in players:
                players[str(scorer)]["goals"] += 1
                players[str(scorer)]["shots"] += 1
            for assist_key in ("assist1PlayerId", "assist2PlayerId"):
                assister = details.get(assist_key)
                if str(assister) in players:
                    players[str(assister)]["assists"] += 1

    ordered_team_ids = sorted(
        teams,
        key=lambda team_id: (
            side_by_team.get(team_id) != "away",
            teams[team_id]["abbreviation"],
        ),
    )
    for index, team_id in enumerate(ordered_team_ids):
        teams[team_id]["side"] = side_by_team.get(team_id, f"TEAM {index + 1}")

    skaters = [
        player for player in players.values()
        if player["position"] != "G" and player["name"]
    ]
    skaters.sort(key=lambda player: (player["name"].casefold(), player["team_id"]))
    return {
        "teams": [teams[team_id] for team_id in ordered_team_ids],
        "players": skaters,
    }


def season_for_game(game_id: Any) -> int | None:
    date_prefix = str(game_id)[:8]
    if len(date_prefix) != 8 or not date_prefix.isdigit():
        return None
    year = int(date_prefix[:4])
    month = int(date_prefix[4:6])
    if not 1 <= month <= 12:
        return None
    return year + 1 if month >= 10 else year


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
    total = float(record.get(stat, 0) or 0)
    games_played = int(record.get("games_played", 0) or 0)
    if games_played <= 0:
        return "0.00"
    return f"{total / games_played:.2f}"


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

    game_paths = sorted(
        (str(path) for path in EXTRACTED_DIR.glob("play_by_play_*.jsonl")),
        key=lambda path: Path(path).stem.removeprefix("play_by_play_"),
        reverse=True,
    )
    if not game_paths:
        st.error(f"No game feeds found in `{EXTRACTED_DIR}`.")
        st.stop()
    game_options = {
        f"Game {Path(path).stem.removeprefix('play_by_play_')}": path
        for path in game_paths
    }

    header_left, header_right = st.columns([1.3, 1])
    with header_left:
        st.markdown('<div class="eyebrow">Hockey analytics</div>', unsafe_allow_html=True)
        st.markdown('<h1 class="page-title">NHL Game Center</h1>', unsafe_allow_html=True)
        st.markdown(
            '<div class="page-subtitle">Live game performance and season context</div>',
            unsafe_allow_html=True,
        )
    with header_right:
        selected_game = st.selectbox(
            "Choose a game feed",
            list(game_options),
            label_visibility="collapsed",
        )
        selected_path = game_options[selected_game]

    events = load_game(selected_path)
    summary = build_game_summary(events)
    teams = summary["teams"]
    players = summary["players"]
    if len(teams) != 2:
        raise ValueError(
            f"Expected two teams in {Path(selected_path).name}; found {len(teams)}."
        )
    if not players:
        st.error(f"No skaters found in `{Path(selected_path).name}`.")
        st.stop()

    team_by_id = {team["id"]: team for team in teams}
    for player in players:
        player["team_abbreviation"] = team_by_id[player["team_id"]]["abbreviation"]
    game_id = events[0].get("game_id")
    game_season = season_for_game(game_id)
    expected_records = load_expected_stats(str(EXPECTED_DIR))
    game_status = (
        "FINAL"
        if any(event.get("event_type") == "game-end" for event in events)
        else "LIVE"
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
