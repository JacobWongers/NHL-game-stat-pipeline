# NHL Actual vs. Expected Stats

This project compares an NHL skater's goals, assists, and shots in a selected game with their regular-season per-game averages. A Streamlit game-center interface lets you enter a game ID, view the matchup and team totals, and select a player for a larger, accessible stats display.

The current GUI calculates game stats locally from NHL play-by-play. It does not require Kafka or Databricks.

## Streamlit Game Center

### Run the app

From the project root, install the Python dependencies and launch Streamlit:

```bash
pip install -r requirements.txt
streamlit run streamlit/gui.py
```

### Use the interface

1. Enter a 10-digit NHL game ID and select **Load game stats**.
2. The app fetches the game's play-by-play from the NHL API and extracts the events.
3. Extracted events are saved to `data/extracted/play_by_play_<game_id>.jsonl`. The app reads this file to calculate actual goals, assists, and shots for each skater and both teams.
4. The app fetches both teams' **regular-season** stats for the game's NHL season and calculates goals, assists, and shots per game.
5. View the score and team totals on the left. Select a skater on the right to compare their game performance with their regular-season per-game averages.

Expected stats always come from the regular-season table, whether the selected game is preseason, regular season, or playoffs. Goalies are excluded. A goal contributes one goal and one shot; assists are counted from the goal event's assist player IDs; shots on goal are counted from shot events.

The app needs internet access to the NHL API and Hockey Reference. It does not need a `.streamlit/secrets.toml` file, Kafka credentials, or Databricks connection settings.

## Data Pipelines

### Streamlit actual and expected stats

```text
NHL game ID
    └── NHL play-by-play API
          └── extracted event JSONL
                └── local player/team stat aggregation ──┐
                                                         ├── Streamlit game center
Hockey Reference regular-season team stats               │
    └── per-game expected stats ─────────────────────────┘
```

### Separate Kafka and Databricks pipeline

The repository also contains a Kafka producer and a Databricks DLT pipeline for a separate streaming demonstration. The current Streamlit GUI does not publish to that pipeline or read its medallion tables.

The DLT source is [`pipelines/actual/databricks/nhl_events_pipeline.py`](./pipelines/actual/databricks/nhl_events_pipeline.py). It reads Confluent credentials from a Databricks secret scope and produces bronze, silver, and gold tables. Configure and run it from Databricks if using that standalone pipeline; its runtime dependencies and secrets belong in Databricks, not the GUI's local virtual environment.

## Project Structure

```text
data/
  extracted/                       # Game play-by-play events saved as JSONL
  expected/                        # Locally calculated expected-stat records
pipelines/
  actual/
    ingestion/                     # NHL game fetch and play extraction
    producer/                      # Optional Kafka event producer
    databricks/                    # Optional Databricks DLT pipeline
  expected/
    calculate_expected_stats.py    # Per-game expected-stat calculations
    hockey_reference/              # Regular-season stat fetching and extraction
streamlit/
  gui.py                           # Streamlit game-center interface
tests/                             # Pipeline and GUI tests
```

Created because I love watching hockey and seeing skater stats :)
