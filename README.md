# NHL Actual vs. Expected Stats

This project compares a skater's performance in a past NHL game with their season averages. Historical play-by-play is replayed through Kafka to simulate a live game feed. The pipeline processes the events with Databricks and stores the results in Delta Lake tables on S3.

The project covers skaters and tracks goals, assists, and shots.

## Data Pipelines

**Actual:** The user enters an NHL game ID in Streamlit. The app fetches play-by-play, extracts and saves the event rows as JSONL, then reads that local file and calculates per-skater game totals directly.

Flow in the GUI: NHL play-by-play JSON → extracted event JSONL → local game summary (left). Kafka and Databricks are no longer required to run the GUI.

**Expected:** The app fetches both teams' season stats, calculates totals and per-game averages for goals, assists, and shots, and displays the matching skater's baseline.

Expected stats always use the regular-season table for the game's NHL season, whether the selected game is preseason, regular season, or playoffs.

Flow used by the game-ID UI: Hockey Reference → `extract_batch` team extraction → `calculate_expected_stats` → Streamlit player details (right).

## Streamlit Interface

Run the interface from the repository root:

```bash
streamlit run streamlit/gui.py
```

Enter a 10-digit NHL game ID and select **Load game stats**. The app performs these steps:

1. Fetch the game's NHL play-by-play.
2. Extract and save the plays as `data/extracted/play_by_play_<game_id>.jsonl`.
3. Read that extracted file and locally aggregate skater goals, assists, and shots.
4. Fetch both teams' regular-season stats and calculate per-game averages.
5. Show the locally calculated game stats and expected season stats side by side.

The GUI requires internet access to the NHL API and Hockey Reference. It does not require Kafka, Databricks, or `.streamlit/secrets.toml`.

### Optional Databricks Streaming Pipeline

The DLT source is [nhl_events_pipeline.py](./pipelines/actual/databricks/nhl_events_pipeline.py). This is an optional, separate project pipeline; the Streamlit GUI no longer writes to Kafka or reads its gold table.

For the standalone Databricks pipeline, create a secret scope named `confluent-keys` and add secrets named `api-key`, `api-secret`, and `bootstrap-server`. The pipeline retrieves these values with `dbutils.secrets.get`; the values themselves are never stored in this repository. Configure its target catalog/schema and Spark setting `nhl.kafka.topic` in Databricks.

The gold table exposes `game_id`, `player_id`, `player_name`, `position`, `sweater_number`, `headshot`, `goals`, `assists`, and `shots`.

## Project Structure

```text
data/                              # Raw game JSON and extracted events
pipelines/
	actual/
		ingestion/                     # Fetch and extract game events
		producer/                      # Publish events to Kafka
		databricks/                    # Stream processing jobs
	expected/batch/                  # Calculate player season averages
streamlit/                            # Streamlit interface
tests/                             # Pipeline tests
```


Created this project because I love watching hockey and seeing the skater stats :)