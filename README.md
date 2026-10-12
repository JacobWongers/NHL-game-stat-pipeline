# NHL Actual vs. Expected Stats

This project compares a skater's performance in a past NHL game with their season averages. Historical play-by-play is replayed through Kafka to simulate a live game feed. The pipeline processes the events with Databricks and stores the results in Delta Lake tables on S3.

The project covers skaters and tracks goals, assists, and shots.

## Data Pipelines

**Actual:** The user enters an NHL game ID in Streamlit. The app fetches and extracts the play-by-play, streams its events to the configured Kafka topic, then reads the per-skater game totals from the Databricks gold table.

Flow: Game play-by-play JSON → Confluent Kafka → Databricks bronze, silver, and gold tables → Streamlit game summary (left).

**Expected:** After the game stats arrive, the app fetches both teams' season stats, calculates totals and per-game averages for goals, assists, and shots, and displays the matching skater's baseline.

Expected stats always use the regular-season table for the game's NHL season, whether the selected game is preseason, regular season, or playoffs.

Flow used by the game-ID UI: Hockey Reference → `extract_batch` team extraction → `calculate_expected_stats` → Streamlit player details (right).

## Streamlit Interface

Run the interface from the repository root:

```bash
streamlit run streamlit/gui.py
```

Enter a 10-digit NHL game ID and select **Run game pipeline**. The app performs these steps:

1. Fetch the game's NHL play-by-play and extract its events.
2. Stream those events to the configured Kafka topic.
3. Wait for the active skaters' totals to arrive in Databricks gold.
4. Fetch both teams' season stats and calculate the season totals and per-game averages.
5. Show the gold-layer game stats and expected season stats side by side.

The Databricks streaming pipeline must already be running and consuming the same Kafka topic. The app waits up to five minutes for gold-layer rows and reports a timeout if they do not arrive.

### Streamlit Configuration

Store the app settings in the git-ignored `.streamlit/secrets.toml`. The producer topic must match the Databricks pipeline topic.

```toml
KAFKA_TOPIC = "events"
DATABRICKS_SERVER_HOSTNAME = "your-workspace.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/your-warehouse-id"
DATABRICKS_ACCESS_TOKEN = "your-token"
DATABRICKS_GOLD_TABLE = "catalog.schema.nhl_events_gold"

[kafka]
"bootstrap.servers" = "your-bootstrap-server"
"security.protocol" = "SASL_SSL"
"sasl.mechanisms" = "PLAIN"
"sasl.username" = "your-api-key"
"sasl.password" = "your-api-secret"
resource_id = "your-cluster-resource-id"
```

The Kafka producer reads its connection settings from the `[kafka]` section. If that section is absent, it falls back to the ignored `kafka.properties` file (configurable with `KAFKA_PROPERTIES`). Keep credential values out of source code and committed configuration.

### Databricks Streaming Pipeline

The DLT source is [nhl_events_pipeline.py](./pipelines/actual/databricks/nhl_events_pipeline.py). Create or update a Databricks pipeline using this Python source, and set its target catalog and schema. The app's `DATABRICKS_GOLD_TABLE` must point to that pipeline's `nhl_events_gold` table.

Create a Databricks secret scope named `confluent-keys` and add secrets named `api-key`, `api-secret`, and `bootstrap-server`. The pipeline retrieves these values with `dbutils.secrets.get`; the values themselves are never stored in this repository. Set the pipeline Spark configuration `nhl.kafka.topic` to the same topic as `KAFKA_TOPIC` (default `events`). Leave the pipeline running before submitting a game ID in Streamlit.

The gold table exposes `game_id`, `player_id`, `player_name`, `position`, `sweater_number`, `headshot`, `goals`, `assists`, and `shots`. `DATABRICKS_GOLD_TABLE` defaults to `nhl_events_gold`; use a catalog- and schema-qualified name when required by your workspace.

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