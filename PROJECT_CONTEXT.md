# Project Context

This file exists to give an AI coding assistant full context on the project so it can write consistent, correct code without needing the plan re-explained each session.

## What This Project Is

This is a data engineering pipeline that compares a hockey player's live, in-progress game performance against their own historical baseline. It answers one question. Given how this player has performed across past games, is tonight's performance ahead of or behind that expectation, updated continuously as the game happens.

The "live" data is not actually live. It is historical play-by-play data replayed with an artificial delay between events, so it behaves like a live feed without requiring a real live game to test against. This is a deliberate design choice and should be treated as a simulation layer, not real-time ingestion, anywhere it comes up in code comments or documentation.

The project is built to demonstrate specific data engineering tools that are not already covered by the author's existing internship experience with Spark and Airflow. The tools being demonstrated are Kafka, Databricks Structured Streaming, BigQuery, and dbt. Airflow is intentionally excluded from this project on purpose, since it would add no new resume value.

## Scope: Skaters Only, Three Stats

This project covers skaters only. Goalies are explicitly out of scope, since shots and scoring stats mean something structurally different for a goalie (shots-against vs. shots-taken) and would require a separate stat model with no added resume value.

The stats tracked on both the actual and expected side are goals, assists, and shots. These three were chosen because they are directly present in NHL play-by-play events, map cleanly to the same three columns on both the live and historical side, and are simple enough to keep the comparison scoped and demoable. No other stats (hits, giveaways, TOI, etc.) are in scope unless explicitly added later.

## The Two Paths

The system has two independent data paths. The actual-data path is streamed through Kafka and Databricks, while the expected-data path is batch-mode reference data read directly by Streamlit.

### Path 1: Batch (historical baseline)

Historical skater stats are pulled from Hockey Reference. That raw data lands in S3, gets loaded into BigQuery, and is modeled through dbt using a standard layered approach: staging models clean and type the raw data, intermediate models apply business logic, and marts models produce the final per-player historical baseline table. This path runs on a normal batch schedule and produces one row per skater with their expected goals, assists, and shots per game, plus a percentile rank for each stat against the rest of the skater pool. This percentile ranking is the main piece of real analytical work dbt is doing, and is what justifies dbt's presence in the project beyond a flat average.

### Path 2: Streaming (simulated live feed)

This path starts from a single game's play-by-play JSON pulled from the NHL API. A Python producer reads that JSON in event order and publishes each event to a Kafka topic, sleeping briefly between messages to simulate the pace of a real broadcast. A Databricks Structured Streaming job consumes that topic, deduplicates events, and computes rolling per-skater totals for goals, assists, and shots as the game progresses. The output is written as Delta Lake tables on S3 using a medallion pattern: bronze holds raw ingested events, silver holds cleaned and deduplicated events, gold holds the rolling goals/assists/shots totals per skater.

### Comparison and UI

At query time, Streamlit reads the live rolling goals/assists/shots from the Databricks gold output. It fetches the two teams' regular-season totals for the game's NHL season through the expected-stats extraction code and calculates per-game baselines with `calculate_expected_stats`, regardless of whether the selected game is preseason, regular season, or playoffs. The GUI currently does not query the dbt mart or display percentile ranks. It compares actual game stats with those baselines and displays the per-stat difference. Expected data is never sent through Kafka or Databricks.

## Data Source Details

The NHL API endpoint used is `https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play`. It is a free, unofficial, no-key-required endpoint. The response contains a `plays` array, which is a flat list of event objects already in game chronological order, not nested by period. Each event has a `periodDescriptor`, `timeInPeriod`, `timeRemaining`, `typeDescKey` (the event type, such as `goal`, `shot-on-goal`, `hit`, `giveaway`, `takeaway`, `blocked-shot`, `missed-shot`, `penalty`, `faceoff`, `stoppage`, `period-start`, `period-end`), and a `details` object whose fields vary depending on the event type. For this project, the relevant event types are `goal` (has `scoringPlayerId`, `assist1PlayerId`, `assist2PlayerId`) and `shot-on-goal` (has `shootingPlayerId`). Other event types are not aggregated for this project's stat scope.

The response also contains a `rosterSpots` array, which maps `playerId` to first name, last name, team ID, position, and headshot URL. This is needed because play events only reference players by numeric ID, not by name, so any downstream display or aggregation needs to join against this roster lookup. `rosterSpots` is also how goalies are filtered out, by position.

The game ID is always treated as a parameter, never hardcoded. Any script or pipeline stage that needs a specific game should accept it as a function argument, CLI argument, or config value.

## Files Built So Far

`fetch_game.py`: takes a game ID as a CLI argument, calls the NHL API endpoint above, flattens the response in memory, and saves one JSON row per play as `play_by_play_{game_id}.jsonl` under `data/extracted/` by default. It does not persist the raw API response.

`extract_plays.py`: can read an NHL game JSON file and flatten each play event into a row containing game ID, event ID, period, time in period, event type, and the raw `details` object. The JSONL output from `fetch_game.py` is what the Kafka producer will publish, one row per Kafka message.

## Not Yet Built

The complete batch path from Hockey Reference through BigQuery and dbt, including the percentile ranking logic.

## Streamlit App

`pipelines/actual/databricks/nhl_events_pipeline.py` defines the DLT bronze, silver, and gold tables. It retrieves Confluent credentials from the Databricks `confluent-keys` secret scope; credential values are not in source control. `streamlit/gui.py` accepts a game ID, fetches NHL play-by-play, extracts events, and publishes them to the configured Kafka topic. It waits for the matching rows in the Databricks gold table, then fetches the two teams' season stats and calculates totals and per-game averages for display. Those calculated records are saved locally; the GUI does not run the expected-stats GCS/BigQuery batch load. The app requires the Databricks streaming pipeline to already be running against the configured topic. Connection details and setup instructions are in the Streamlit section of `README.md`.

The expected stats remain a separate batch path; they are never sent through Kafka or Databricks.

## Build Order and Priority

The original recommended build order was batch path first to validate BigQuery and dbt, then the Kafka producer, the Databricks streaming consumer, and Streamlit integration last. The current Streamlit app orchestrates game ingestion and expected-stat extraction, but depends on a running Databricks consumer and configured Kafka and Databricks SQL access.

## Constraints to Respect in Any Code Suggestions

Do not suggest Airflow for orchestration anywhere in this project. Do not suggest Snowflake; the warehouse is BigQuery. Do not hardcode a specific game ID, team, or player anywhere; everything should be parameterized. Keep the xG or any predictive modeling out of scope; this project is about comparing actual performance to historical actuals, not building a predictive model. Do not include goalies or goalie-specific stats anywhere in aggregation logic. Do not add stats beyond goals, assists, and shots without an explicit scope change. Any code should assume a co-op/internship-level portfolio project, so favor clarity and correctness over premature optimization or overengineering.
