# NHL Actual vs. Expected Stats

This project compares a skater's performance in a past NHL game with their season averages. Historical play-by-play is replayed through Kafka to simulate a live game feed. The pipeline processes the events with Databricks and stores the results in Delta Lake tables on S3.

The project covers skaters and tracks goals, assists, and shots.

## Data Pipelines

**Actual:** Replays a past game's play-by-play as a simulated live feed. The Streamlit game summary shows the score and shots for both teams.

Flow: Game play-by-play JSON → Kafka → S3 → Databricks bronze, silver, and gold Delta Lake tables → Streamlit game summary (left).

**Expected:** Calculates each player's season totals and per-game averages for goals, assists, and shots from season stats for both teams. The Streamlit player view shows season totals and per-game averages alongside the player's game stats.

Flow: Team season stats → Python script → Google Cloud Platform/BigQuery → Streamlit player details (right).

## Streamlit Interface

Run the interface from the repository root:

```bash
streamlit run streamlit/gui.py
```

The interface uses two panels based on the game-center layout:

- **Game scoreboard:** Select a local feed from `data/extracted/` to see both teams' logos, the final score, and goals and shots for each team.
- **Player spotlight:** Select a skater to see their headshot and goals, assists, and shots in that game. Season totals and per-game averages are shown when a matching player, team, and season are available in `data/expected/`; otherwise the UI identifies the missing baseline.

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