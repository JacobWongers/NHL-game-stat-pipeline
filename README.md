# NHL Actual vs. Expected Stats

This project compares a skater's performance in a past NHL game with their season averages. Historical play-by-play is replayed through Kafka to simulate a live game feed. The pipeline processes the events with Databricks and stores the results in Delta Lake tables on S3.

The project covers skaters and tracks goals, assists, and shots.

## Data Pipelines

**Actual:** Replays a past game's play-by-play as a simulated live feed. The Streamlit game summary shows the score and shots for both teams.

Flow: Game play-by-play JSON → Kafka → S3 → Databricks bronze, silver, and gold Delta Lake tables → Streamlit game summary (left).

**Expected:** Calculates each player's season totals and per-game averages for goals, assists, and shots from season stats for both teams. The Streamlit player view shows season totals and per-game averages alongside the player's game stats.

Flow: Team season stats → Python script → Google Cloud Platform/BigQuery → Streamlit player details (right).

## Streamlit Interface

The interface uses two columns:

- **Left — Game summary:** Displays both teams' logos, the game score, and shots for each team.
- **Right — Player details:** A single player dropdown selects the skater to display. The player's headshot appears at the top, followed by their goals, assists, and shots in the game. Below the game stats, the interface shows their season goals and assists totals, then their average goals and assists per game.

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