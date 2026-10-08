import argparse
from pathlib import Path

from google.cloud import bigquery


SKATER_STATS_SCHEMA = [
    bigquery.SchemaField("player_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("player_url", "STRING"),
    bigquery.SchemaField("team_abbrev", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("season", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("games_played", "INT64"),
    bigquery.SchemaField("goals", "INT64"),
    bigquery.SchemaField("assists", "INT64"),
    bigquery.SchemaField("shots", "INT64"),
]


def load_gcs_jsonl_to_bigquery(
    gcs_uri: str,
    table_id: str,
    client: bigquery.Client | None = None,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> int:
    """Load a GCS JSONL object into BigQuery and return the loaded row count."""
    if not gcs_uri.startswith("gs://"):
        raise ValueError("gcs_uri must start with gs://")
    if not table_id:
        raise ValueError("table_id must not be empty")

    load_config = bigquery.LoadJobConfig(
        schema=SKATER_STATS_SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
    )
    load_job = (client or bigquery.Client()).load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=load_config,
    )
    load_job.result()
    return int(load_job.output_rows or 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load an expected-stats JSONL object from GCS into BigQuery.")
    parser.add_argument("gcs_uri", help="GCS URI for the JSONL object")
    parser.add_argument("table_id", help="BigQuery table ID, such as project.dataset.table")
    args = parser.parse_args()

    rows_loaded = load_gcs_jsonl_to_bigquery(args.gcs_uri, args.table_id)
    print(f"Loaded {rows_loaded} rows -> {args.table_id}")


if __name__ == "__main__":
    main()
