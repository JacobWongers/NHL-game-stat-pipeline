import argparse
from pathlib import Path

from google.cloud import storage


def build_gcs_key(file_path: Path, prefix: str) -> str:
    filename = file_path.name
    clean_prefix = prefix.strip("/")
    return f"{clean_prefix}/{filename}" if clean_prefix else filename


def upload_file_to_gcs(file_path: Path, bucket: str, prefix: str = "expected/hockey_reference") -> str:
    """Upload one generated batch file and return its GCS URI."""
    if not file_path.is_file():
        raise FileNotFoundError(f"Input file does not exist: {file_path}")

    key = build_gcs_key(file_path, prefix)
    blob = storage.Client().bucket(bucket).blob(key)
    blob.upload_from_filename(str(file_path), content_type="application/x-ndjson")
    return f"gs://{bucket}/{key}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload one expected-stats file to Google Cloud Storage.")
    parser.add_argument("file", type=Path, help="Generated JSONL or JSON file to upload")
    parser.add_argument("--bucket", required=True, help="Google Cloud Storage bucket name")
    parser.add_argument(
        "--prefix",
        default="expected/hockey_reference",
        help="GCS object prefix; the input filename is appended",
    )
    args = parser.parse_args()

    gcs_uri = upload_file_to_gcs(args.file, args.bucket, args.prefix)
    print(f"Uploaded -> {gcs_uri}")


if __name__ == "__main__":
    main()