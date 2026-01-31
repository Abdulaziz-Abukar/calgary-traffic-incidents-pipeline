import os
from pathlib import Path
import argparse

from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, NotFound



load_dotenv()


def load_jsonl_to_bq(jsonl_path: str | Path) -> int | None:

    jsonl_path = Path(jsonl_path)

    # --------- fail fast: local file checks ---------

    if not jsonl_path.exists():
        raise FileNotFoundError(f"Input file not found: {jsonl_path}")
    if jsonl_path.stat().st_size == 0:
        raise RuntimeError(f"Input file is empty: {jsonl_path}")
    

    # --------- Config ---------
    GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
    if not GCP_PROJECT_ID:
        raise RuntimeError("GCP_PROJECT_ID is not set in environment variables.")

    table_id = f"{GCP_PROJECT_ID}.traffic_bronze.traffic_incidents_raw"
    dataset_id = f"{GCP_PROJECT_ID}.traffic_bronze"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
        schema=[
            bigquery.SchemaField("snapshot_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("snapshot_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("run_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("query_name", "STRING", mode="REQUIRED"),

            bigquery.SchemaField("incident_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("incident_info", "STRING"),
            bigquery.SchemaField("description", "STRING"),

            bigquery.SchemaField("start_ts", "TIMESTAMP"),
            bigquery.SchemaField("modified_ts", "TIMESTAMP"),

            bigquery.SchemaField("quadrant", "STRING"),
            bigquery.SchemaField("longitude", "FLOAT64"),
            bigquery.SchemaField("latitude", "FLOAT64"),
            bigquery.SchemaField("count", "INT64"),

            bigquery.SchemaField("source_row_id", "STRING"),
            bigquery.SchemaField("source_version", "STRING"),
            bigquery.SchemaField("source_created_at", "TIMESTAMP"),
            bigquery.SchemaField("source_updated_at", "TIMESTAMP"),
        ],
        ignore_unknown_values=False,
        max_bad_records=0,
    )

    # --------- Client ---------
    try:
        client = bigquery.Client()
    except GoogleAPIError as e:
        raise RuntimeError("Failed to create BigQuery client") from e


    #--------- fail fast: dataset/table existence/access ---------

    try:
        client.get_dataset(dataset_id)
    except NotFound as e:
        raise RuntimeError(f"Dataset not found: {dataset_id}") from e
    except GoogleAPIError as e:
        raise RuntimeError(f"Cannot access dataset: {dataset_id}") from e
    

    try:
        client.get_table(table_id)
    except NotFound as e:
        raise RuntimeError(f"Table not found: {table_id}") from e
    except GoogleAPIError as e:
        raise RuntimeError(f"Cannot access table: {table_id}") from e
    
    # --------- Submit + Wait ---------
    try:
        with open(jsonl_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
    except GoogleAPIError as e:
        raise RuntimeError(f"BigQuery load failed for {table_id}") from e
    
    if job.error_result:
        raise RuntimeError(f"BigQuery job error_result: {job.error_result}")

    if job.errors:
        raise RuntimeError(f"BigQuery load completed with errors: {job.errors}")
    
    print(f"JobID {job.job_id}")
    print(f"Loaded {job.output_rows} rows into {table_id}")
    return job.output_rows



def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--in", dest="in_path", required=True, help="Path to .jsonl file")
    args = parser.parse_args()

    load_jsonl_to_bq(args.in_path)



if __name__ == "__main__":
    main()