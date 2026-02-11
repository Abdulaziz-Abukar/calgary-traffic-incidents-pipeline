import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from src.storage.exceptions import (
    make_bq_client,
    assert_dataset_access,
    assert_table_access
)
from src.storage.bq_jobs import assert_job_succeeded
from src.common.exceptions import require_env
from src.ingestion.queries import build_merge_sql


load_dotenv()


def _latest_snapshot_id(client: bigquery.Client, bronze_table: str) -> str:
    sql = f"""
    SELECT snapshot_id
    FROM `{bronze_table}`
    WHERE snapshot_id IS NOT NULL
    ORDER BY snapshot_ts DESC, snapshot_id DESC
    LIMIT 1
    """

    row = next(client.query(sql).result(), None)
    if row is None or row.snapshot_id is None:
        raise RuntimeError(f"No snapshot_id found in {bronze_table}")
    return str(row.snapshot_id)

def run_silver_merge(snapshot_id: str | None = None) -> str | None:
    
    # --------- Config ---------
    GCP_PROJECT_ID = require_env("GCP_PROJECT_ID")
    BRONZE_DATASET_ID = require_env("BRONZE_DATASET_ID")
    BRONZE_TABLE_ID = require_env("BRONZE_TABLE_ID")
    SILVER_DATASET_ID = require_env("SILVER_DATASET_ID")
    SILVER_TABLE_ID = require_env("SILVER_TABLE_ID")

    merge_sql = build_merge_sql(
        gcp_project_id=GCP_PROJECT_ID,
        bronze_dataset_id=BRONZE_DATASET_ID,
        bronze_table_id=BRONZE_TABLE_ID,
        silver_dataset_id=SILVER_DATASET_ID,
        silver_table_id=SILVER_TABLE_ID
    )

    table_id = f"{GCP_PROJECT_ID}.{SILVER_DATASET_ID}.{SILVER_TABLE_ID}"
    dataset_id = f"{GCP_PROJECT_ID}.{SILVER_DATASET_ID}"

    
    # --------- Client ---------
    client = make_bq_client()


    # If no snapshot provided, resolve automatically (standalone usage)
    if snapshot_id is None:
        bronze_table = f"{GCP_PROJECT_ID}.{BRONZE_DATASET_ID}.{BRONZE_TABLE_ID}"
        snapshot_id = _latest_snapshot_id(client, bronze_table)
        print(f"[silver] auto snapshot_id = {snapshot_id}")


    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
        ],
        labels={"layer": "silver", "job": "merge_incident_current"},
    )

    # --------- fail fast: dataset/table existance/access ---------
    assert_dataset_access(client, dataset_id)
    assert_table_access(client, table_id)


    # --------- Submit + Wait ---------
    try:
        job = client.query(merge_sql, job_config=job_config)
        job.result()
    except GoogleAPIError as e:
        raise RuntimeError(f"BigQuery job failed for {table_id}") from e
    
    
    assert_job_succeeded(
        job,
        context={
            "layer": "silver",
            "table": table_id,
            "snapshot_id": snapshot_id
        }
    )
    
    print(f"JobID {job.job_id}")
    print(f"Successfully merged rows into {table_id}")
    return job.job_id


