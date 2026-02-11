from __future__ import annotations
import os


from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, NotFound


def make_bq_client() -> bigquery.Client:
    """Create a BigQuery client with a consistent error message"""
    try:
        return bigquery.Client()
    except GoogleAPIError as e:
        raise RuntimeError("Failed to create BigQuery Client.") from e
    except Exception as e:
        # Covers credential/config edge cases that might not be GoogleAPIError
        raise RuntimeError("Failed to create BigQuery client (unexpected error).")


def assert_dataset_access(client: bigquery.Client, dataset_id: str) -> None:
    """Ensure dataset exists and is accessible"""
    try:
        client.get_dataset(dataset_id)
    except NotFound as e:
        raise RuntimeError(f"Dataset not found: {dataset_id}") from e
    except GoogleAPIError as e:
        raise RuntimeError(f"Cannot access dataset: {dataset_id}") from e
    

def assert_table_access(client: bigquery.Client, table_id: str) -> None:
    """Ensure table exists and is accessible."""
    try:
        client.get_table(table_id)
    except NotFound as e:
        raise RuntimeError(f"Table not found: {table_id}") from e
    except GoogleAPIError as e:
        raise RuntimeError(f"Cannot access table: {table_id}") from e