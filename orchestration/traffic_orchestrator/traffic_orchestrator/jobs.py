from dagster import job, in_process_executor

from .ops import run_dbt_build, run_ingestion
from .ops_cleanup import cleanup_raw_files

@job(executor_def=in_process_executor)
def traffic_pipeline_job():
    ingestion_result = run_ingestion()
    run_dbt_build(ingestion_result)

@job(executor_def=in_process_executor)
def cleanup_job():
    cleanup_raw_files()