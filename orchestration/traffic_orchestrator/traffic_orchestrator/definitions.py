from dagster import Definitions

from .jobs import traffic_pipeline_job, cleanup_job
from .schedules import (
    daily_pull_at_utc_midnight,
    monthly_backfill_previous_month,
    cleanup_schedule
)

defs = Definitions(
    jobs=[traffic_pipeline_job, cleanup_job],
    schedules=[
        daily_pull_at_utc_midnight, 
        monthly_backfill_previous_month, 
        cleanup_schedule
        ],
)