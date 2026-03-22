from datetime import timedelta, timezone
from dagster import schedule

from .jobs import traffic_pipeline_job, cleanup_job

@schedule(
    job=traffic_pipeline_job,
    cron_schedule="0 0 * * *",
    execution_timezone="UTC"
)
def daily_pull_at_utc_midnight(context):
    scheduled = context.scheduled_execution_time    # tz-aware

    # Run covers "yesterday UTC" with overlap to avoid boundary misses
    overlap = timedelta(minutes=5)


    # e.g. scheduled = 2026-02-18 00:00Z
    # since_base = 2026-02-17 00:00Z
    since_base = (scheduled - timedelta(days=1)).astimezone(timezone.utc)

    # since = 2026-02-16 23:55Z (5 min overlap)
    since_dt = since_base - overlap
    since = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    out_date = since_base.strftime("%Y-%m-%d")  # label file by the day being pulled

    return {
        "ops": {
            "run_ingestion": {
                "config": {
                    "mode": "pull",
                    "since": since,
                    "out": f"data/raw/incremental/dagster_pull_{out_date}.jsonl",
                    "page_size": 1000,
                    "max_pages": 10,
                    "load_to_bq": True,
                    "run_silver_merge": True,
                }
            }
        }
    }

@schedule(
    job=traffic_pipeline_job,
    cron_schedule="10 0 1 * *",      # 00:10 UTC on the 1st of every month
    execution_timezone="UTC",
)
def monthly_backfill_previous_month(context):
    scheduled = context.scheduled_execution_time.astimezone(timezone.utc)

    # scheduled example: 2026-03-01 00:10Z
    # backfill month: 2026-02
    first_day_of_this_month = scheduled.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day_prev_month = first_day_of_this_month - timedelta(days=1)
    month_str = last_day_prev_month.strftime("%Y-%m")

    return {
        "ops": {
            "run_ingestion": {
                "config": {
                    "mode": "backfill",
                    "month": month_str,
                    "out": f"data/raw/backfill/dagster_backfill_{month_str}.jsonl",
                    "page_size": 1000,
                    "max_pages": 10,
                    "load_to_bq": True,
                    "run_silver_merge": True,
                }
            }
        }
    }

@schedule(
    job=cleanup_job,
    cron_schedule="0 1 * * *",
    execution_timezone="UTC",
)
def cleanup_schedule(context):
    return {
        "ops": {
            "cleanup_raw_files": {
                "config": {
                    "paths": ["data/raw/incremental", "data/raw/backfill"],
                    "older_than_days": 14,
                    "include_patterns": ["*.jsonl", "*.ndjson"],
                    "dry_run": False,
                }
            }
        }
    }