import os
import subprocess
import sys
from pathlib import Path
from typing import Literal, Optional

from dagster import op, OpExecutionContext, Config, MetadataValue, RetryPolicy

from src.ingestion.runner import run_pipeline

REPO_ROOT = Path(__file__).resolve().parents[3]


RunMode = Literal["pull", "backfill"]


class IngestionConfig(Config):
    mode: RunMode = "pull"

    # pull
    since: Optional[str] = None     # e.g. "2026-02-17T00:00:00Z"

    # backfill
    month: Optional[str] = None     # e.g. "2026-01"

    # common
    page_size: int = 1000
    max_pages: int = 10
    out: str = "data/raw/dagster/run.jsonl"
    load_to_bq: bool = True
    run_silver_merge: bool = True


@op(retry_policy=RetryPolicy(max_retries=3))
def run_ingestion(context: OpExecutionContext, config: IngestionConfig) -> dict:
    result = run_pipeline(
        command=config.mode,
        since=config.since,
        month=config.month,
        page_size=config.page_size,
        max_pages=config.max_pages,
        out=config.out,
        load_to_bq=config.load_to_bq,
        run_silver_merge_flag=config.run_silver_merge,
    )

    context.log.info(
        f"[RUN][ingestion] mode={result['command']} "
        f"snapshot_id={result['snapshot_id']} "
        f"rows_written={result['rows_written']} "
        f"rows_loaded={result['rows_loaded']} "
        f"loaded_to_bq={result['loaded_to_bq']} "
        f"silver_merge_ran={result['silver_merge_ran']}"
    )
    
    context.log.info(f"[RUN][ingestion] output_path={result['output_path']}")
    
    if result.get("watermark_before"):
        context.log.info(f"[RUN][ingestion] watermark_before={result['watermark_before']}")
    if result.get("watermark_after"):
        context.log.info(f"[RUN][ingestion] watermark_after={result['watermark_after']}")
    if result.get("silver_merge_job_id"):
        context.log.info(f"[RUN][ingestion] silver_merge_job_id={result['silver_merge_job_id']}")
    
    context.add_output_metadata(
        {
            "command": result["command"],
            "snapshot_id": result["snapshot_id"] or "none",
            "rows_written": result["rows_written"],
            "rows_loaded": result["rows_loaded"],
            "output_path": MetadataValue.path(result["output_path"]),
            "loaded_to_bq": result["loaded_to_bq"],
            "silver_merge_ran": result["silver_merge_ran"],
            "watermark_before": result["watermark_before"] or "none",
            "watermark_after": result["watermark_after"] or "none",
            "silver_merge_job_id": result["silver_merge_job_id"] or "none",

        }
    )

    return result

@op
def run_dbt_build(context: OpExecutionContext, ingestion_result: dict) -> dict:
    dbt_dir = REPO_ROOT / "dbt" / "traffic_incidents"

    if ingestion_result["rows_loaded"] == 0:
        context.log.info("[dbt] skipped (no new data)")
        output = {
            "skipped": True,
            "command": None,
            "cwd": str(dbt_dir),
            "returncode": None,
        }
        context.log.info(
            f"[RUN][dbt] skipped=True reason=no_new_data "
            f"snapshot_id={ingestion_result.get('snapshot_id')}"
        )
        context.add_output_metadata(
            {
                "skipped": True,
                "reason": "no_new_data",
                "cwd": MetadataValue.path(output["cwd"]),
                "returncode": output["returncode"],
            }
        )
        return output

    dbt_exe = Path(sys.executable).parent / "dbt.exe"
    cmd = [str(dbt_exe), "build", "--debug"]

    context.log.info(f"Running: {' '.join(cmd)} (cwd={dbt_dir})")

    result = subprocess.run(
        cmd,
        cwd=str(dbt_dir),
        text=True,
        env=os.environ,
    )


    if result.returncode != 0:
        if result.stderr:
            context.log.error(result.stderr)
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            result.stdout,
            result.stderr,
        )

    output = {
        "skipped": False,
        "command": " ".join(cmd),
        "cwd": str(dbt_dir),
        "returncode": result.returncode,
    }

    context.log.info(
        f"f[RUN][dbt] skipped=False returnCode={output['returncode']} "
        f"snapshot_id={ingestion_result.get('snapshot_id')}"
    )

    context.add_output_metadata(
        {
            "skipped": False,
            "command": output["command"],
            "cwd": MetadataValue.path(output["cwd"]),
            "returncode": output["returncode"],
        }
    )

    return output