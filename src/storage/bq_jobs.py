from __future__ import annotations

def assert_job_succeeded(job, *, context: dict | None = None) -> None:
    context = context or {}

    if job.error_result:
        raise RuntimeError(
            f"BigQuery job failed: {job.error_result} | ctx={context}"
        )

    if job.errors:
        raise RuntimeError(
            f"BigQuery job completed with errors: {job.errors} | ctx={context}"
        )
