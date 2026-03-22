from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable

from dagster import op, OpExecutionContext


def _iter_files(root: Path, patterns: list[str] | None) -> Iterable[Path]:
    if patterns:
        for pat in patterns:
            yield from root.rglob(pat)
    else:
        yield from root.rglob("*")


@op
def cleanup_raw_files(context: OpExecutionContext, config: dict) -> None:
    """
    Delete raw files older than N days.

    Uses simple dict config instead of Dagster Config (more stable).
    """

    repo_root = Path(__file__).resolve().parents[3]

    paths = config.get("paths", ["data/raw/incremental", "data/raw/backfill"])
    patterns = config.get("include_patterns", ["*.jsonl", "*.ndjson"])
    older_than_days = config.get("older_than_days", 7)
    dry_run = config.get("dry_run", False)

    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)

    deleted = 0
    kept = 0
    bytes_deleted = 0

    for p in paths:
        root = Path(p)
        if not root.is_absolute():
            root = repo_root / root

        if not root.exists():
            context.log.info(f"[cleanup] skip (missing): {root}")
            continue

        for f in _iter_files(root, patterns):
            if not f.is_file():
                continue

            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)

            if mtime < cutoff:
                size = f.stat().st_size

                if dry_run:
                    context.log.info(
                        f"[cleanup][dry-run] would delete: {f} (mtime={mtime.isoformat()})"
                    )
                else:
                    try:
                        f.unlink()
                        deleted += 1
                        bytes_deleted += size
                        context.log.info(
                            f"[cleanup] deleted {f} (mtime={mtime.isoformat()})"
                        )
                    except Exception as e:
                        context.log.error(f"[cleanup] failed to delete {f}: {e}")
            else:
                kept += 1

    context.log.info(
        f"[cleanup] done: deleted={deleted} files, kept={kept} files, "
        f"bytes_deleted={bytes_deleted}, older_than_days={older_than_days}"
    )