import os
import argparse
from pathlib import Path
import json
import requests
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

from src.ingestion.socrata_models import TrafficIncidentRow
from src.ingestion.mappers import IngestionMeta, to_bronze_row
from src.utils.time_utils import month_bounds
from src.storage.bq_loader import load_jsonl_to_bq
from src.storage.bq_silver import run_silver_merge
from src.utils.make_snapshot_id import make_snapshot_id
from src.common.exceptions import require_env

load_dotenv()

API_BASE_URL = require_env("API_BASE_URL")
APP_TOKEN = require_env("APP_TOKEN")

STATE_DIR = 'state'
WATERMARK_PATH = os.path.join(STATE_DIR, "watermark.json")

# Overlap window for incremental pulls when using >= since
WATERMARK_OVERLAP_MINUTES = 5

# -----------------------------------------------------
# CLI
# -----------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()


    sub = parser.add_subparsers(dest='command', required=True)

    # Incremental pulls
    incremental = sub.add_parser('pull')

    incremental.add_argument('--since', required=False, help='ISO datetime... (optional if watermark exists)')
    incremental.add_argument('--page-size', type=int, required=True)
    incremental.add_argument('--max-pages', type=int, required=True)
    incremental.add_argument('--out', required=True)
    incremental.add_argument('--load-to-bq', action="store_true")
    incremental.add_argument('--run-silver-merge', action='store_true')

    
    # Backfill pulls
    backfill = sub.add_parser('backfill')

    backfill.add_argument('--month', required=True, help='YYYY-MM, e.g. 2025-12')
    backfill.add_argument('--page-size', type=int, required=True)
    backfill.add_argument('--max-pages', type=int, required=True)
    backfill.add_argument('--out', required=True)
    backfill.add_argument('--load-to-bq', action="store_true")
    backfill.add_argument('--run-silver-merge', action='store_true')

    return parser.parse_args()


# -----------------------------------------------------
# Shared helpers
# -----------------------------------------------------

def _build_headers() -> dict:
    if not API_BASE_URL:
        raise ValueError('API_BASE_URL is empty. Set it in environment/.env')
    
    headers = {'Content-Type': 'application/json'}
    if APP_TOKEN:
        headers['X-App-Token'] = APP_TOKEN
    return headers

def _iso_z(dt: datetime) -> str:
    """Convert datetime -> UTC ISO string with 'Z' suffix"""
    return dt.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')

def _iso_floating(dt: datetime) -> str:
    # No timezone suffix for Socrata floating_timestamp fields
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")

def _base_select() -> str:
    return (
        "SELECT incident_info, description, start_dt, modified_dt, quadrant, "
        "longitude, latitude, count, id, point, :id, :version, :created_at, :updated_at "
    )

def _ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)

def read_watermark() -> datetime | None:
    """Return last_source_updated_at as datetime (UTC), or None if not found."""
    try:
        with open(WATERMARK_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        s = data.get("last_source_updated_at")
        if not s:
            return None
        # supports "Z", or "+00:00"
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except FileNotFoundError:
        return None
    
def write_watermark(dt: datetime) -> None:
    """Persist last_source_updated_at in UTC ISO 'Z' format."""
    _ensure_state_dir()
    payload = {"last_source_updated_at": _iso_z(dt)}
    with open(WATERMARK_PATH, 'w', encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")

def _pull_pages_to_ndjson(
        *,
        soql: str,
        page_size: int,
        max_pages: int,
        meta: IngestionMeta,
        out_path: str,
) -> tuple[datetime | None, int]:
    
    headers = _build_headers()

    page_count = 0

    total_rows = 0
    distinct_incidents: set[str] = set()
    max_source_updated_at: datetime | None = None


    out_dir = os.path.dirname(out_path) or "."
    os.makedirs(out_dir, exist_ok=True)

    with open(out_path, 'w', encoding='utf-8') as f:
        while page_count < max_pages:
            body = {
                "query": soql,
                "page": {"pageNumber": page_count + 1, "pageSize": page_size},
            }

            response = requests.post(API_BASE_URL, headers=headers, json=body, timeout=30)
            if response.status_code >= 400:
                print("STATUS:", response.status_code)
                print("RESPONSE:", response.text)
                print("SOQL:", soql)
                print("BODY:", json.dumps(body, indent=2))

            response.raise_for_status()

            payload = response.json()

            if isinstance(payload, dict) and "data" in payload:
                rows = payload['data']
            elif isinstance(payload, list):
                rows = payload
            else:
                raise ValueError(
                    f"Unexpected response shape: {type(payload)} keys={getattr(payload, 'keys', lambda: [])()}"
                )
            
            if not rows:
                break

            for raw_row in rows:
                validated = TrafficIncidentRow.model_validate(raw_row)
                bronze = to_bronze_row(meta, validated)

                total_rows += 1

                incident_id = bronze.get("incident_id")
                if incident_id:
                    distinct_incidents.add(str(incident_id))
                

                
                updated = bronze.get("source_updated_at")


                if updated is not None:
                    if isinstance(updated, str):
                        upd_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    else:
                        upd_dt = updated
                    if upd_dt.tzinfo is None:
                        upd_dt = upd_dt.replace(tzinfo=timezone.utc)
                    else:
                        upd_dt = upd_dt.astimezone(timezone.utc)

                    if max_source_updated_at is None or upd_dt > max_source_updated_at:
                        max_source_updated_at = upd_dt


                json.dump(bronze, f, ensure_ascii=False)
                f.write('\n')
            
            page_count += 1
    
    print("\n=== Pull Summary ===")
    print(f"Pages pulled:                       {page_count}")
    print(f"Rows written:                       {total_rows}")
    print(f"Distinct incident_id:               {len(distinct_incidents)}")

    return max_source_updated_at, total_rows

# -----------------------------------------------------
# Entry Functions
# -----------------------------------------------------

def incremental(
        *,
        since: datetime,
        page_size: int,
        max_pages: int,
        out_path: str,
) -> tuple[str, datetime | None, int]:
    run_type = "daily"
    query_name = "incremental"
    snapshot_id = make_snapshot_id(run_type, query_name)

    since_str = _iso_z(since)

    soql = (
        _base_select()
        + f"WHERE :updated_at >= '{since_str}' "
        + "ORDER BY :updated_at ASC, :id ASC"
    )

    meta = IngestionMeta(
        snapshot_id=snapshot_id,
        snapshot_ts=datetime.now(timezone.utc),
        run_type=run_type,
        query_name=query_name,
    )

    new_max, rows_written = _pull_pages_to_ndjson(
        soql=soql,
        page_size=page_size,
        max_pages=max_pages,
        meta=meta,
        out_path=out_path,
    )
    
    return snapshot_id, new_max, rows_written


def backfill(
        *,
        month: str,
        page_size: int,
        max_pages: int,
        out_path: str,
) -> tuple[str, int]:
    
    run_type = "monthly"
    query_name = "backfill"
    snapshot_id = make_snapshot_id(run_type, query_name)


    start_dt, end_dt = month_bounds(month)
    start_date = _iso_floating(start_dt)
    end_date = _iso_floating(end_dt)


    # Backfill by EVENT TIME (start_dt) for that month
    soql = (
        _base_select()
        + f"WHERE start_dt >= '{start_date}' AND start_dt < '{end_date}' "
        + "ORDER BY start_dt ASC, :id ASC"
    )
    meta = IngestionMeta(
        snapshot_id=snapshot_id,
        snapshot_ts=datetime.now(timezone.utc),
        run_type=run_type,
        query_name=query_name,
    )

    _, rows_written = _pull_pages_to_ndjson(
        soql=soql,
        page_size=page_size,
        max_pages=max_pages,
        meta=meta,
        out_path=out_path,
    )

    return snapshot_id, rows_written


def run_pipeline(
    *,
    command: str,
    since: str | datetime | None = None,
    month: str | None = None,
    page_size: int,
    max_pages: int,
    out: str,
    load_to_bq: bool,
    run_silver_merge_flag: bool,
) -> dict:
    if not API_BASE_URL:
        raise RuntimeError("API_BASE_URL is empty. Set it in environment/.env")

    if run_silver_merge_flag and not load_to_bq:
        raise ValueError("--run-silver-merge requires --load-to-bq")

    snapshot_id: str | None = None
    new_max: datetime | None = None
    rows_written: int = 0
    rows_loaded: int = 0
    watermark_before: str | None = None
    watermark_after: str | None = None

    if command == "pull":
        if since:
            if isinstance(since, str):
                since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
            else:
                since_dt = since

            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            else:
                since_dt = since_dt.astimezone(timezone.utc)
        else:
            stored_watermark = read_watermark()
            if stored_watermark is None:
                raise ValueError("No --since provided and no state/watermark.json found.")
            
            since_dt = stored_watermark - timedelta(minutes=WATERMARK_OVERLAP_MINUTES)
        
        watermark_before = _iso_z(since_dt)

        snapshot_id, new_max, rows_written = incremental(
            since=since_dt,
            page_size=page_size,
            max_pages=max_pages,
            out_path=out,
        )

    elif command == "backfill":
        if not month:
            raise ValueError("backfill requires month in YYYY-MM format")

        snapshot_id, rows_written = backfill(
            month=month,
            page_size=page_size,
            max_pages=max_pages,
            out_path=out,
        )
    
    else:
        raise ValueError(f"Unsupported command: {command}")
    
    out_path = Path(out)
    size = out_path.stat().st_size if out_path.exists() else 0

    if command == "backfill" and size == 0:
        raise RuntimeError(f"Backfill returned no data: {out_path}")
    elif size == 0:
        return {
            "command": command,
            "snapshot_id": snapshot_id,
            "rows_written": 0,
            "rows_loaded": 0,
            "output_path": str(out_path),
            "watermark_before": watermark_before,
            "watermark_after": None,
            "silver_merge_job_id": None,
            "loaded_to_bq": False,
            "silver_merge_ran": False,
            "message": f"[bq] skipped load (no data): {out_path}"
        }

    silver_job_id: str | None = None

    if not load_to_bq:
        return {
            "command": command,
            "snapshot_id": snapshot_id,
            "rows_written": rows_written,
            "rows_loaded": 0,
            "output_path": str(out_path),
            "watermark_before": watermark_before,
            "watermark_after": _iso_z(new_max) if new_max else None,
            "silver_merge_job_id": None,
            "loaded_to_bq": False,
            "silver_merge_ran": False,
            "message": f"[bq] skipped load (pull only): command={command} out={out_path}"
        }
    
    rows = load_jsonl_to_bq(out_path)
    rows_loaded = rows or 0

    if run_silver_merge_flag:
        if snapshot_id is None:
            raise RuntimeError("snapshot_id was not set; cannot run silver merge")
        
        silver_job_id = run_silver_merge(snapshot_id)
    
    if command == "pull":
        if new_max is not None:
            write_watermark(new_max)
            watermark_after = _iso_z(new_max)
    
    return {
        "command": command,
        "snapshot_id": snapshot_id,
        "rows_written": rows_written,
        "rows_loaded": rows_loaded,
        "output_path": str(out_path),
        "watermark_before": watermark_before,
        "watermark_after": watermark_after,
        "silver_merge_job_id": silver_job_id,
        "loaded_to_bq": True,
        "silver_merge_ran": bool(run_silver_merge_flag),
        "message": "Pipeline completed successfully"
    }

# -----------------------------------------------------
# main
# -----------------------------------------------------

def main() -> None:
    args = parse_args()

    result = run_pipeline(
        command=args.command,
        since=getattr(args, "since", None),
        month=getattr(args, "month", None),
        page_size=args.page_size,
        max_pages=args.max_pages,
        out=args.out,
        load_to_bq=args.load_to_bq,
        run_silver_merge_flag=args.run_silver_merge,
    )

    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()

# pull command -> python -m src.ingestion.runner pull --since YYYY-MM-DDT00:00:00Z --page-size 1000 --max-pages 10 --out data/raw/incremental/(filename).jsonl --load-to-bq --run-silver-merge
# backfill command -> python -m src.ingestion.runner backfill --month YYYY-MM --page-size 1000 --max-pages 10 --out data/raw/backfill/(filename).jsonl --load-to-bq --run-silver-merge