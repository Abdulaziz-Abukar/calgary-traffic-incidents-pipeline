import os
import argparse
from pathlib import Path
import json
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv

from .socrata_models import TrafficIncidentRow
from .mappers import IngestionMeta, to_bronze_row
from utils.time_utils import month_bounds
from storage.bq_loader import load_jsonl_to_bq
from utils.make_snapshot_id import make_snapshot_id

load_dotenv()

api_base_url = os.getenv('API_BASE_URL', '')
app_token = os.getenv('APP_TOKEN', '')

STATE_DIR = 'state'
WATERMARK_PATH = os.path.join(STATE_DIR, "watermark.json")

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

    
    # Backfill pulls
    backfill = sub.add_parser('backfill')

    backfill.add_argument('--month', required=True, help='YYYY-MM, e.g. 2025-12')
    backfill.add_argument('--page-size', type=int, required=True)
    backfill.add_argument('--max-pages', type=int, required=True)
    backfill.add_argument('--out', required=True)
    backfill.add_argument('--load-to-bq', action="store_true")

    return parser.parse_args()


# -----------------------------------------------------
# Shared helpers
# -----------------------------------------------------

def _build_headers() -> dict:
    if not api_base_url:
        raise ValueError('API_BASE_URL is empty. Set it in environment/.env')
    
    headers = {'Content-Type': 'application/json'}
    if app_token:
        headers['X-App-Token'] = app_token
    return headers

def _iso_z(dt: datetime) -> str:
    """Convert datetime -> UTC ISO string with 'Z' suffix"""
    return dt.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')

def _iso_floating(dt: datetime) -> str:
    # No timezone suffic for Socrata floating_timestamp fields
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
    """Presist last_source_updated_at in UTC ISO 'Z' format."""
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
) -> datetime | None:
    
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

            response = requests.post(api_base_url, headers=headers, json=body, timeout=30)
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

    return max_source_updated_at

# -----------------------------------------------------
# Entry Functions
# -----------------------------------------------------

def incremental(
        *,
        since: datetime,
        page_size: int,
        max_pages: int,
        out_path: str,
) -> None:
    # snapshot_id = str(uuid.uuid4())
    run_type = "daily"
    query_name = "incremental"
    snapshot_id = make_snapshot_id(run_type, query_name)

    since_str = _iso_z(since)

    soql = (
        _base_select()
        + f"WHERE :updated_at > '{since_str}' "
        + "ORDER BY :updated_at ASC, :id ASC"
    )

    meta = IngestionMeta(
        snapshot_id=snapshot_id,
        snapshot_ts=datetime.now(timezone.utc),
        run_type=run_type,
        query_name=query_name,
    )

    new_max = _pull_pages_to_ndjson(
        soql=soql,
        page_size=page_size,
        max_pages=max_pages,
        meta=meta,
        out_path=out_path,
    )

    
    if new_max is not None:
        write_watermark(new_max)
        print(f"[watermark] updated to { _iso_z(new_max) }")
    else:
        print("[watermark] no rows returned; watermark unchanged")


def backfill(
        *,
        month: str,
        page_size: int,
        max_pages: int,
        out_path: str,
) -> None:
    
    run_type = "weekly"
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

    _pull_pages_to_ndjson(
        soql=soql,
        page_size=page_size,
        max_pages=max_pages,
        meta=meta,
        out_path=out_path,
    )


# -----------------------------------------------------
# main
# -----------------------------------------------------

def main() -> None:
    args = parse_args()

    if not api_base_url:
        raise RuntimeError("API_BASE_URL is empty. Set it in environment/.env")


    if args.command == 'pull':
        if args.since:
            since_dt = datetime.fromisoformat(args.since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            else:
                since_dt = since_dt.astimezone(timezone.utc)
        else:
            since_dt = read_watermark()
            if since_dt is None:
                raise ValueError("No --since provided and no state/watermark.json found.")
        
        incremental(
            since=since_dt,
            page_size=args.page_size,
            max_pages=args.max_pages,
            out_path=args.out,
        )
    
    elif args.command == "backfill":
        backfill(
            month=args.month,
            page_size = args.page_size,
            max_pages=args.max_pages,
            out_path=args.out,
        )

    
    out_path = Path(args.out)
    size = out_path.stat().st_size if out_path.exists() else 0

    if args.command == "backfill" and size == 0:
        raise RuntimeError(f"Backfill returned no data: {out_path}")
    elif size == 0:
        print(f"[bq] skipped load (no data): {out_path}")
        return
    
    if not args.load_to_bq:
        print(f"[bq] skipped load (pull only): command={args.command} out={out_path}")
        return
    
    rows = load_jsonl_to_bq(out_path)
    print(f"[bq] loaded {rows} rows from {out_path}")

if __name__ == "__main__":
    main()
