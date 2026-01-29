from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict

from .socrata_models import TrafficIncidentRow

@dataclass(frozen=True)
class IngestionMeta:
    snapshot_id: str
    snapshot_ts: datetime   # when pulled from the API
    run_type: str           # "daily" or "backfill"
    query_name: str         # e.g. "incremental_since_watermark" or "backfill_2025_12"


def _iso(dt: datetime | None) -> str | None:
    """
    Convert a datetime to an ISO-8601 UTC string.
    
    - Returns None if the input is None
    - Ensures the timestamp is timezone-aware (assumes UTC if native)
    - Normalizes UTC format using a trailing 'Z'

    Used to make datetime fields JSON-serializable for bronzer-layer output.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.isoformat().replace("+00:00", "Z")

def to_bronze_row(row, r: TrafficIncidentRow) -> Dict[str, Any]:
    """
    Convert a validated API row into canonical bronze schema.
    This is the schema that will be stored long-term.
    """

    # Prefer point coords if present (already validated to match lon/lat in the model)
    lon = float(r.longitude)
    lat = float(r.latitude)

    return {
        # Ingestion metadata
        "snapshot_id": row.snapshot_id,
        "snapshot_ts": _iso(row.snapshot_ts),
        "run_type": row.run_type,
        "query_name": row.query_name,

        # business keys / fields
        "incident_id": r.id,
        "incident_info": r.incident_info,
        "description": r.description,
        "start_ts": _iso(r.start_dt),
        "modified_ts": _iso(r.modified_dt),
        "quadrant": r.quadrant,

        # geo (canonical numeric)
        "longitude": lon,
        "latitude": lat,

        # metrics / misc
        "count": r.count,

        # socrata system fields (incremental + debugging purposes)
        "source_row_id": r.socrata_row_id,
        "source_version": r.socrata_version,
        "source_created_at": _iso(r.socrata_created_at),
        "source_updated_at": _iso(r.socrata_updated_at),
    }