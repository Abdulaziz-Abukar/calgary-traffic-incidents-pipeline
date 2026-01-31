import json
from pathlib import Path
from datetime import datetime, timezone

from ingestion.socrata_models import TrafficIncidentRow
from ingestion.mappers import IngestionMeta, to_bronze_row

def test_bronze_sample_has_expected_fields():
    row = json.loads(Path("data/samples/2025_02.jsonl").read_text().splitlines()[0])

    # ingestion metadata
    assert "snapshot_id" in row
    assert "snapshot_ts" in row
    assert "run_type" in row
    assert "query_name" in row


    # bronze business fields
    assert "incident_id" in row
    assert "start_ts" in row
    assert "source_updated_at" in row


def test_to_bronze_row_injects_meta_and_maps_keys():
    payload = json.loads(Path("data/samples/raw_2026_01_24.json").read_text())
    raw_row = payload[0]
    validated = TrafficIncidentRow.model_validate(raw_row)

    meta = IngestionMeta(
        snapshot_id="snap_test",
        snapshot_ts=datetime(2026, 1, 31, 12, 0, 0, tzinfo=timezone.utc),
        run_type="daily",
        query_name="incremental"
    )

    bronze = to_bronze_row(meta, validated)


    # meta injected
    assert bronze["snapshot_id"] == "snap_test"
    assert bronze["run_type"] == "daily"
    assert bronze["query_name"] == "incremental"

    # key mapping
    assert bronze["incident_id"] == validated.id
    assert bronze["incident_info"] == validated.incident_info