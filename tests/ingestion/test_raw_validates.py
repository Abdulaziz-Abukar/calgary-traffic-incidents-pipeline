import json
from pathlib import Path
from ingestion.socrata_models import TrafficIncidentRow

def test_raw_row_validates():
    payload = json.loads(Path("data/samples/raw_2026_01_24.json").read_text())
    raw_row = payload[0]
    TrafficIncidentRow.model_validate(raw_row)