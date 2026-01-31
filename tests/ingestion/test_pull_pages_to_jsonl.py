import json
from pathlib import Path
from datetime import datetime, timezone

import ingestion.runner as runner
from ingestion.mappers import IngestionMeta

class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}: {self.text}")
    
    def json(self):
        return self._payload
    

def test_pull_pages_writes_jsonl_and_track_max_updated(tmp_path, monkeypatch):
    # ----- Arrange -----
    # Ensure env base url check passes
    monkeypatch.setattr(runner, "api_base_url", "https://example.test/api")
    monkeypatch.setattr(runner, "app_token", "")    # keep headers simple

    # use real sample bronze rows so we don't need to guess schema
    bronze_lines = Path("data/samples/2025_02.jsonl").read_text().splitlines()
    bronze_rows = [json.loads(line) for line in bronze_lines]

    # simulate two pages of 2 rows, then an empty page to stop
    page1_bronze = bronze_rows[:2]
    page2_bronze = bronze_rows[2:4]

    # because runner validates raw rows then maps to bronze,
    # patch those two steps so the test focuses on paging + IO + max updated logic
    monkeypatch.setattr(runner, "TrafficIncidentRow", type(
        "X", (), {"model_validate": staticmethod(lambda raw: raw)}
    ))

    
    def fake_to_bronze_row(meta, validated):
        # validated will be the "raw" dicts; feed this into already-bronze dicts.
        return validated
    

    monkeypatch.setattr(runner, "to_bronze_row", fake_to_bronze_row)


    # Fake request pages: return dict with "data" as runner supports both dict+data and list.
    responses = [
        FakeResponse({"data": page1_bronze}),
        FakeResponse({"data": page2_bronze}),
        FakeResponse({"data": []}),
    ]

    captured = {"calls": []}


    def fake_post(url, headers=None, json=None, timeout=None):
        captured["calls"].append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return responses.pop(0)
    

    monkeypatch.setattr(runner.requests, "post", fake_post)

    out_path = tmp_path / "out.jsonl"

    meta = IngestionMeta(
        snapshot_id="snap_test",
        snapshot_ts=datetime(2026, 1, 31, 12, 0, 0, tzinfo=timezone.utc),
        run_type="weekly",
        query_name="backfill",
    )


    # ----- Act -----
    max_dt = runner._pull_pages_to_ndjson(
        soql="SELECT * FROM whatever",
        page_size=2,
        max_pages=10,
        meta=meta,
        out_path=str(out_path),
    )


    # ----- Assert: requests/paging -----
    assert len(captured["calls"]) == 3      # two pages + empty page
    assert captured["calls"][0]["json"]["page"]["pageNumber"] == 1
    assert captured["calls"][1]["json"]["page"]["pageNumber"] == 2
    assert captured["calls"][0]["json"]["page"]["pageSize"] == 2

    
    # ----- Assert: file output is JSONL and has 4 rows -----
    lines = out_path.read_text().splitlines()
    assert len(lines) == 4
    parsed = [json.loads(l) for l in lines]
    assert parsed[0]["incident_id"] == page1_bronze[0]["incident_id"]


    # ----- Assert: max_source_updated_at tracked correctly -----
    # Find the expected max from written rows (source_updated_at exists in bronze sample)
    expected_max = max(
        datetime.fromisoformat(r["source_updated_at"].replace("Z", "+00:00"))
        for r in (page1_bronze + page2_bronze)
    ).astimezone(timezone.utc)

    assert max_dt == expected_max


def test_pull_pages_respects_max_pages(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "api_base_url", "https://example.test/api")

    monkeypatch.setattr(runner, "TrafficIncidentRow", type(
        "X", (), {"model_validate": staticmethod(lambda raw: raw)}
    ))
    monkeypatch.setattr(runner, "to_bronze_row", lambda meta, validated: validated)


    # Return non-empty pages forever; max_pages should stop it
    responses = [ FakeResponse( { "data": [ {"source_updated_at": "2025-02-01T09:10:06:828000Z"} ] } ) for _ in range(10) ]
    

    def fake_post(*args, **kwargs):
        return responses.pop(0)
    
    monkeypatch.setattr(runner.requests, "post", fake_post)

    out_path = tmp_path / "out.jsonl"
    meta = IngestionMeta(
        snapshot_id="snap_test",
        snapshot_ts=datetime.now(timezone.utc),
        run_type="weekly",
        query_name="backfill",
    )

    max_dt = runner._pull_pages_to_ndjson(
        soql="SELECT *",
        page_size=1,
        max_pages=2,     # should stop after 2
        meta=meta,
        out_path=str(out_path),
    )

    lines = out_path.read_text().splitlines()
    assert len(lines) == 2
    assert max_dt is not None