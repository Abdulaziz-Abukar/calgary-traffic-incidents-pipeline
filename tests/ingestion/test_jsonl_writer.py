import json

def test_write_jsonl_roundtrip(tmp_path):
    from ingestion.common import write_jsonl

    out = tmp_path / "out.jsonl"
    rows = [{"a": 1}, {"b": "x"}]

    write_jsonl(out, rows)

    lines = out.read_text().splitlines()
    assert len(lines) == 2
    assert [json.loads(l) for l in lines] == rows
