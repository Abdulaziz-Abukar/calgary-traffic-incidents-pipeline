from types import SimpleNamespace
from pathlib import Path
from typing import Optional
import pytest

import ingestion.runner as runner

def _args(**overrides):
    # mimic argparse.Namespace
    defaults = dict(
        command="backfill",
        month="2025-02",
        since=None,
        page_size=10,
        max_pages=1,
        out="data/tmp/out.jsonl",
        load_to_bq=False,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)



def test_main_skips_bq_when_flag_not_set(tmp_path, monkeypatch, capsys):
    # ----- Arrange -----
    out = tmp_path / "out.jsonl"
    out.write_text('{"x": 1}\n')    # non-empty

    monkeypatch.setattr(runner, "api_base_url", "https://example.test/api")
    monkeypatch.setattr(runner, "parse_args", lambda: _args(command="backfill", out=str(out), load_to_bq=False))


    # ----- Prevent API Work -----
    monkeypatch.setattr(runner, "backfill", lambda **kwargs: None)


    # If this gets called, test should fail
    monkeypatch.setattr(runner, "load_jsonl_to_bq", lambda path: (_ for _ in ()).throw(AssertionError("Should not load to BQ")))

    
    # ----- Act -----
    runner.main()


    # ----- Assert -----
    captured = capsys.readouterr().out
    assert "[bq] skipped load (pull only)" in captured



def test_main_calls_bq_loader_when_flag_set(tmp_path, monkeypatch, capsys):
    # ----- Arrange -----
    out = tmp_path / "out.jsonl"
    out.write_text('{"x": 1}\n{"x": 2}\n')  # non-empty

    monkeypatch.setattr(runner, "api_base_url", "https://example.test/api")
    monkeypatch.setattr(runner, "parse_args", lambda: _args(command="backfill", out=str(out), load_to_bq=True))


    
    monkeypatch.setattr(runner, "backfill", lambda **kwargs: None)

    called: dict[str, Optional[Path]] = {"path": None}

    def fake_load_jsonl_to_bq(path):
        called["path"] = Path(path)
        return 2
    
    monkeypatch.setattr(runner, "load_jsonl_to_bq", fake_load_jsonl_to_bq)


    # ----- Act -----
    runner.main()


    # ----- Assert -----
    assert called["path"] == out
    captured = capsys.readouterr().out
    assert "[bq] loaded 2 rows" in captured



def test_main_skips_when_output_empty_non_backfill(tmp_path, monkeypatch, capsys):
    # ----- Arrange: command=pull but file empty => should skip load and not raise -----
    out = tmp_path / "out.jsonl"
    out.write_text("")  # empty

    monkeypatch.setattr(runner, "api_base_url", "https://example.test/api")
    monkeypatch.setattr(runner, "parse_args", lambda: _args(command="pull", out=str(out), load_to_bq=True))

    monkeypatch.setattr(runner, "incremental", lambda **kwargs: None)

    monkeypatch.setattr(runner, "load_jsonl_to_bq", lambda path: (_ for _ in ()).throw(AssertionError("Should not load to BQ")))


    # ----- Act -----
    runner.main()


    # ----- Assert -----
    captured = capsys.readouterr().out
    assert "[bq] skipped load (no data)" in captured



def test_main_raises_when_backfill_output_empty(tmp_path, monkeypatch):
    # ----- Arrange: backfill + empty file -> should raise -----
    out = tmp_path / "out.jsonl"
    out.write_text("")  # empty

    monkeypatch.setattr(runner, "api_base_url", "https://example.test/api")
    monkeypatch.setattr(runner, "parse_args", lambda: _args(command="backfill", out=str(out), load_to_bq=False))

    monkeypatch.setattr(runner, "backfill", lambda **kwargs: None)

    # ----- Act / Assert -----
    with pytest.raises(RuntimeError, match="Backfill returned no data"):
        runner.main()



def test_main_fails_fast_when_api_base_url_missing(tmp_path, monkeypatch):
    out = tmp_path / "out.jsonl"
    out.write_text('{"x": 1}\n')

    monkeypatch.setattr(runner, "api_base_url", "")
    monkeypatch.setattr(runner, "parse_args", lambda: _args(command="backfill", out=str(out), load_to_bq=False))
    monkeypatch.setattr(runner, "backfill", lambda **kwargs: None)

    with pytest.raises(RuntimeError, match="API_BASE_URL is empty"):
        runner.main()