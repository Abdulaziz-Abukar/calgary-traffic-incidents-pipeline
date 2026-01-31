import re
from utils.make_snapshot_id import make_snapshot_id

def test_snapshot_id_shape():
    sid = make_snapshot_id("weekly", "backfill")
    assert re.match(r"^\d{14}_weekly_backfill_[a-z0-9]{6}$", sid)