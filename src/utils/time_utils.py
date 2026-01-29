from datetime import datetime, timezone

def month_bounds(month: str) -> tuple[datetime, datetime]:
    # month = 'YYYY-MM'
    start = datetime.fromisoformat(f"{month}-01").replace(tzinfo=timezone.utc)

    if start.month == 12:
        end = datetime(start.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(start.year, start.month + 1, 1, tzinfo=timezone.utc)

    return start, end