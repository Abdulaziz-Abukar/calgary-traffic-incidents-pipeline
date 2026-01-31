from datetime import datetime, timezone
import secrets
import string

def make_snapshot_id(run_type, query_name):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    alphabet = string.ascii_lowercase + string.digits
    suffix = ''.join(secrets.choice(alphabet) for _ in range(6))
    return f"{timestamp}_{run_type}_{query_name}_{suffix}"
