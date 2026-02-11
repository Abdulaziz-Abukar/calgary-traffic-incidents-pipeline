from __future__ import annotations
import os

def require_env(name: str) -> str:
    """Return env var or raise a clear error"""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set in environment variables.")
    return value