"""Explicit native record requests for owned cloud-recovery homes."""

from pathlib import Path
import sys

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness.records import create, key, replace_row as replace  # noqa: E402

__all__ = ["create", "key", "replace"]
