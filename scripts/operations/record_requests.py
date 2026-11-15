"""Native complete-record requests used by generated-home operations.

Supplies revision conditions and explicit addresses without merging payload fields.
"""

from pathlib import Path
import sys

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness.records import conditioned, coordinate, mutation, read_records  # noqa: E402

__all__ = ["conditioned", "coordinate", "mutation", "read_records"]
