"""Read script-facing format constants from their authoritative Rust definitions.

No home, daemon or generated receipt is read. A missing or conflicting source
constant is a configuration error, rather than a guessed layout version.
"""
import ast
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
FORMAT = "fold_db/crates/core/src/storage/format.rs"


def source_constant(path, name):
    source = (ROOT / path).read_text()
    pattern = (r'\bconst\s+' + re.escape(name)
               + r'\s*:[^=]+?=\s*(b?"(?:\\.|[^"\\])*"|[0-9]+)\s*;')
    matches = re.findall(pattern, source)
    if len(matches) != 1:
        raise ValueError(f"expected one authoritative {name} in {path}")
    return ast.literal_eval(matches[0])


HOME_FORMAT = source_constant(FORMAT, "HOME_FORMAT")
NODE_MARKER = source_constant(FORMAT, "RECORD_NODE_BYTES")
BACKUP_PROFILE = source_constant(FORMAT, "RECORD_PROFILE")
CREDENTIALS_FILE = source_constant("lastdb_node/src/cloud.rs", "CLOUD_CREDENTIALS_FILE")
BACKUP_DIR = source_constant("vendor/laststore/src/disk_run/home/mod.rs", "BACKUP_DIR")
if not NODE_MARKER.startswith(f"format={HOME_FORMAT}\n".encode()):
    raise ValueError("the authoritative home version and node marker disagree")
