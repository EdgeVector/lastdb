"""Local file references included in the native DEV snapshot and restored by path."""

import hashlib
from pathlib import Path

import v1_records as v1
from support import require, write_private

SCHEMA = "SnapshotGate"
SIZES = (1024, 65536, 262145)


def payload(index):
    unit = hashlib.sha256(f"snapshot-local-file/{index}".encode()).digest()
    return (unit * (1 + SIZES[index] // len(unit)))[:SIZES[index]]


def reads(database_id):
    return [v1.point(SCHEMA, f"{index:06d}", database=database_id) for index in range(len(SIZES))]


def seed(node, database_id, root):
    root.mkdir(mode=0o700)
    pages = v1.read_items(node, reads(database_id))
    require(len(pages) == len(SIZES), "file_seed_read_count")
    mutations = []
    for index, page in enumerate(pages):
        require("error" not in page and page.get("next_cursor") is None
                and len(page.get("records", [])) == 1, "file_seed_record")
        row = page["records"][0]
        path = root / str(index)
        write_private(path, payload(index))
        mutations.append(v1.replace_item(SCHEMA, row["key"], row["record"], row["revision"],
                                        database=database_id, file={"path": str(path)}))
    v1.write_items(node, mutations)
    verify(node, database_id, root, referenced=True)


def verify(node, database_id, root, referenced=False):
    if not referenced:
        root.mkdir(mode=0o700)
    requested = [{**read, "file": True, "file_dir": str(root)} for read in reads(database_id)]
    pages = v1.read_items(node, requested)
    require(len(pages) == len(SIZES), "file_read_count")
    for index, page in enumerate(pages):
        require("error" not in page and page.get("next_cursor") is None
                and len(page.get("records", [])) == 1, "file_read_record")
        row = page["records"][0]
        marker = row.get("file") or {}
        path = Path(marker.get("path") or "")
        wanted = root / (str(index) if referenced else hashlib.sha256(payload(index)).hexdigest())
        require(row.get("key") == {"hash": f"{index:06d}", "range": None}
                and marker.get("size") == SIZES[index], "file_record_marker")
        require(path == wanted and marker.get("local") == str(wanted) and path.is_file(), "file_local_path")
        require(path.read_bytes() == payload(index), "file_restored_bytes")
