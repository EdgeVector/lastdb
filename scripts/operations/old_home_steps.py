"""What a node holds of an old home: records by primary key, groups by alternate key, and the file."""
import hashlib
from pathlib import Path
import sys

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from old_home_nodes import ALPHA, BETA, GROUPS  # noqa: E402
from personal_proof_nodes import wait  # noqa: E402
import v1_records as v1  # noqa: E402


def group_keys(device, group):
    """Every key of one group, read by the alternate key, page by page."""
    keys, cursor = [], None
    while True:
        page = v1.read_items(device.node, [v1.range_read(BETA, {"hash": group}, via="by_group", limit=100, cursor=cursor)])[0]
        keys.extend(row["key"]["hash"] for row in page["records"])
        cursor = page.get("next_cursor")
        if cursor is None:
            return sorted(keys)


def file_of(device, directory):
    item = v1.point(BETA, {"hash": "bf"}, file=True)
    item["file_dir"] = str(directory)
    rows = v1.read_items(device.node, [item])[0]["records"]
    if not rows or not rows[0].get("file") or not rows[0]["file"].get("path"):
        return None
    info = rows[0]["file"]
    data = Path(info["path"]).read_bytes()
    return {"name": info["name"], "media_type": info["media_type"], "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest()}


def view(device, directory):
    """Everything a device reads of the old data: both schemas, each group, and the file."""
    directory.mkdir(parents=True, exist_ok=True)
    return {"alpha": device.records(ALPHA), "beta": device.records(BETA),
            "groups": {group: group_keys(device, group) for group in GROUPS}, "file": file_of(device, directory)}


def expected_view(expected):
    return {"alpha": expected["alpha"], "beta": expected["beta"], "groups": expected["groups"], "file": expected["file"]}


def seen(device):
    try:
        return view(device, device.out / "files")
    except RuntimeError:
        return None


def wait_view(device, expected, what, seconds=300):
    """Wait until the device reads exactly what the proof wrote."""
    wanted = expected_view(expected)
    wait(what, lambda: seen(device) == wanted, seconds=seconds)
