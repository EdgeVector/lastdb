"""What a node holds of an old home: records by primary key, groups by alternate key, and the file."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from old_home_nodes import ALPHA, BETA, GROUPS, recorded  # noqa: E402
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


def database_of(cli, home):
    out = subprocess.run([str(cli), "--data-dir", str(home), "admin", "identity", "--json"],
                         capture_output=True, text=True, check=True).stdout
    return json.loads(out)["database_id"]


def post_writes(device, expected, tag):
    """Replace and delete old records and create new ones on the running moved home. Returns the count."""
    alpha, beta = expected["alpha"], expected["beta"]
    old = sorted(alpha)
    changes = []
    for key in old[1:200:5]:
        changes.append({"schema": ALPHA, "op": "replace", "key": {"hash": key}, "record": recorded(key, tag)})
        alpha[key] = recorded(key, tag)
    for key in old[2:200:9]:
        if key in alpha and key not in {c["key"]["hash"] for c in changes}:
            changes.append({"schema": ALPHA, "op": "delete", "key": {"hash": key}})
            del alpha[key]
    device.change(changes)
    fresh = [f"{tag}-{n}" for n in range(25)]
    device.write([v1.create_item(ALPHA, {"hash": key}, recorded(key, tag)) for key in fresh])
    alpha.update({key: recorded(key, tag) for key in fresh})
    group = GROUPS[2]
    new_beta = f"{tag}-beta"
    device.write([v1.create_item(BETA, {"hash": new_beta}, recorded(new_beta, tag),
                                 alternates={"by_group": {"hash": group, "range": new_beta}})])
    beta[new_beta] = recorded(new_beta, tag)
    expected["groups"][group] = sorted([*expected["groups"][group], new_beta])
    return len(changes) + len(fresh) + 1
