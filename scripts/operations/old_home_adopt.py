"""Run `adopt_old_home.py` on homes of the old build and check what it does and what it refuses."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from record_runs import require  # noqa: E402


def run(script, home, cli, *more):
    result = subprocess.run([sys.executable, str(script), "--home", str(home), "--lastdb", str(cli), *more],
                            capture_output=True, text=True, check=False)
    print(result.stdout, end="", flush=True)
    print(result.stderr, end="", file=sys.stderr, flush=True)
    return result


def fingerprint(home):
    """Every path of the home with its size and modification time. A run that changed nothing leaves it equal."""
    found = []
    # walk-ok: one generated home of the proof, under its own temporary root
    for directory, folders, files in os.walk(home):
        for name in sorted(files) + sorted(folders):
            path = os.path.join(directory, name)
            info = os.lstat(path)
            found.append((os.path.relpath(path, home), info.st_size, info.st_mtime_ns))
    return sorted(found)


def registry_owner(home):
    return json.loads((home / "databases" / "registry.json").read_text())["owner"]["id"]


def refuses_running(node, cli, script):
    """The script refuses while the daemon runs and changes nothing."""
    owner = registry_owner(node.home)
    result = run(script, node.home, cli)
    require(result.returncode == 2 and "daemon is running" in result.stderr,
            f"the script did not refuse a running daemon: {result.returncode} {result.stderr!r}")
    require(registry_owner(node.home) == owner, "the refused run changed the registry")
    return "refused with status 2"


def refuses_other(cli, script):
    """The script refuses a directory that is not a home of the old build."""
    with tempfile.TemporaryDirectory() as empty:
        result = run(script, Path(empty), cli)
        require(result.returncode == 2, f"the script did not refuse an empty directory: {result.returncode}")


def adopt_and_repeat(home, cli, script, expect_spool):
    """--check changes nothing, the run does what it says, and a second run changes nothing."""
    refuses_other(cli, script)
    spool = home / "data" / "disk-run" / "accepted-v1"
    old_spool = spool.is_dir()
    if expect_spool:
        require(old_spool, "the variant home has no old spool: the old build did not capture without a cloud")
    old_ids = sum(1 for _ in (spool / "ids").iterdir()) if (spool / "ids").is_dir() else 0
    old_state = json.loads((spool / "state.json").read_text()) if (spool / "state.json").is_file() else None
    before = fingerprint(home)
    owner_before = registry_owner(home)
    checked = run(script, home, cli, "--check")
    require(checked.returncode == 0 and "would: write the owner id" in checked.stdout, f"--check: {checked.stdout!r}")
    require(fingerprint(home) == before, "--check changed the home")
    done = run(script, home, cli)
    require(done.returncode == 0 and "done:  write the owner id" in done.stdout, f"the run: {done.stdout!r} {done.stderr!r}")
    require(registry_owner(home) != owner_before, "the registry still holds the old owner id")
    require(not spool.exists(), "the old spool is still in the home")
    aside = sorted(home.parent.glob(f"{home.name}.old-build-*"))
    if old_spool:
        require(bool(aside), "no set-aside directory for the old spool")
        require((aside[0] / "data__disk-run__accepted-v1" / "state.json").is_file(), "the old spool did not move whole")
    after = fingerprint(home)
    again = run(script, home, cli)
    require(again.returncode == 0 and "nothing to do" in again.stdout, f"the second run: {again.stdout!r}")
    require(fingerprint(home) == after, "the second run changed the home")
    return {"old_spool": old_spool, "old_spool_state": old_state, "old_id_records": old_ids,
            "set_aside": [path.name for path in aside]}
