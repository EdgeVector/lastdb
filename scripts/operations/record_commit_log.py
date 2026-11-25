"""Acknowledged writes in the commit log survive kill -9, and a clean stop removes the log.

Each record commit appends one frame to `disk-run/commit-log` with one ordered flush and stays in
the memory layer until a seal writes it into a run. These steps write fewer bytes than a seal
takes, so after kill -9 every write below comes back only through the log replay.
"""
from harness import stop_process
from record_runs import require

COUNT = 96


def log_dir(node):
    return node.home / "data" / "disk-run" / "commit-log"


def stats(node):
    return node.call("/api/status")[0]["status"].get("commit_log")


def crash(node):
    process = node.process
    stop_process(process, timeout=10, crash=True)
    node.process = None
    node.log.close()
    node.log = None
    require(process.returncode == -9, f"kill -9 left exit code {process.returncode}")


def check(node, write, delete, read):
    """`write(key, values)`, `delete(key)` and `read(key)` act on one Hash schema."""
    require(log_dir(node).is_dir(), "the running home has no commit log")
    before = stats(node)
    require(before is not None, "/api/status reports no commit_log")
    keys = [f"crash-{n:03d}" for n in range(COUNT)]
    for n, key in enumerate(keys):
        write(key, {"id": key, "bucket": "crash", "value": f"v{n}"})
    delete(keys[0])
    after = stats(node)
    require(after["flushes"] >= before["flushes"] + COUNT + 1,
            f"the writes did not each flush the commit log: {before} then {after}")
    require(after["memory_commits"] > COUNT, f"the writes are not in the memory layer: {after}")
    print(f"Commit log: {after['flushes']} flushes, mean {after['mean_flush_micros']} us, "
          f"slowest {after['slowest_flush_micros']} us", flush=True)
    crash(node)
    require(log_dir(node).is_dir(), "kill -9 left no commit log")
    node.start()
    require(read(keys[0]) is None, "kill -9 lost a logged delete")
    for n, key in enumerate(keys[1:], 1):
        got = read(key)
        require(got == {"id": key, "bucket": "crash", "value": f"v{n}"}, f"{key} after kill -9: {got!r}")
    print(f"{COUNT} acknowledged logged writes and a delete survived kill -9", flush=True)


def check_stopped(node):
    """A clean stop seals the logged commits into a run and removes the log."""
    require(not log_dir(node).exists(), "a clean stop left the commit log")
    print("A clean stop sealed and removed the commit log", flush=True)
