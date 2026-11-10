"""Refuse a copied home whose node marker is not an admitted format.

Admission compares the marker file to the private and shared byte strings.
This proof copies a stopped generated home, changes that marker, and requires
a second daemon to exit with the unsupported-format error. The copy stays
under the generated temp root. A successful open fails the job.
"""
import http.client
import os
import shutil
import subprocess
import time

from home_format import FORMAT, HOME_FORMAT, NODE_MARKER as PRIVATE_MARKER, source_constant
from node import UnixConnection

MARKER_FILE = source_constant(FORMAT, "NODE_MARKER")
SHARED_MARKER = source_constant(FORMAT, "SHARED_NODE_BYTES")
BOOT_ERROR = source_constant("lastdb_node/src/host/recovery.rs", "LAST_BOOT_ERROR_FILE")
CRASH_REPORTS = source_constant("fold_db/crates/observability/src/crash.rs", "CRASH_REPORTS_DIR")
# admit() rejects every other marker through invalid()
# ("node range admission: {text}") in run_admission.rs.
REFUSAL = "node range admission: the node range format is unsupported"
PANIC_MARKER = "LastDB panic"
DEADLINE_SECONDS = 120


def require(value, message):
    if not value:
        raise RuntimeError(message)


def wrong_bytes(marker):
    require(len(marker) > 2 and marker.endswith(b"\n"), "the private marker has no trailing line")
    changed = bytearray(marker)
    changed[-2] ^= 1
    return bytes(changed)


def older_format(marker):
    prefix = f"format={HOME_FORMAT}\n".encode()
    require(marker.startswith(prefix), "the private marker does not start with the current format")
    claimed = HOME_FORMAT - 1
    require(claimed > 0, "there is no older format to claim")
    return f"format={claimed}\n".encode() + marker[len(prefix):]


def exercise(node):
    require(node.process is None, "stop the daemon before copying its home")
    marker = (node.home / "data" / MARKER_FILE).read_bytes()
    require(marker == PRIVATE_MARKER, "the generated home marker is not the current private marker")
    changed, previous = wrong_bytes(marker), older_format(marker)
    require(changed != previous, "the two refused markers are the same")
    require(changed.startswith(f"format={HOME_FORMAT}\n".encode()),
            "the altered marker no longer claims the current format")
    require(previous.startswith(f"format={HOME_FORMAT - 1}\n".encode()),
            "the older marker does not claim the previous format")
    root = node.root / "format-refusal"
    root.mkdir(mode=0o700)
    refuse(node, root / "wrong-bytes", changed, "altered marker bytes")
    refuse(node, root / "older-format", previous, f"format {HOME_FORMAT - 1} marker")
    print("Format refusal: a copied home with altered marker bytes and a copied home claiming "
          f"format {HOME_FORMAT - 1} both failed open with the unsupported-format error", flush=True)


def refuse(node, directory, marker, label):
    directory.mkdir(mode=0o700)
    home = directory / "home"
    shutil.copytree(node.home, home, symlinks=True)
    path = home / "data" / MARKER_FILE
    require(path.read_bytes() == PRIVATE_MARKER, "the copy changed the node marker")
    require(marker not in (PRIVATE_MARKER, SHARED_MARKER), f"{label} is still an admitted marker")
    path.write_bytes(marker)
    error_file = home / BOOT_ERROR
    error_file.unlink(missing_ok=True)
    reports = crash_reports(home)
    log_path = directory / "daemon.log"
    socket = node.root / "r.sock"
    process = start(node, home, socket, log_path)
    try:
        outcome = wait(process, socket)
        code = process.returncode
    finally:
        stop(process, socket)
    log = log_path.read_text(errors="replace")
    require(outcome != "opened", f"the {label} home opened")
    require(PANIC_MARKER not in log and crash_reports(home) == reports,
            f"the {label} home panicked instead of refusing the format")
    require(outcome == "exited" and code is not None and code > 0,
            f"the {label} home did not exit with a format refusal")
    boot = error_file.read_text(errors="replace") if error_file.is_file() else ""
    require(REFUSAL in boot,
            f"the {label} home failed without the unsupported-format error: {boot[-500:]}")
    require("lastdbd: daemon_failed" in log, f"the {label} daemon did not report a controlled failure")
    observed = path.read_bytes() if path.is_file() else b""
    require(observed == marker, f"startup rewrote the {label} node marker")
    shutil.rmtree(directory)
    print(f"Format refusal: {label} exited with the unsupported-format error", flush=True)


def crash_reports(home):
    directory = home / CRASH_REPORTS
    if not directory.is_dir():
        return set()
    return {entry.name for entry in directory.iterdir() if entry.is_file()}


def environment(node, home):
    values = {name: value for name, value in os.environ.items()
              if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))}
    values.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                  LASTDB_HOME=str(home), RUST_MIN_STACK="16777216")
    values.update(node.environment)
    return values


def start(node, home, socket, log_path):
    if socket.exists():
        socket.unlink()
    log = log_path.open("w")
    try:
        process = subprocess.Popen(
            [str(node.binary), "--data-dir", str(home), "--socket-path", str(socket), *node.arguments],
            env=environment(node, home), stdin=subprocess.DEVNULL, stdout=log, stderr=log)
    except Exception:
        log.close()
        raise
    process.log = log
    return process


def serving(socket):
    if not socket.exists():
        return False
    connection = UnixConnection(socket)
    connection.timeout = 2
    try:
        connection.request("GET", "/health")
        response = connection.getresponse()
        response.read()
        return True
    except (OSError, TimeoutError, http.client.HTTPException):
        return False
    finally:
        connection.close()


def wait(process, socket):
    deadline = time.monotonic() + DEADLINE_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return "exited"
        if serving(socket):
            return "opened"
        time.sleep(0.05)
    return "running"


def stop(process, socket):
    if process.poll() is None:
        process.kill()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.wait()
    log = getattr(process, "log", None)
    if log is not None and not log.closed:
        log.close()
    if socket.exists():
        socket.unlink()
