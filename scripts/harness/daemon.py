"""Owned LastDB daemon lifecycle, marker, and shutdown-receipt helpers."""

import json
import signal
import subprocess
import time


def marker_format(marker):
    """Return the integer `format=` header of a home marker string, or None."""
    if marker is None:
        return None
    first = marker.split("\n", 1)[0]
    return int(first[len("format=") :]) if first.startswith("format=") else None


def read_shutdown_receipt(home):
    """Read `.shutdown_flush_ready` from an owned home if present."""
    receipt_path = home / ".shutdown_flush_ready"
    return json.loads(receipt_path.read_text(encoding="utf-8")) if receipt_path.is_file() else None


def verify_shutdown_receipt(receipt, pid, start_ts=None):
    """Verify a clean daemon shutdown flush receipt for the expected PID (and optional start_ts)."""
    if not isinstance(receipt, dict):
        return False
    if receipt.get("pid") != pid or receipt.get("flush_ok") is not True:
        return False
    if "version" in receipt and receipt.get("version") != 1:
        return False
    if start_ts is not None and receipt.get("start_ts") != start_ts:
        return False
    return True


def stop_process(process, *, timeout=45, kill_timeout=10, crash=False):
    """Terminate (or deliberately SIGKILL) a child daemon and return whether SIGKILL fallback was needed."""
    forced = False
    if process.poll() is None:
        if crash:
            process.send_signal(signal.SIGKILL)
        else:
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            forced = True
            process.kill()
            process.wait(timeout=kill_timeout)
    return forced


def poll_until(timeout_seconds, step, *, interval=0.05, timeout_error="timed out"):
    """Poll `step()` until it returns a truthy result or `timeout_seconds` elapses."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = step()
        if result:
            return result
        time.sleep(interval)
    if callable(timeout_error):
        raise timeout_error()
    if isinstance(timeout_error, BaseException):
        raise timeout_error
    raise RuntimeError(timeout_error)
