"""Bounded Linux observations for CI-owned processes, outside request timers.

Process and host counters describe their observed interval. Host counters and
pressure totals are shared observations; they do not establish request causes.
"""

import itertools
import os
from pathlib import Path
import time

MAX_THREADS = 128
CPU_FIELDS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq",
              "steal", "guest", "guest_nice")


def _text(path):
    with path.open() as source:
        value = source.read(65537)
    if len(value) > 65536:
        raise ValueError("The named proc file exceeds its64KiB bound")
    return value


def _stat(path):
    # comm can contain spaces or parentheses. The final ')' ends field2.
    fields = _text(path).rsplit(")", 1)[1].split()
    return {"start_time_ticks": int(fields[19]),
            "process_user_cpu_ticks": int(fields[11]),
            "process_system_cpu_ticks": int(fields[12]),
            "process_minor_faults": int(fields[7]),
            "process_major_faults": int(fields[9])}


def _io(pid):
    raw = dict(line.split(":", 1) for line in _text(Path(f"/proc/{pid}/io")).splitlines())
    byte_fields = ("rchar", "wchar", "read_bytes", "write_bytes", "cancelled_write_bytes")
    names = {name: name if name.endswith("_bytes") else name + "_bytes" for name in byte_fields}
    names.update(syscr="read_calls", syscw="write_calls")
    return {"process_io_" + label: int(raw[name]) for name, label in names.items()}


def _host_cpu():
    with Path("/proc/stat").open() as source:
        line = source.readline(16385)
    if len(line) > 16384 or not line.startswith("cpu "):
        raise ValueError("The host CPU line is absent or exceeds16KiB")
    values = line.split()[1:]
    if len(values) < 4:
        raise ValueError("The host CPU line lacks its required counters")
    return {"host_cpu_" + name + "_ticks": int(value)
            for name, value in zip(CPU_FIELDS, values)}


def _pressure(kind):
    values = {}
    for line in _text(Path("/proc/pressure") / kind).splitlines():
        category, *fields = line.split()
        if category not in ("some", "full"):
            continue
        raw = dict(field.split("=", 1) for field in fields)
        if "total" in raw:
            values[f"{kind}_pressure_{category}_total_us"] = int(raw["total"])
    if not values:
        raise ValueError("The pressure file contains no total counters")
    return values


def _thread_stats(pid):
    # This is one named process directory, bounded before any per-thread read.
    with os.scandir(f"/proc/{pid}/task") as entries:
        names = [entry.name for entry in itertools.islice(entries, MAX_THREADS + 1)]
    if not names or len(names) > MAX_THREADS or any(not name.isdecimal() for name in names):
        raise ValueError("The process task set exceeds its128-thread bound")
    identities, totals = [], [0, 0, 0]
    for name in sorted(names, key=int):
        root = Path(f"/proc/{pid}/task/{name}")
        before = _stat(root / "stat")["start_time_ticks"]
        values = [int(value) for value in _text(root / "schedstat").split()]
        after = _stat(root / "stat")["start_time_ticks"]
        if before != after or len(values) != 3:
            raise ValueError("A thread identity or schedstat shape changed")
        identities.append([int(name), before])
        totals = [total + value for total, value in zip(totals, values)]
    try:
        enabled = int(_text(Path("/proc/sys/kernel/sched_schedstats")).strip()) == 1
    except (OSError, ValueError):
        enabled = None
    return {"identities": identities, "schedstats_enabled": enabled,
            "counters": {"threads_runtime_ns": totals[0],
                         "threads_runqueue_wait_ns": totals[1] if enabled else None,
                         "threads_timeslices": totals[2] if enabled else None}}


def _capture(availability, label, read):
    try:
        value = read()
    except (OSError, ValueError, IndexError, KeyError) as failure:
        availability[label] = {"available": False, "reason": type(failure).__name__}
        return None
    availability[label] = {"available": True}
    return value


def read_counters(pid):
    """Read only named proc paths for one CI-owned PID; report missing sources."""
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError("A positive process ID is required")
    started = time.monotonic_ns()
    result = {"timestamp_ns": started, "unix_us": time.time_ns() // 1000,
              "pid": pid, "process_identity": None, "thread_identities": None,
              "counters": {}, "availability": {},
              "host_cpu_ticks_scope": "All CPUs; user/nice include guest/guest_nice, so do not add guest twice",
              "process_io_scope": "rchar/wchar include cached I/O; read_bytes/write_bytes are storage accounting",
              "scope": "Aggregate process/host observations; shared host signals are correlation only"}
    available, counters = result["availability"], result["counters"]
    result["clock_ticks_per_second"] = _capture(
        available, "clock_ticks", lambda: os.sysconf("SC_CLK_TCK"))
    root = Path(f"/proc/{pid}")
    first = _capture(available, "process_stat", lambda: _stat(root / "stat"))
    if first is not None:
        counters.update({name: value for name, value in first.items() if name != "start_time_ticks"})
    for label, read in (("process_io", lambda: _io(pid)), ("host_cpu", _host_cpu),
                        ("cpu_pressure", lambda: _pressure("cpu")),
                        ("io_pressure", lambda: _pressure("io"))):
        value = _capture(available, label, read)
        if value is not None:
            counters.update(value)
    threads = _capture(available, "threads_schedstat", lambda: _thread_stats(pid))
    if threads is not None:
        counters.update(threads["counters"])
        result["thread_identities"] = threads["identities"]
        available["threads_schedstat"].update(
            schedstats_enabled=threads["schedstats_enabled"],
            runqueue_wait_available=threads["schedstats_enabled"] is True,
            runqueue_wait_scope="Disabled or unknown kernel schedstats means unreported, not zero")
    last = _capture(available, "process_identity_check", lambda: _stat(root / "stat"))
    if first is not None and last is not None and first["start_time_ticks"] == last["start_time_ticks"]:
        result["process_identity"] = {"pid": pid, "start_time_ticks": first["start_time_ticks"]}
    else:
        available["process_identity_check"] = {"available": False, "reason": "identity_unverified"}
    result["capture_elapsed_ns"] = time.monotonic_ns() - started
    return result


def _delta_reason(name, before, after):
    first, last = before.get("counters", {}).get(name), after.get("counters", {}).get(name)
    if first is None or last is None:
        return "counter_unavailable"
    if name.startswith(("process_", "threads_")) and (
            before.get("process_identity") is None
            or before.get("process_identity") != after.get("process_identity")):
        return "process_identity_changed_or_unverified"
    if name.startswith("threads_") and (
            before.get("thread_identities") is None
            or before.get("thread_identities") != after.get("thread_identities")):
        return "thread_set_changed_or_unverified"
    if name.endswith("_ticks") and before.get("clock_ticks_per_second") != after.get("clock_ticks_per_second"):
        return "clock_tick_rate_changed"
    if last < first:
        return "counter_decreased_or_reset"
    return None


def counter_delta(before, after):
    """Subtract stable counters; missing, reset, or changed identities stay None."""
    elapsed = after.get("timestamp_ns", 0) - before.get("timestamp_ns", 0)
    result = {"elapsed_ns": elapsed if elapsed > 0 else None,
              "before_unix_us": before.get("unix_us"), "after_unix_us": after.get("unix_us"),
              "process_identity_same": before.get("process_identity") is not None
                  and before.get("process_identity") == after.get("process_identity"),
              "clock_ticks_per_second": after.get("clock_ticks_per_second"),
              "availability": {"before": before.get("availability", {}),
                               "after": after.get("availability", {})},
              "counters": {}, "unavailable": {}, "derived": {},
              "scope": "Aggregate interval deltas; host counters do not identify request causes"}
    names = set(before.get("counters", {})) | set(after.get("counters", {}))
    for name in sorted(names):
        reason = "nonpositive_interval" if elapsed <= 0 else _delta_reason(name, before, after)
        result["counters"][name] = None if reason else after["counters"][name] - before["counters"][name]
        if reason:
            result["unavailable"][name] = reason
    user = result["counters"].get("process_user_cpu_ticks")
    system = result["counters"].get("process_system_cpu_ticks")
    ticks = result["clock_ticks_per_second"]
    result["derived"]["process_cpu_ns"] = (
        (user + system) * 1_000_000_000 // ticks
        if user is not None and system is not None and ticks and ticks > 0 else None)
    return result
