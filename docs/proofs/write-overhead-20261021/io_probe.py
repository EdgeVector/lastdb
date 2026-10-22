#!/usr/bin/env python3
"""Read cumulative Darwin process I/O counters for one explicitly supplied PID."""

import argparse
import ctypes
import json
import os
import sys
import time


class RusageInfoV4(ctypes.Structure):
    # Mirrors the installed macOS SDK's sys/resource.h rusage_info_v4.
    _fields_ = [("ri_uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64)
        for name in (
            "ri_user_time", "ri_system_time", "ri_pkg_idle_wkups",
            "ri_interrupt_wkups", "ri_pageins", "ri_wired_size",
            "ri_resident_size", "ri_phys_footprint", "ri_proc_start_abstime",
            "ri_proc_exit_abstime", "ri_child_user_time", "ri_child_system_time",
            "ri_child_pkg_idle_wkups", "ri_child_interrupt_wkups",
            "ri_child_pageins", "ri_child_elapsed_abstime", "ri_diskio_bytesread",
            "ri_diskio_byteswritten", "ri_cpu_time_qos_default",
            "ri_cpu_time_qos_maintenance", "ri_cpu_time_qos_background",
            "ri_cpu_time_qos_utility", "ri_cpu_time_qos_legacy",
            "ri_cpu_time_qos_user_initiated", "ri_cpu_time_qos_user_interactive",
            "ri_billed_system_time", "ri_serviced_system_time", "ri_logical_writes",
            "ri_lifetime_max_phys_footprint", "ri_instructions", "ri_cycles",
            "ri_billed_energy", "ri_serviced_energy", "ri_interval_max_phys_footprint",
            "ri_runnable_time",
        )
    ]


def sample_process(pid):
    """Return JSON-safe lifetime counters; compare only matching PID/start identity."""
    if sys.platform != "darwin":
        raise ValueError("this probe requires Darwin proc_pid_rusage")
    if not isinstance(pid, int) or pid < 1:
        raise ValueError("pid must be a positive integer")
    library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    function = library.proc_pid_rusage
    function.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    function.restype = ctypes.c_int
    info = RusageInfoV4()
    if function(pid, 4, ctypes.byref(info)) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    names = (
        "ri_proc_start_abstime", "ri_user_time", "ri_system_time",
        "ri_diskio_bytesread", "ri_diskio_byteswritten", "ri_logical_writes",
        "ri_resident_size", "ri_phys_footprint", "ri_lifetime_max_phys_footprint",
    )
    return {
        "pid": pid,
        "executable_uuid": bytes(info.ri_uuid).hex(),
        "sampled_unix_ns": time.time_ns(),
        "sampled_monotonic_ns": time.monotonic_ns(),
        **{name: getattr(info, name) for name in names},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", required=True, type=int)
    args = parser.parse_args()
    print(json.dumps(sample_process(args.pid), separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        print(f"I/O probe failed: {error}", file=sys.stderr)
        raise SystemExit(1)
