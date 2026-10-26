#!/usr/bin/env python3
"""A free, manual cloud capacity measurement through the real LastDB daemon."""

import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from node import file_hash, save  # noqa: E402
from client import SustainedNode  # noqa: E402
import data  # noqa: E402
from load import run_phase  # noqa: E402
from telemetry import Telemetry  # noqa: E402


GIB = 1024 ** 3


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--gib", type=int, choices=(1, 10), default=1)
    parser.add_argument("--try-large", action="store_true")
    parser.add_argument("--seconds", type=int, default=1800)
    parser.add_argument("--rates", default="100,1000,5000")
    parser.add_argument("--seed-seconds", type=int, default=1800)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--batch", type=int, default=64)
    parser.add_argument("--body-bytes", type=int, default=1024)
    args = parser.parse_args()
    args.rates = [int(rate) for rate in args.rates.split(",")]
    if (not 1 <= args.seconds <= 1800 or not 1 <= args.seed_seconds <= 3600
            or not args.rates or len(args.rates) > 6
            or any(not 1 <= rate <= 10000 for rate in args.rates)
            or not 3 <= args.workers <= 16 or not 1 <= args.batch <= 64
            or not 256 <= args.body_bytes <= 65536):
        parser.error("The requested workload exceeds its bounded cloud budget")
    return args


def crash_recovery(node):
    """Kill the owned process during a serial durable-write stream; keep a bounded ack record."""
    stop = threading.Event()
    pending = threading.Event()
    state = {"last_ack": 0, "errors": []}
    lock = threading.Lock()

    def writer():
        version = 1
        while not stop.is_set():
            pending.set()
            try:
                result, _, _ = node.call("/api/mutations/batch", data.write_canary_request(version))
                _, failed, error = data.mutation_outcome(result, 1)
                if failed:
                    raise RuntimeError(error)
                with lock:
                    state["last_ack"] = version
                version += 1
            except Exception as error:
                with lock:
                    state["errors"].append(str(error)[:300])
                break
            finally:
                pending.clear()

    thread = threading.Thread(target=writer, name="recovery-writer", daemon=True)
    thread.start()
    try:
        return _crash_and_read(node, thread, stop, pending, state, lock)
    finally:
        stop.set()
        thread.join(timeout=65)
        if thread.is_alive():
            raise RuntimeError("The recovery writer did not stop before node cleanup")


def _crash_and_read(node, thread, stop, pending, state, lock):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        with lock:
            acknowledged = state["last_ack"]
            errors = list(state["errors"])
        if errors:
            raise RuntimeError(f"Recovery writer failed before crash: {errors[0]}")
        if acknowledged >= 4 and pending.wait(0.01):
            break
        time.sleep(0.002)
    else:
        stop.set()
        thread.join(timeout=65)
        raise RuntimeError("No acknowledged durable recovery writes before the crash deadline")
    process = node.process
    killed_at = time.monotonic()
    process.kill()
    process.wait(timeout=10)
    stop.set()
    thread.join(timeout=65)
    if thread.is_alive():
        raise RuntimeError("The crashed writer did not stop")
    with lock:
        acknowledged = state["last_ack"]
    node.log.close()
    node.log, node.process = None, None
    node.start()
    result, _, _ = node.call("/api/query", data.canary_query())
    observed = data.validate_canary(result)
    if observed < acknowledged:
        raise RuntimeError(f"Recovery lost a durable ack: observed {observed}, acknowledged {acknowledged}")
    return {"last_acknowledged_version": acknowledged, "recovered_version": observed,
            "crash_to_verified_read_seconds": time.monotonic() - killed_at,
            "method": "SIGKILL during a serial durable-write stream; no pre-crash flush",
            "scope": "one acknowledged canary key; not exhaustive crash-position coverage"}


def corpus(args, size):
    output = args.output / f"payload-{size}-gib"
    output.mkdir()
    required = max(20 * GIB, (3 * size + 6) * GIB)
    free = shutil.disk_usage("/tmp").free
    if free < required:
        return {"payload_gib": size, "skipped": "insufficient free temporary disk space",
                "required_free_bytes": required, "available_free_bytes": free}
    settings = argparse.Namespace(**vars(args))
    settings.gib = size
    node = SustainedNode(args.binary, args.revision, output)
    report = {"payload_gib": size, "phases": []}
    telemetry = None
    try:
        node.start()
        if node.disk_format() != 2:
            raise RuntimeError("The benchmark requires the real disk format v2")
        telemetry = Telemetry(node, output)
        telemetry.start()
        report["seed"] = data.seed(node, settings, telemetry)
        save(output / "seed.json", report["seed"])
        if report["seed"].get("measurement_failed") or report["seed"].get("errors"):
            raise RuntimeError(f"Seed requests failed: {report['seed'].get('errors')}")
        if not report["seed"].get("complete", True):
            report["incomplete"] = "seed duration cap; the requested corpus size was not reached"
            return report
        for rate in args.rates:
            print(f"START payload={size}GiB offered={rate}/s duration={args.seconds}s", flush=True)
            result = run_phase(node, settings, report["seed"], rate, telemetry)
            report["phases"].append(result)
            save(output / f"rate-{rate}.json", result)
            print(f"END payload={size}GiB offered={rate}/s {json.dumps(result)}", flush=True)
            if result.get("measurement_failed"):
                raise RuntimeError("A load phase contains a correctness or driver failure")
        # Stop the sampler before the deliberately killed process changes PID.
        report["resources"] = telemetry.finish()
        telemetry = None
        report["recovery"] = crash_recovery(node)
        if report["resources"]["errors"]:
            raise RuntimeError("Resource collection failed; the measurement is incomplete")
        return report
    except Exception as error:
        report["error"] = str(error)[:1200]
        return report
    finally:
        if telemetry is not None:
            try:
                report["resources"] = telemetry.finish()
            except Exception as error:
                report["error"] = f"resource finish: {error}"
        try:
            node.close()
        except Exception as error:
            report["cleanup_error"] = str(error)[:1200]
            report.setdefault("error", f"owned-node cleanup: {error}")
        finally:
            save(output / "result.json", report)


def summary(report):
    lines = ["## V2 sustained cloud benchmark", "", f"Source: `{report['revision']}`", "",
             "Standard public GitHub runner only. Generated data; primary unchanged.",
             "GiB means generated body bytes. Actual node files and memory are separate measures.",
             "Rates count application record mutations, not internal storage operations.", ""]
    for run in report["corpora"]:
        lines += [f"### {run['payload_gib']} GiB payload", ""]
        if run.get("skipped") or run.get("incomplete") or run.get("error"):
            lines += [run.get("skipped") or run.get("incomplete") or run.get("error"), ""]
        lines += ["```json", json.dumps(run, indent=2, sort_keys=True), "```", ""]
    lines += ["Limits: finite samples; one machine; 1 KiB generated values; Linux device counters",
              "can omit delayed writeback. Sampled file peaks are not hard space bounds.",
              "These results do not establish the M4 Max limit or long-held read behavior."]
    return "\n".join(lines) + "\n"


def main():
    args = arguments()
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_OS") != "Linux"
            or os.environ.get("GITHUB_REPOSITORY") != "EdgeVector/lastdb"):
        raise SystemExit("This benchmark runs only in LastDB Linux GitHub Actions")
    args.binary = args.binary.resolve(strict=True)
    # The generated daemon does not need GitHub credentials.
    for name in ("GH_TOKEN", "GITHUB_TOKEN"):
        os.environ.pop(name, None)
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"revision": args.revision, "binary_sha256": file_hash(args.binary),
              "platform": platform.platform(), "cpus": os.cpu_count(),
              "settings": {key: str(value) if isinstance(value, Path) else value
                           for key, value in vars(args).items()}, "corpora": []}
    failed = False
    try:
        for size in ([args.gib, 10] if args.try_large and args.gib != 10 else [args.gib]):
            result = corpus(args, size)
            report["corpora"].append(result)
            if result.get("error"):
                raise RuntimeError(f"Corpus measurement failed: {result['error']}")
            if size == args.gib and (result.get("skipped") or result.get("incomplete")):
                raise RuntimeError("The initial requested corpus did not complete")
    except Exception as error:
        report["error"] = str(error)[:1200]
        failed = True
    finally:
        save(args.output / "result.json", report)
        text = summary(report)
        (args.output / "summary.md").write_text(text)
        print(text, flush=True)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
