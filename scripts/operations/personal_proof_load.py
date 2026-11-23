"""Steps 8 to 10 of the personal database proof: idle turns, kill -9 under write load, and scale."""
import random
import threading
import time

from personal_proof_nodes import (
    BATCH, EPSILON, FIELDS, DELTA, IDS_BOUND, MEBIBYTE, MEMORY_BOUND_BYTES, MEMORY_GROWTH_BYTES, SCALE_WAIT_SECONDS, wait)
from record_runs import definition, require, system_seed
import v1_records as v1


# 8. Idle turns -----------------------------------------------------------------------------

def count_turns(relay, mark):
    return len(relay.requests_since(mark, "log/streams"))


def idle_turn_reads_nothing(relay, a):
    """8. After a restart with everything synced, the turns read no entry and write nothing to the store."""
    wait("A to upload", a.synced_up)
    a.stop()
    mark = len(relay.lines())
    a.start()
    wait("the first turns of A after its restart", lambda: count_turns(relay, mark) >= 2)

    def fingerprint():
        return sorted((str(path.relative_to(a.node.home / "data")), path.stat().st_size, path.stat().st_mtime_ns)
                      for path in (a.node.home / "data" / "disk-run").rglob("*") if path.is_file())

    first = fingerprint()
    turns = count_turns(relay, mark)
    wait("three more turns", lambda: count_turns(relay, mark) >= turns + 3)
    require(fingerprint() == first, "an idle turn wrote to the store")
    reads = relay.requests_since(mark, "log/read")
    require(not reads, f"a restart with everything synced read entries from the relay: {reads!r}")
    print("8. After a restart with everything synced, the first turns read no entry from the relay and "
          "wrote nothing to the store", flush=True)
    return {"turns_checked": count_turns(relay, mark)}


# 9. Kill -9 under load ---------------------------------------------------------------------

def kill_under_load(relay, a, b, rounds):
    """9. Kill -9 the daemon of A repeatedly while it writes. Nothing is lost and nothing is repeated."""
    system_seed(a.node, definition(EPSILON, "Hash", FIELDS, description_prefix="Personal proof "))
    wait("A to upload", a.synced_up)
    base = a.spool_state()["head"]
    acknowledged, uncertain = set(), set()
    stop, up = threading.Event(), threading.Event()
    up.set()

    def writer():
        batch = 0
        while not stop.is_set():
            if not up.wait(timeout=1):
                continue
            keys = [f"k{batch}-{i}" for i in range(10)]
            batch += 1
            try:
                a.write([v1.create_item(EPSILON, {"hash": key}, {"id": key, "note": "load"}) for key in keys])
                acknowledged.update(keys)
            except Exception:  # noqa: BLE001 the daemon was killed under this call: the writes may or may not have committed
                uncertain.update(keys)

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    for _ in range(rounds):
        time.sleep(random.uniform(2.0, 4.0))
        up.clear()
        a.crash()
        a.start()
        up.set()
    time.sleep(2)
    stop.set()
    thread.join(timeout=60)
    wait("A to upload after the kills", a.synced_up, seconds=300)
    present = set(a.keys(EPSILON))
    require(acknowledged <= present, f"A lost {len(acknowledged - present)} acknowledged writes")
    require(present <= acknowledged | uncertain, "A holds writes that nobody made")
    head = a.spool_state()["head"]
    require(head - base == len(present), f"A's spool holds {head - base} writes of the load, A holds {len(present)} records")
    # A snapshot of the proof dropped the entries it covers, so the stream starts after its floor.
    sequences = relay.stream_entries(a.database, a.device_id())
    floor = relay.stream_floor(a.database, a.device_id())
    require(sequences == list(range(floor + 1, head + 1)),
            f"the relay holds {len(sequences)} entries of A after floor {floor} for {head} writes")
    require(a.checkpoint()["writer_seq"] == head, f"A confirmed {a.checkpoint()!r} for {head} writes")
    require(a.spool_bytes()[1] == 0, "the spool of A still holds entries after the relay confirmed them")
    wait("B to hold every record of the load", lambda: set(b.keys(EPSILON)) == present if b.has_schema(EPSILON) else False)
    require(b.records(EPSILON) == a.records(EPSILON), "B differs from A after the kills")
    print(f"9. {rounds} kills under load: {len(acknowledged)} acknowledged and {len(present - acknowledged)} "
          f"uncertain writes are on A, each once on the relay ({head} entries); B equals A and A's spool is empty", flush=True)
    return {"kills": rounds, "records": len(present), "acknowledged": len(acknowledged), "relay_entries": head}


# 10. Scale ---------------------------------------------------------------------------------

def write_many(a, schema, first, count):
    for start in range(first, first + count, BATCH):
        stop = min(start + BATCH, first + count)
        a.write([v1.create_item(schema, {"hash": f"s{key:07d}"}, {"id": f"s{key:07d}", "note": f"scale-{key}"})
                 for key in range(start, stop)])


def memory_report(devices):
    return {device.name: device.peak_memory_bytes() for device in devices}


def scale(relay, a, b, writes):
    """10. Tens of thousands of writes: memory does not grow with them, the spool empties, idle turns read nothing."""
    system_seed(a.node, definition(DELTA, "Hash", FIELDS, description_prefix="Personal proof "))
    wait("B to hold the scale schema", lambda: b.has_schema(DELTA))
    quarter = writes // 4
    report = {"writes": writes}
    for label, first, count in (("quarter", 0, quarter), ("all", quarter, writes - quarter)):
        started = time.monotonic()
        write_many(a, DELTA, first, count)
        written = time.monotonic()
        wait(f"A to upload {first + count} writes", a.synced_up, seconds=SCALE_WAIT_SECONDS)
        uploaded = time.monotonic()
        wait(f"B to hold {first + count} records", lambda: len(b.keys(DELTA)) == first + count, seconds=SCALE_WAIT_SECONDS)
        applied = time.monotonic()
        report[label] = {"records": first + count, "write_seconds": round(written - started, 1),
                         "upload_seconds": round(uploaded - written, 1), "apply_seconds": round(applied - uploaded, 1),
                         "peak_memory_bytes": memory_report((a, b))}
        print(f"   scale {label}: {first + count} records; written {written - started:.0f}s, uploaded "
              f"{uploaded - written:.0f}s, applied by B {applied - uploaded:.0f}s; peak memory "
              f"{ {k: v // MEBIBYTE for k, v in report[label]['peak_memory_bytes'].items()} } MiB", flush=True)
    for device in (a, b):
        early, late = report["quarter"]["peak_memory_bytes"][device.name], report["all"]["peak_memory_bytes"][device.name]
        require(late <= early + MEMORY_GROWTH_BYTES,
                f"{device.name}: peak memory grew from {early // MEBIBYTE} to {late // MEBIBYTE} MiB with the entries")
        require(late <= MEMORY_BOUND_BYTES, f"{device.name}: peak memory {late // MEBIBYTE} MiB passed its bound")
    require(a.records(DELTA) == b.records(DELTA) and len(a.records(DELTA)) == writes, "B differs from A after the scale run")
    total, pending = a.spool_bytes()
    require(pending == 0 and total <= max(MEBIBYTE, 100 * writes),
            f"the spool of A is {total} bytes ({pending} of entries) after the relay confirmed everything")
    # The id records of accepted writes do not grow with the writes: a retry of a write older than
    # the newest IDS_KEPT is taken as a new write, which is what the node can promise.
    ids, accepted = a.id_records(), a.spool_state()["head"]
    require(ids <= IDS_BOUND and (accepted <= IDS_BOUND or ids < accepted // 2),
            f"A holds {ids} id records for {accepted} accepted writes (bound {IDS_BOUND})")
    report["id_records"] = {"files": ids, "accepted_writes": accepted, "bound": IDS_BOUND}
    mark = len(relay.lines())
    wait("idle turns", lambda: count_turns(relay, mark) >= 4)
    reads = relay.requests_since(mark, "log/read")
    require(not reads, f"an idle turn read entries from the relay: {reads!r}")
    report["spool_bytes_after_sync"] = total
    print(f"10. {writes} writes: peak memory stays under {MEMORY_BOUND_BYTES // MEBIBYTE} MiB and grew at most "
          f"{MEMORY_GROWTH_BYTES // MEBIBYTE} MiB from a quarter of them; the spool is {total} bytes; "
          "idle turns read no entry", flush=True)
    print(f"    the spool holds {ids} id records for {accepted} accepted writes", flush=True)
    return report
