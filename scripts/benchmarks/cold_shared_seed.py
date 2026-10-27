#!/usr/bin/env python3
"""CI-only declaration comparison with the real development Schema Service."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import time

from declaration_service import Service, build_service
from node import Node, UnixConnection, file_hash, save
from report import compare
from workload import describe, encoded

PAIRS, SAMPLES = 4, 256
APPS, COMPAT = "/api/apps/declare-schema", "/api/schemas/declare"
DESCRIPTIONS = {"id": "a generic opaque primary identifier",
                "category": "a category label classifying a piece of content",
                "title": "a title or headline for a piece of content",
                "description": "a longer textual description of an entity or piece of content"}


def proposal(expanded=False, check=False, compat=False, ttl=None, confirmed=False):
    fields = ["category", "id", "title"] + (["description"] if expanded else [])
    result = {"namespace" if compat else "app_id": "declaration-benchmark",
              "schema": {"name": "Books", "descriptive_name": "Library Book",
                         "purpose_statement": "Records of books in a library catalog",
                         "schema_type": "HashRange",
                         "key": {"hash_field": "category", "range_field": "id"},
                         "fields": fields, "field_descriptions": {f: DESCRIPTIONS[f] for f in fields}},
              "intent": "check" if check else "catalog_sync"}
    if ttl is not None:
        result["retention"] = {"ttl_seconds": ttl, "confirm_expire_existing": confirmed}
    return result


def stable(value):
    if isinstance(value, dict):
        return {k: stable(v) for k, v in sorted(value.items())
                if k not in {"user_hash", "event_id", "audit_event_id", "occurred_at", "owner_user_id"}}
    if isinstance(value, list):
        return [stable(v) for v in value]
    return value


def rpc(node, path, body, expected=200):
    body = body if isinstance(body, bytes) else encoded(body)
    conn = UnixConnection(node.socket)
    started = time.perf_counter_ns()
    try:
        conn.request("POST", path, body, {"Content-Type": "application/json",
                     "X-LastDB-Client": "github-declaration-benchmark"})
        reply = conn.getresponse()
        raw = reply.read()
        elapsed = (time.perf_counter_ns() - started) / 1e6
        if reply.status != expected:
            raise RuntimeError(f"{path}: expected {expected}, got {reply.status}: {raw[:1500]!r}")
        result = json.loads(raw)
        if expected == 200 and result.get("ok") is not True:
            raise RuntimeError(f"The operation lacks its ack: {raw[:1500]!r}")
        return stable(result), elapsed, len(raw)
    finally:
        conn.close()


def audit(node):
    return {name: (node.home / "logs" / name).read_bytes() if (node.home / "logs" / name).exists() else b""
            for name in ("schema-sync-audit.jsonl", "retention-audit.jsonl")}


def observe(node, service):
    return {"service_writes": service.health()["schema_writes"], "audit": audit(node),
            "schemas": node.call("/api/schemas?include_system=true&include_full=true")[0]}


def read_only(node, service, action):
    before = observe(node, service)
    result = action()
    if observe(node, service) != before:
        raise RuntimeError("A check changed installed schemas, service schema writes, or an audit")
    return result


def resolution(result, expected, check=False):
    if result.get("resolution") != expected:
        raise RuntimeError(f"Expected {expected}, got {result}")
    if check and (result.get("dry_run") is not True or result.get("bind_eligible") is not False):
        raise RuntimeError("The check lacks its read-only reply flags")


def data(node, write=False):
    schema = "declaration-benchmark/Books"
    if write:
        mutations = [{"type": "mutation", "schema": schema, "mutation_type": "create",
                      "durability": "durable", "key_value": {"hash": "library", "range": f"book-{i:04d}"},
                      "fields_and_values": {"category": "library", "id": f"book-{i:04d}", "title": f"Book {i}"}}
                     for i in range(32)]
        result = node.call("/api/mutations/batch", {"mutations": mutations})[0]
        if result.get("count") != 32 or result.get("durability") != "durable":
            raise RuntimeError(f"The declared schema did not accept the batch: {result}")
    result = node.call("/api/query", {"schema_name": schema, "fields": ["category", "id", "title"],
                      "filter": {"HashRangeKeys": [["library", f"book-{i:04d}"] for i in range(32)]}, "limit": 32})[0]
    rows = result.get("results", [])
    found = {r["fields"]["id"]: r["fields"]["title"] for r in rows}
    if len(rows) != 32 or found != {f"book-{i:04d}": f"Book {i}" for i in range(32)}:
        raise RuntimeError(f"The known-key batch read lost data: {result}")
    return {"records": 32, "values_sha256": hashlib.sha256(encoded(found)).hexdigest()}


def measure(binary, revision, service_binary, output):
    output.mkdir()
    service, nodes = Service(service_binary, output), []
    proof, metrics, sequence, error = {}, {}, [], None
    raw = gzip.open(output / "samples.jsonl.gz", "wt")

    def call(node, name, body, expected=200, path=APPS):
        result, _, _ = rpc(node, path, body, expected)
        proof[name] = result
        save(output / "operational.json", proof)
        return result

    def timed(node, name, body, outcome=None, path=APPS, expected=200, check=True):
        body = encoded(body)
        request_hash, response_hash, samples = hashlib.sha256(body).hexdigest(), None, []
        started = time.monotonic()
        for number in range(SAMPLES):
            result, elapsed, size = rpc(node, path, body, expected)
            if outcome:
                resolution(result, outcome, check)
            digest = hashlib.sha256(encoded(result)).hexdigest()
            if response_hash is not None and response_hash != digest:
                raise RuntimeError(f"{name} changed replies across identical calls")
            response_hash = digest
            row = {"case": name, "client_request": number, "elapsed_ms": elapsed, "records": 1,
                   "request_sha256": request_hash, "response_sha256": digest, "response_bytes": size}
            samples.append(row)
            raw.write(json.dumps(row) + "\n")
        metrics[name] = describe(samples, time.monotonic() - started)
        sequence.append({"case": name, "requests": SAMPLES, "request_sha256": request_hash,
                         "response_sha256": response_hash})
        save(output / "metrics.json", metrics)

    try:
        service.start()
        for label in ("installed", "fresh"):
            folder = output / label
            folder.mkdir()
            nodes.append(Node(binary, revision, folder, environment={"FOLD_SCHEMA_SERVICE_URL": service.url}))
        node, fresh = nodes
        node.start()
        read_only(node, service, lambda: timed(node, "novel-check", proposal(check=True), "register"))
        registered = call(node, "register", proposal())
        resolution(registered, "register")
        fresh.start()
        read_only(fresh, service, lambda: timed(fresh, "exact-catalog-check", proposal(check=True), "reuse"))
        resolution(call(fresh, "exact-catalog-sync", proposal()), "reuse")
        fresh.stop()
        proof["initial-data"] = data(node, write=True)
        read_only(node, service, lambda: timed(node, "loaded-check", proposal(check=True), "reuse"))
        read_only(node, service, lambda: timed(node, "compat-check", proposal(check=True, compat=True), "reuse", path=COMPAT))
        timed(node, "loaded-sync", proposal(), "reuse", check=False)
        preview = read_only(node, service, lambda: call(node, "expand-check", proposal(True, True)))
        resolution(preview, "expand", True)
        if preview.get("register_reason") != "insufficient_field_coverage":
            raise RuntimeError("The real under-covered candidate lacks its fallback reason")
        expanded = call(node, "expand-sync", proposal(True))
        resolution(expanded, "expand")
        if expanded.get("replaced_schema") != registered["canonical"]:
            raise RuntimeError("Expansion lost its predecessor")
        proof["expanded-data"] = data(node)
        read_only(node, service, lambda: timed(node, "retention-preview", proposal(True, True, ttl=7200), "reuse"))
        retained = call(node, "retention-sync", proposal(True, ttl=7200, confirmed=True))
        if retained["retention"].get("status") != "applied":
            raise RuntimeError("The real retention policy did not apply")
        shorter = read_only(node, service, lambda: call(node, "shorter-check", proposal(True, True, ttl=3600)))
        if not shorter["retention"]["requires_confirmation"] or shorter["retention"]["would_apply"]:
            raise RuntimeError("The shorter policy lacks its confirmation refusal")
        call(node, "shorter-refusal", proposal(True, ttl=3600), 409)
        updated = call(node, "shorter-confirmed", proposal(True, ttl=3600, confirmed=True))
        if updated["retention"].get("status") != "updated":
            raise RuntimeError("The confirmed shorter policy did not update")
        bad = {**proposal(True, True), "unknown_request_key": True}
        read_only(node, service, lambda: timed(node, "rejected-check", bad, expected=400))
        embedded = proposal(True, True)
        embedded["schema"]["retention"] = {"ttl_seconds": 7200}
        for name, body, path in (
                ("embedded-retention", embedded, APPS), ("invalid-ttl", proposal(True, True, ttl=0), APPS),
                ("invalid-app", {**proposal(True, True), "app_id": "wrong/namespace"}, APPS),
                ("invalid-compat", {**proposal(True, True, True), "namespace": "wrong/namespace"}, COMPAT)):
            read_only(node, service, lambda n=name, b=body, p=path: call(node, n, b, 400, p))
        before = audit(node)["schema-sync-audit.jsonl"].count(b"\n")
        call(node, "sync-refusal", {**proposal(True), "app_id": "wrong/namespace"}, 400)
        if audit(node)["schema-sync-audit.jsonl"].count(b"\n") != before + 1:
            raise RuntimeError("The sync refusal lacks its one audit event")
        events = [json.loads(line) for line in audit(node)["schema-sync-audit.jsonl"].splitlines()]
        proof["audit-outcomes"] = {k: sum(e["outcome"] == k for e in events) for k in ("register", "reuse", "expand", "rejected")}
        if any(n == 0 for n in proof["audit-outcomes"].values()):
            raise RuntimeError("A real sync branch lacks an audit")
        proof["service-schema-writes"] = service.health()["schema_writes"]
        proof["retention-audit-events"] = audit(node)["retention-audit.jsonl"].count(b"\n")
        node.stop()
        node.start()
        proof["restart-data"] = data(node)
        repeat = call(node, "restart-reuse", proposal(True, ttl=3600))
        resolution(repeat, "reuse")
        if repeat["canonical"] != expanded["canonical"] or repeat["retention"]["status"] != "unchanged":
            raise RuntimeError("Restart lost the canonical bind or retention")
        proof["read-only-state-unchanged"] = True
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        raw.close()
        for node in reversed(nodes):
            try:
                node.close()
            except BaseException as failure:
                error = f"{error or ''}; node cleanup: {failure}"
        try:
            service.close()
        except BaseException as failure:
            error = f"{error or ''}; service cleanup: {failure}"
        save(output / "operational.json", proof)
        save(output / "sequence.json", sequence)
        save(output / "result.json", {"complete": error is None, "error": error})
    if error:
        raise RuntimeError(error)
    return proof, sequence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}-binary", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    revisions = {k: getattr(args, f"{k}_revision") for k in ("baseline", "candidate")}
    if os.environ.get("GITHUB_ACTIONS") != "true" or any(not re.fullmatch(r"[0-9a-f]{40}", v) for v in revisions.values()):
        raise RuntimeError("Run only in GitHub Actions with full revisions")
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {k: getattr(args, f"{k}_binary").resolve() for k in revisions}
    manifest = {"complete": False, "driver_revision": os.environ["DRIVER_SHA"], "runs": [],
                "recipe": {"pairs": PAIRS, "samples": SAMPLES},
                "binaries": {k: {"revision": revisions[k], "sha256": file_hash(b)} for k, b in binaries.items()}}
    save(args.output / "benchmark.json", manifest)
    service_binary = build_service(args.output)
    first = None
    for pair in range(PAIRS):
        for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
            directory = f"{pair}-{label}"
            observed = measure(binaries[label], revisions[label], service_binary, args.output / directory)
            if first is not None and observed != first:
                raise RuntimeError("Operational replies or request sequences differ across programs or pairs")
            first = observed
            manifest["runs"].append({"pair": pair, "label": label, "directory": directory, "complete": True})
            save(args.output / "benchmark.json", manifest)
    comparison = compare(args.output, manifest)
    save(args.output / "comparison.json", comparison)
    manifest["complete"] = True
    save(args.output / "benchmark.json", manifest)
    lines = ["## Real-service schema declaration comparison", "",
             f"Base: `{revisions['baseline']}`  ", f"Candidate: `{revisions['candidate']}`  ",
             f"Driver: `{manifest['driver_revision']}`", "",
             "Four alternating pairs; 256 closed-loop calls per case per program per pair; all opening samples retained.",
             "Each run uses new generated homes and a new real development Schema Service with the same fixed service build.",
             "Registration, exact and loaded reuse, expansion, batch data continuity, retention, refusals, audits, and restart passed.",
             "Checks left installed schemas, both audits, and service schema-write counts unchanged.",
             "Operational replies and measured request/reply hashes match across both programs and all pairs.",
             "The real service uses its default disabled embedder. No primary data, production catalog, model, or fake resolver response.",
             "Scope excludes semantic composition, unavailable-service recovery, and artificial failure injection.", "",
             "| Case | Base P50 / P95 ms | Candidate P50 / P95 ms | P95 change | Pair changes |",
             "|---|---:|---:|---:|---:|---|"]
    for case, item in comparison.items():
        a, b = item["baseline"], item["candidate"]
        pairs = ", ".join(f"{v:+.1f}%" for v in item["paired_p95_changes_percent"])
        lines.append(f"| {case} | {a['p50_ms']:.3f} / {a['p95_ms']:.3f} | {b['p50_ms']:.3f} / {b['p95_ms']:.3f} | "
                     f"{item['changes_percent']['p95_ms']:+.1f}% | {pairs} |")
    lines += ["", "Registration and expansion are operational checks, not tail-latency estimates.",
              "Shared-runner and localhost-service results establish no fixed production latency guarantee.",
              "Generated homes and identities are removed after normal durable shutdown."]
    summary = "\n".join(lines) + "\n"
    (args.output / "summary.md").write_text(summary)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(summary)


if __name__ == "__main__":
    main()
