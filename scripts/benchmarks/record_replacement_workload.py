"""Actual-daemon baseline for conditional full records and application read/edit/replace."""

from concurrent.futures import ThreadPoolExecutor
import threading
import time

from alternate_key_workload import install, require, task_definition

RECORDS = 32
ROUNDS = 8
OPERATIONS = RECORDS * ROUNDS
PAYLOAD_SIZES = (128, 4096, 48 * 1024)
FIELD_COUNTS = (4, 16)
ALTERNATE_COUNTS = (0, 2)
MODES = (("engine_full_replace", 1), ("application_read_replace", 1),
         ("application_read_replace", 4), ("application_read_replace", 16))
CALLS = len(PAYLOAD_SIZES) * len(FIELD_COUNTS) * len(ALTERNATE_COUNTS) * len(MODES) * OPERATIONS


def configurations():
    for size in PAYLOAD_SIZES:
        for fields in FIELD_COUNTS:
            for alternates in ALTERNATE_COUNTS:
                yield {"schema": f"Replacement_s{size}_f{fields}_a{alternates}",
                       "size": size, "fields": fields, "alternates": alternates}


def values(config, number, suffix=""):
    return {"record_id": f"r{number:03}", "group_id": "bucket", "owner_id": "owner",
            "payload": "p" * config["size"] + suffix,
            **{f"extra_{n:02}": f"value-{number}-{n}" for n in range(config["fields"] - 4)}}


def declaration(config):
    schema = task_definition(config["schema"], tuple(f"extra_{n:02}" for n in range(config["fields"] - 4)))
    if not config["alternates"]:
        schema["alternate_keys"] = {}
    return schema


def mutation(config, number, fields, expected=None):
    body = {"type": "mutation", "schema": config["schema"], "mutation_type": "update",
            "key_value": {"hash": f"r{number:03}"},
            "fields_and_values": fields, "durability": "durable", "convergence": "sync"}
    if expected is not None:
        body["expected"] = {"type": "value", "field": "payload", "value": expected}
    return body


def query(config, number=None):
    fields = ["record_id", "group_id", "owner_id", "payload"]
    fields.extend(f"extra_{n:02}" for n in range(config["fields"] - 4))
    predicate = {"HashRangeKeys": [[f"r{n:03}", ""] for n in range(RECORDS)]} if number is None else {
        "HashKey": f"r{number:03}"}
    return {"schema_name": config["schema"], "fields": fields, "filter": predicate, "limit": RECORDS}


def verify(node, config, suffix):
    # One batch of known keys covers complete membership; never N point reads.
    reply = node.call("/api/query", query(config))[0]
    actual = {row["fields"]["record_id"]: row["fields"] for row in reply.get("results", [])}
    expected = {f"r{n:03}": values(config, n, suffix) for n in range(RECORDS)}
    require(len(reply.get("results", [])) == RECORDS and actual == expected,
            "replacement benchmark returned incomplete or divergent records")


def execute(node, config, number, mode, before, after):
    """One successful logical update, including its read and serialization when applicable."""
    started = time.perf_counter_ns()
    observed = []
    if mode == "application_read_replace":
        transport = {}
        reply = node.call("/api/query", query(config, number), transport=transport)[0]
        rows = reply.get("results", [])
        require(len(rows) == 1 and rows[0]["fields"] == values(config, number, before),
                "read/edit/replace fetched a partial or unexpected record")
        fields = dict(rows[0]["fields"])
        observed.append(transport)
    else:
        fields = values(config, number, before)
    old_payload = fields["payload"]
    fields["payload"] = "p" * config["size"] + after
    transport = {}
    reply, _, receipt = node.call("/api/mutation", mutation(config, number, fields, old_payload),
                                  receipt=True, transport=transport, complete_records=True)
    elapsed = (time.perf_counter_ns() - started) / 1e6
    require(reply.get("durability") == "durable", "replacement has no durable acknowledgement")
    observed.append(transport)
    return {"latency_ms": elapsed, "receipt": receipt, "successful_updates": 1,
            "rpc_count": sum(item["rpc_count"] for item in observed),
            "retries": sum(item.get("retries", 0) for item in observed),
            **{name: sum(item[name] for item in observed)
               for name in ("request_body_bytes", "response_body_bytes")}}


def measure_case(node, label, pair, config, mode, clients, before, after):
    barrier = threading.Barrier(clients + 1)

    def worker(client):
        barrier.wait(timeout=30)
        completed = []
        for round_number in range(ROUNDS):
            prior = before if round_number == 0 else f"{after}:r{round_number - 1}"
            current = f"{after}:r{round_number}"
            for number in range(client, RECORDS, clients):
                ordinal = round_number * RECORDS + number
                completed.append((ordinal, execute(node, config, number, mode, prior, current)))
        return completed

    with ThreadPoolExecutor(max_workers=clients) as pool:
        futures = [pool.submit(worker, client) for client in range(clients)]
        started = time.perf_counter_ns()
        barrier.wait(timeout=30)
        completed = [item for future in futures for item in future.result()]
        wall_ms = (time.perf_counter_ns() - started) / 1e6
    require(len(completed) == OPERATIONS, "replacement measurement omitted logical operations")
    case = f"replacement_s{config['size']}_f{config['fields']}_a{config['alternates']}_{mode}_c{clients}"
    verify(node, config, f"{after}:r{ROUNDS - 1}")
    return [dict(sample, label=label, pair=pair, case=case, ordinal=number,
                 case_wall_ms=wall_ms, clients=clients, rejected_attempts=sample["retries"])
            for number, sample in completed]


def measure(node, label, pair, records):
    for config in configurations():
        install(node, declaration(config))
        reply = node.call("/api/mutations/batch",
                          [mutation(config, n, values(config, n)) for n in range(RECORDS)])[0]
        require(reply.get("count") == RECORDS and reply.get("durability") == "durable",
                "replacement seed batch lacks complete durable acknowledgement")
        verify(node, config, "")
        suffix = ""
        for index, (mode, clients) in enumerate(MODES):
            after = suffix + f":v{index}"
            records.extend(measure_case(node, label, pair, config, mode, clients, suffix, after))
            suffix = f"{after}:r{ROUNDS - 1}"


def verify_reopened(node):
    suffix = "".join(f":v{index}:r{ROUNDS - 1}" for index in range(len(MODES)))
    for config in configurations():
        verify(node, config, suffix)
