"""Actual app declarations through the node and the in-memory schema service."""

import http.client
import os
from pathlib import Path
import socket
import subprocess
import time

from node_ops import ENVIRONMENT, metadata, molecule, protein, require, write
from node import Node, save
from ci_publisher import CiPublisher


def app_declare(args, output):
    output.mkdir(mode=0o700)
    publisher = CiPublisher()
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_", "SCHEMA_", "EXEMEM", "APP_IDENTITY", "APP_REGISTRY"))}
    environment.update(publisher.environment)
    environment["OBS_FILE_PATH"] = str(output / "schema-service-observability.jsonl")
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
    log = (output / "schema-service.log").open("w")
    service = subprocess.Popen([str(args.schema_service), "--port", str(port)], env=environment,
                               stdin=subprocess.DEVNULL, stdout=log, stderr=log)
    node = None
    try:
        deadline = time.monotonic() + 60
        while True:
            require(service.poll() is None, "schema service exited before health; inspect its log")
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                connection.request("GET", "/v1/health")
                response = connection.getresponse()
                response.read()
                if response.status == 200:
                    break
            except (OSError, http.client.HTTPException):
                pass
            finally:
                connection.close()
            require(time.monotonic() < deadline, "schema service missed its health deadline")
            time.sleep(0.05)
        node = Node(args.candidate, args.candidate_revision, output,
                    environment=dict(ENVIRONMENT, FOLD_SCHEMA_SERVICE_URL=f"http://127.0.0.1:{port}"),
                    temporary_directory=Path(os.environ["RUNNER_TEMP"]).resolve())
        node.start()
        result = declarations(node, publisher, port)
        save(output / "result.json", result)
        return result
    finally:
        try:
            if node is not None:
                node.close()
        finally:
            service.terminate()
            try:
                service.wait(timeout=15)
            except subprocess.TimeoutExpired:
                service.kill()
                service.wait(timeout=5)
            log.close()
            save(output / "service-cleanup.json", {"stopped": service.poll() is not None})


def declarations(node, publisher, port):
    descriptions = {
        "task_record_id": "Unique identifier of a task record",
        "task_group_id": "Identifier of the work group that contains a task",
        "task_owner_id": "Identifier of the person responsible for a task",
        "task_payload": "Text content of a task record",
    }
    definition = {"name": "OwnedTask", "schema_type": "Hash", "key": {"hash_field": "task_record_id"},
                  "fields": list(descriptions), "field_descriptions": descriptions,
                  "field_types": {field: "String" for field in descriptions},
                  "alternate_keys": {"by_group": {"hash_field": "task_group_id", "range_field": "task_record_id", "missing": "reject"}}}
    bodies = [{"app_id": app, "schema": definition} for app in ("app-a", "app-b")]
    before = node.call("/api/schemas")[0]
    for body in bodies:
        checked = node.call("/api/apps/declare-schema", dict(body, intent="check"))[0]
        require(checked["dry_run"] and not checked["bind_eligible"], "check intent returned a bind")
    require(node.call("/api/schemas")[0] == before, "check intent changed the local schema directory")
    # Developers publish definitions first. Owner nodes install the published
    # definitions without receiving developer credentials or skipping auth.
    publisher.publish(port, bodies)
    replies = [node.call("/api/apps/declare-schema", body)[0] for body in bodies]
    entries = [entry for entry in node.call("/api/schemas")[0]["schemas"]
               if entry.get("owner_app_id") in ("app-a", "app-b")]
    require(len(entries) == 2 and all(entry.get("independent_fields") is True for entry in entries),
            "the schema summary lost record ownership or includes duplicate data schemas")
    legacy = dict(definition)
    legacy.pop("alternate_keys")
    try:
        node.call("/api/apps/declare-schema", {"app_id": "app-a", "schema": legacy})
    except RuntimeError as error:
        require("HTTP 409" in str(error) and "independent" in str(error), "an older caller received the wrong refusal")
    else:
        raise RuntimeError("an older caller replaced an independent declaration")
    require(sorted(node.call("/api/schemas")[0]["schemas"], key=lambda entry: entry["name"])
            == sorted(entries, key=lambda entry: entry["name"]),
            "a refused legacy declaration installed a second data schema")
    for body, reply in zip(bodies, replies):
        runtime = body["app_id"] + "/OwnedTask"
        require(reply["canonical"] == runtime and reply["independent_fields"] is True,
                "app declaration lost its owned runtime name")
        require(reply["adapter"]["field_mappers"] == {}, "app declaration created value mappers")
        info = metadata(node, runtime)
        require(not info.get("field_mappers") and info["alternate_keys"] == definition["alternate_keys"],
                "app declaration lost its alternate keys or added value aliases")
        require(protein(node, molecule(node, runtime, "task_payload")) is None, "public app fields joined a protein")
        write(node, runtime, {"hash": "same-key"},
              {"task_record_id": "same-key", "task_group_id": "g1", "task_owner_id": "o1", "task_payload": body["app_id"]})
    a, b = [metadata(node, body["app_id"] + "/OwnedTask") for body in bodies]
    require(a["field_hashes"] == b["field_hashes"], "equal field definitions did not retain their shared identities")
    require(a["field_molecule_uuids"] != b["field_molecule_uuids"], "field definition reuse shared record molecules")
    for body, reply in zip(bodies, replies):
        runtime = body["app_id"] + "/OwnedTask"
        repeated = node.call("/api/apps/declare-schema", body)[0]
        require(repeated["identity_hash"] == reply["identity_hash"], "repeat declare changed the definition identity")
        for name in (runtime, reply["identity_hash"]):
            rows = node.call("/api/query", {"schema_name": name, "key_name": "by_group", "fields": [], "filter": {"HashKey": "g1"}})[0]["results"]
            require(len(rows) == 1 and rows[0]["fields"]["task_payload"] == body["app_id"],
                    "app values crossed namespaces or identity resolution lost the record")
    node.stop()
    node.start()
    for body in bodies:
        rows = node.call("/api/query", {"schema_name": body["app_id"] + "/OwnedTask", "key_name": "by_group", "fields": [], "filter": {"HashKey": "g1"}})[0]["results"]
        require(len(rows) == 1 and rows[0]["fields"]["task_payload"] == body["app_id"],
                "public declaration values or alternate keys changed after reopen")
    result = {"real_schema_service": True, "check_writes_nothing": True,
              "one_owned_schema_per_app": True, "definition_reuse_without_value_reuse": True,
              "identity_alias": True, "repeat_declare": True, "reopen": True}
    return result
