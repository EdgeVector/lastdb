"""Exercise a Social circle's whole-database reads and scoped writes in DEV."""

from contextlib import closing
import json
import os
import sys
import uuid

from node import Connection
from signed_files import retry, run_operation, summary
from social_app import call, observe_access, window
from social_log_views import inbox, pending_positions, published_views, scope_for
from support import OperationError, decode_json, require

SCOPE = {"name": "whole-database", "version": 1}
ALBUM = "dev-albums/Album"
COMMENT = "dev-comments/Comment"
REFUSALS = (
    ("select writable schemas", "circle_invalid_schema_selection"),
    ("save verified contacts", "circle_contact_unavailable"),
    ("share_configuration_unconfirmed", "circle_lastdb_unconfirmed"),
    ("invalid_share_configuration", "circle_lastdb_rejected_config"),
    ("share_update_pending", "circle_update_pending"),
    ("mismatched sharing acknowledgement", "circle_ack_mismatch"),
    ("changed while you edited", "circle_revision_changed"),
    ("POST /api/mutation", "circle_private_mutation_refused"),
    ("POST /api/schemas/declare", "circle_private_schema_refused"),
    ("POST /api/queries/batch", "circle_contact_batch_refused"),
    ("LastDB could not read a contact", "circle_contact_read_refused"),
)


def declare(node, database, schema, *, expanded=False):
    namespace, name = schema.split("/")
    fields = {"text": "Sample text."}
    if expanded:
        fields["caption"] = "Optional caption on the same record."
    return node.call(f"/api/databases/{database}/schemas/declare", {
        "namespace": namespace, "schema": {
            "name": name, "descriptive_name": name, "purpose_statement": f"Disposable DEV {name} record.",
            "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
            "fields": list(fields), "field_types": dict.fromkeys(fields, "String"), "field_descriptions": fields,
        },
    })["definition_id"]


def mutation(schema, definition, key, text):
    return {"type": "mutation", "schema": schema, "definition_id": definition,
            "key_value": {"hash": key, "range": None}, "alternates": {},
            "mutation_type": "create", "expected": {"type": "absent"}, "record": {"text": text}}


def query(node, database, schema, key):
    return node.call(f"/api/databases/{database}/query", {
        "schema_name": schema, "filter": {"HashKey": key},
    })["results"]


def wait_text(node, database, schema, key, text, stage):
    retry(stage, lambda: query(node, database, schema, key),
          lambda rows: len(rows) == 1 and rows[0]["fields"]["text"] == text)


def submit(node, database, session, update):
    mutation_id = str(uuid.uuid4())
    with closing(Connection(node.socket)) as connection:
        connection.request("POST", f"/api/databases/{database}/mutation", json.dumps(update), {
            "Content-Type": "application/json", "Idempotency-Key": mutation_id,
            "X-LastDB-Database-Session": session,
        })
        with connection.getresponse() as reply:
            require(reply.status == 202, f"circle_write_http_{reply.status}_expected_202")
            body = decode_json(reply.read(65537))
    require(body["mutation_id"] == mutation_id and body["status"] == "submitted", "circle_submission_receipt")
    return mutation_id


def transfer(programs, root, publisher, recipient, api_key):
    name = f"ci-circles-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    with window(publisher) as owner:
        database = create_database(owner, name)
        second = create_database(owner, f"{name}-second")
        definitions = {schema: declare(publisher, database, schema) for schema in (ALBUM, COMMENT)}
        for schema in (ALBUM, COMMENT):
            publisher.call(f"/api/databases/{database}/mutation",
                           mutation(schema, definitions[schema], "seed", f"Initial {schema}"))
        definition = declare(publisher, second, COMMENT)
        publisher.call(f"/api/databases/{second}/mutation", mutation(COMMENT, definition, "seed", "Second database"))
        # Enroll the stopped publisher, then record its connected boot identity.
        # Enrolling a running node can re-exec it to enable local capture.
        publisher.stop()
        connect(programs, publisher, api_key)
        scope = scope_for(publisher, api_key)
        publisher.keep_saved_credentials = True
        publisher.start()
        contact = root / "recipient.contact"
        reader = programs.command(recipient.home, ["share", "contact", "--out", str(contact)])["recipient_public_key"]
        sender = programs.command(publisher.home, ["share", "contact", "--out", str(root / "owner.contact")])["recipient_public_key"]
        require(reader != sender, "circle_two_identities")
        call(owner, "/api/remember", {"name": "Ana", "nodeKey": reader, "contactCode": contact.read_text().strip()})
        ready = retry("circle_cloud_ready", lambda: call(owner, f"/api/database?databaseId={database}"),
                      lambda row: row["sharing"].get("revision") == "0")
        require(set(definitions).issubset(ready["schemas"]), "circle_schema_catalog_missing")
        retry("circle_second_cloud_ready", lambda: call(owner, f"/api/database?databaseId={second}"),
              lambda row: row["sharing"].get("revision") == "0")
        circle = {"id": str(uuid.uuid4()), "name": "Family", "members": [reader],
                  "links": {database: [COMMENT], second: []}}
        try:
            require(edit_circle(owner, circle) == {database: "1", second: "1"}, "circle_grant_revisions")
            receipt = call(owner, "/api/circle-invitation", {"databaseId": database, "nodeKey": reader})
            require(receipt["publisher"] == sender and receipt["databaseId"] == database, "circle_sealed_invitation")
            with window(recipient) as remote:
                exercise(publisher, recipient, remote, database, receipt, definitions, scope)
                second_receipt = call(owner, "/api/circle-invitation", {"databaseId": second, "nodeKey": reader})
                call(remote, "/api/accept", {"receipt": second_receipt, "publisher": sender, "name": "second"})
                wait_text(recipient, second, COMMENT, "seed", "Second database", "circle_second_database_read")
                observe_access(remote, second, "active", 1, write_schemas=[])
                definitions[COMMENT] = exercise_upgrade(publisher, recipient, remote, database, definitions[COMMENT])
                call(owner, "/api/revoke", {"databaseId": second, "nodeKey": reader})
                read_circle = {**circle, "links": {database: [], second: [COMMENT]}}
                require(edit_circle(owner, read_circle) == {database: "2", second: "2"}, "circle_reader_revision")
                observe_access(remote, second, "revoked", 2)
                exercise_reader(publisher, recipient, remote, database, definitions, scope)
                report("One reusable circle granted distinct access to two databases and preserved an explicit revoke after an edit.")
        finally:
            failed = sys.exc_info()[0] is not None
            try:
                for target in (database, second):
                    cleanup(owner, target, reader)
            except OperationError:
                summary("The final DEV recipient revocation could not be confirmed.")
                if not failed:
                    raise


def create_database(owner, name):
    database = call(owner, "/api/create", {"name": name, "sharingMode": "whole_database"})["id"]
    report(f"Owned DEV mutation log {database} for {name}.")
    return database


def report(message):
    print(message, flush=True)
    summary(message)


def edit_circle(owner, circle):
    edit = {"circle": circle}
    preview = call(owner, "/api/circles/preview", edit, refusals=REFUSALS)
    require(all("error" not in row for row in preview["databases"]), "circle_reusable_preview")
    response = call(owner, "/api/circles/apply", {**edit, "token": preview["token"]}, refusals=REFUSALS)
    require(not response["pending"], "circle_reusable_apply")
    return {row["databaseId"]: row["revision"] for row in response["databases"]}


def cleanup(owner, database, reader):
    path = f"/api/database?databaseId={database}"
    roster = call(owner, path)["sharing"]
    if roster.get("status") == "pending":
        roster = call(owner, "/api/retry", {"databaseId": database}, refusals=REFUSALS)["sharing"]
    members = roster.get("members")
    require(members is not None, "circle_cleanup_roster_unavailable")
    if any(row["state"] == "active" for row in members):
        call(owner, "/api/revoke", {"databaseId": database, "nodeKey": reader}, refusals=REFUSALS)
        revoked = call(owner, path)["sharing"].get("members") or []
        require(len(revoked) == 1 and revoked[0]["state"] == "revoked", "circle_cleanup_revoked")
        print("ExeMem confirmed the final recipient revocation.", flush=True)
        summary("ExeMem confirmed the final recipient revocation.")


def connect(programs, publisher, api_key):
    receipt = programs.command(publisher.home, [
        "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
        "--expected-user-hash", publisher.account, "--json",
    ], input_bytes=api_key, timeout=20)
    require(receipt["environment"] == "dev" and receipt["account_id"] == publisher.account
            and receipt["credentials_saved"] is True, "circle_dev_connection")


def exercise(publisher, recipient, remote, database, receipt, definitions, scope):
    accepted = call(remote, "/api/accept", {"receipt": receipt, "publisher": receipt["publisher"], "name": "family"})
    require(accepted["id"] == database and accepted["sharingMode"] == "whole_database", "circle_received_database")
    for schema in (ALBUM, COMMENT):
        wait_text(recipient, database, schema, "seed", f"Initial {schema}", "circle_whole_database_read")
    summary("Ana read the seeded records in both Album and Comment.")
    observe_access(remote, database, "active", 1, role="submit", write_schemas={"names": [COMMENT]})
    updates = [
        mutation(ALBUM, definitions[ALBUM], "blocked", "Cannot write"),
        mutation(COMMENT, definitions[COMMENT], "allowed", "From Ana"),
        mutation(COMMENT, definitions[COMMENT], "seed", "A create cannot replace the seed"),
    ]
    queue_and_restart(publisher, recipient, remote, database, scope, 1, updates)
    for node in (publisher, recipient):
        wait_text(node, database, COMMENT, "allowed", "From Ana", "circle_authorized_write_applied")
        require(not query(node, database, ALBUM, "blocked"), "circle_forbidden_write_missing")
        wait_text(node, database, COMMENT, "seed", f"Initial {COMMENT}", "circle_invalid_create_discarded")
    message = "The publisher published the valid Comment and discarded the Album and invalid Comment mutations."
    print(message, flush=True)
    summary(message)


def exercise_upgrade(publisher, recipient, remote, database, previous):
    new = declare(publisher, database, COMMENT, expanded=True)
    require(new != previous, "circle_schema_upgrade_new_definition")
    seeded = mutation(COMMENT, new, "upgrade", "Before edit")
    seeded["record"]["caption"] = "Keep the optional field"
    publisher.call(f"/api/databases/{database}/mutation", seeded)
    wait_text(recipient, database, COMMENT, "upgrade", "Before edit", "circle_upgraded_record_received")
    observe_access(remote, database, "active", 1, role="submit", write_schemas={"names": [COMMENT]})
    current = query(recipient, database, COMMENT, "upgrade")[0]
    update = mutation(COMMENT, new, "upgrade", "After edit")
    update.update(mutation_type="replace", expected={"type": "revision", "revision": current["revision"]},
                  record={**current["fields"], "text": "After edit"})
    session = call(remote, "/api/pick", {"spec": SCOPE, "intent": "write", "databaseId": database})["databaseSession"]
    submit(recipient, database, session, update)
    for node in (publisher, recipient):
        wait_text(node, database, COMMENT, "upgrade", "After edit", "circle_upgraded_record_edited")
        require(query(node, database, COMMENT, "upgrade")[0]["fields"] == update["record"],
                "circle_upgraded_fields_preserved")
        wait_text(node, database, COMMENT, "allowed", "From Ana", "circle_old_records_preserved")
    report("Ana edited the upgraded Comment at the same sharing revision and kept its added field and the older record.")
    return new


def exercise_reader(publisher, recipient, remote, database, definitions, scope):
    observe_access(remote, database, "active", 2)
    wait_text(recipient, database, COMMENT, "allowed", "From Ana", "circle_reader_retains_access")
    updates = [mutation(COMMENT, definitions[COMMENT], "reader", "Reader submission")]
    queue_and_restart(publisher, recipient, remote, database, scope, 2, updates)
    for node in (publisher, recipient):
        require(not query(node, database, COMMENT, "reader"), "circle_reader_write_discarded")
    message = "Ana submitted with an empty write scope. The publisher consumed the entry and published no record."
    print(message, flush=True)
    summary(message)


def queue_and_restart(publisher, recipient, remote, database, scope, revision, updates):
    session = call(remote, "/api/pick", {"spec": SCOPE, "intent": "write", "databaseId": database})["databaseSession"]
    publisher.stop()
    try:
        require(not inbox(publisher, scope, database, revision)["entries"], "circle_empty_inbox")
        published_before = published_views(recipient, scope, database, revision)
        queued = [submit(recipient, database, session, update) for update in updates]
        through = pending_positions(publisher, recipient, scope, database, revision, queued, published_before)
        summary("ExeMem queued the mutations in the publisher's inbox and refused Ana's inbox read and settlement. The published log did not advance.")
    finally:
        publisher.start()
    retry("circle_publisher_consumed_submissions", lambda: inbox(publisher, scope, database, revision),
          lambda page: int(page["next"]) >= through and not page["entries"])


if __name__ == "__main__":
    run_operation(transfer)
