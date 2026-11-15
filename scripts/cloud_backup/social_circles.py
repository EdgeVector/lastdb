"""Exercise a Social circle's whole-database reads and scoped writes in DEV."""

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

from node import Connection
from social_app import call, install, observe_access, sdk, window
from social_declarations import configure_bindings, external_changes, independent_update, revoke_roster
from social_log_views import inbox, pending_positions, published_views, scope_for
from support import OperationError, decode_json, require, retry, run_operation, summary

SCOPE = {"name": "whole-database", "version": 1}
ALBUM = "dev-albums/Album"
COMMENT = "dev-comments/Comment"
STANDARD_COMMENT = "social/Comment"
COMMENT_GRANT = {"names": [COMMENT, STANDARD_COMMENT]}
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


def submit(node, database, session, update, *, mutation_id=None):
    mutation_id = mutation_id or str(uuid.uuid4())
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
    if revision := os.environ.get("SOCIAL_INSTALL_REVISION"):
        install(publisher, root, revision)
    with window(publisher) as owner:
        created = sdk(publisher, "create-shared", name=name)
        require(created["sharingMode"] == "whole_database" and created["name"] == name, "circle_sdk_create")
        database = created["id"]
        report(f"Owned DEV mutation log {database} for {name} through the packaged SDK.")
        second = create_database(owner, f"{name}-second")
        definitions = {schema: declare(publisher, database, schema) for schema in (ALBUM, COMMENT)}
        definition = Path(__file__).resolve().parents[2] / "spec/social/Comment.json"
        definitions[STANDARD_COMMENT] = publisher.call(
            f"/api/databases/{database}/schemas/declare", json.loads(definition.read_text())
        )["definition_id"]
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
        circle = {"id": str(uuid.uuid4()), "name": "Family", "members": [], "links": {}}
        try:
            require(edit_circle(owner, circle) == {}, "circle_starts_as_private_membership")
            links = configure_bindings(publisher, owner, circle, database, second, COMMENT_GRANT["names"])
            circle = {**circle, "links": links, "members": [reader]}
            require(edit_circle(owner, circle) == {database: "1", second: "1"}, "circle_grant_revisions")
            receipt = sdk(publisher, "seal", database=database, nodeKey=reader)
            require(receipt["publisher"] == sender and receipt["databaseId"] == database
                    and receipt["revision"] == "1", "circle_sdk_sealed_invitation")
            with window(recipient) as remote:
                exercise(publisher, recipient, remote, database, receipt, definitions, scope, reader)
                exercise_comment_ownership(publisher, recipient, remote, database,
                                           definitions[STANDARD_COMMENT], scope, reader)
                second_receipt = call(owner, "/api/circle-invitation", {"databaseId": second, "nodeKey": reader})
                call(remote, "/api/accept", {"receipt": second_receipt, "publisher": sender, "name": "second"})
                wait_text(recipient, second, COMMENT, "seed", "Second database", "circle_second_database_read")
                observe_access(remote, second, "active", 1, write_schemas=[])
                definitions[COMMENT] = exercise_upgrade(publisher, recipient, remote, database,
                                                        definitions[COMMENT], scope, sender)
                read_circle = {**circle, "links": {database: [], second: [COMMENT]}}
                preview = call(owner, "/api/circles/preview", {"circle": read_circle})
                call(owner, "/api/revoke", {"databaseId": second, "nodeKey": reader})
                call(owner, "/api/circles/apply", {"circle": read_circle, "token": preview["token"]}, status=400)
                require(edit_circle(owner, read_circle) == {database: "2", second: "2"}, "circle_reader_revision")
                observe_access(remote, second, "revoked")
                exercise_reader(publisher, recipient, remote, database, definitions, scope)
                external_changes(publisher, owner, circle, database, reader, COMMENT)
                independent_update(publisher, owner, circle["id"], database, second, reader, COMMENT)
                report("One reusable circle granted distinct access to two databases and preserved an explicit revoke after an edit.")
        finally:
            failed = sys.exc_info()[0] is not None
            cleanup_error = None
            for target in (database, second):
                try:
                    cleanup(owner, publisher, target)
                except OperationError as error:
                    summary(f"The final DEV recipient revocation for {target} could not be confirmed.")
                    cleanup_error = cleanup_error or error
            if cleanup_error and not failed:
                raise cleanup_error


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


def cleanup(owner, publisher, database):
    path = f"/api/database?databaseId={database}"
    roster = call(owner, path)["sharing"]
    if roster.get("status") == "pending":
        roster = call(owner, "/api/retry", {"databaseId": database}, refusals=REFUSALS)["sharing"]
    require(roster.get("members") is not None, "circle_cleanup_roster_unavailable")
    revoke_roster(publisher, database)
    report("ExeMem confirmed the final recipient revocations.")


def connect(programs, publisher, api_key):
    receipt = programs.command(publisher.home, [
        "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
        "--expected-user-hash", publisher.account, "--json",
    ], input_bytes=api_key, timeout=20)
    require(receipt["environment"] == "dev" and receipt["account_id"] == publisher.account
            and receipt["credentials_saved"] is True, "circle_dev_connection")


def exercise(publisher, recipient, remote, database, receipt, definitions, scope, reader):
    accepted = sdk(recipient, "accept", receipt=receipt, publisher=receipt["publisher"], name="family")
    require(accepted["id"] == database and accepted["sharingMode"] == "whole_database", "circle_received_database")
    for schema in (ALBUM, COMMENT):
        wait_text(recipient, database, schema, "seed", f"Initial {schema}", "circle_whole_database_read")
    album = sdk(recipient, "read", database=database, schema=ALBUM, key="seed")
    second_app = sdk(recipient, "read", database=database, schema=ALBUM, key="seed", appId="album-viewer")
    require(second_app == album, "independent_app_opens_same_published_album")
    require(album["fields"]["text"] == f"Initial {ALBUM}", "circle_sdk_read_shared_content")
    report("The packaged SDK used Social to create a whole-database share, seal a circle invitation and accept it. The recipient read shared data through the LastDB SDK.")
    summary("Ana read the seeded records in both Album and Comment.")
    observe_access(remote, database, "active", 1, role="submit", write_schemas=COMMENT_GRANT)
    sdk_access = next(row for row in sdk(recipient, "list-databases") if row["id"] == database)["selfAccess"]
    require(sdk_access["state"] == "active" and sdk_access["stale"] is False
            and sdk_access["writeSchemas"] == COMMENT_GRANT, "circle_sdk_confirmed_grant")
    created = mutation(COMMENT, definitions[COMMENT], "allowed", "From Ana")
    created["metadata"] = {"share_author": "unverified-author"}
    updates = [
        mutation(ALBUM, definitions[ALBUM], "blocked", "Cannot write"),
        created,
        mutation(COMMENT, definitions[COMMENT], "seed", "A create cannot replace the seed"),
    ]
    queued = queue_and_restart(publisher, recipient, remote, database, scope, 1, updates,
                               check_status=True)
    verify_results(recipient, database, queued)
    verify_settled_retry(publisher, recipient, remote, database, scope, updates[1], queued[1])
    for node in (publisher, recipient):
        wait_text(node, database, COMMENT, "allowed", "From Ana", "circle_authorized_write_applied")
        require(not query(node, database, ALBUM, "blocked"), "circle_forbidden_write_missing")
        wait_text(node, database, COMMENT, "seed", f"Initial {COMMENT}", "circle_invalid_create_discarded")
    stored = query(publisher, database, COMMENT, "allowed")[0]
    require(stored["metadata"]["metadata"]["share_author"] == reader, "circle_comment_author_from_sender")
    message = "The publisher set the new Comment's author from its verified sender and discarded the Album and invalid Comment mutations."
    print(message, flush=True)
    summary(message)


def standard_comment(database, definition, key, text, author):
    update = mutation(STANDARD_COMMENT, definition, key, text)
    update["key_value"] = {"hash": "seed-album", "range": key}
    update["record"] = {
        "target": {"database": database, "schema": ALBUM, "key": "seed"},
        "author_id": author,
        "created_at": "2026-11-14T00:00:00Z",
        "social_comment_text": text,
    }
    return update


def standard_rows(node, database):
    rows = node.call(f"/api/databases/{database}/query", {
        "schema_name": STANDARD_COMMENT,
        "filter": {"HashRangeKeys": [["seed-album", "publisher"], ["seed-album", "receiver"]]},
    })["results"]
    return {row["key"]["range"]: row for row in rows}


def texts(rows):
    return {key: row["fields"]["social_comment_text"] for key, row in rows.items()}


def replace_comment(update, row, text):
    return {
        **update, "mutation_type": "replace", "expected": {"type": "revision", "revision": row["revision"]},
        "record": {**row["fields"], "social_comment_text": text},
    }


def exercise_comment_ownership(publisher, recipient, remote, database, definition, scope, reader):
    author = hashlib.sha256(reader.encode()).hexdigest()
    owner_comment = standard_comment(database, definition, "publisher", "Publisher wrote this", author)
    mine = standard_comment(database, definition, "receiver", "Ana wrote this", "spoofed-author")
    # Neither publisher-controlled record metadata nor a matching author field grants ownership.
    owner_comment["metadata"] = {"share_author": reader}
    path = f"/api/databases/{database}/mutation"
    publisher.call(path, owner_comment)
    queue_and_restart(publisher, recipient, remote, database, scope, 1, [mine])
    expected = {"publisher": "Publisher wrote this", "receiver": "Ana wrote this"}
    for node in (publisher, recipient):
        rows = retry("standard_comments_visible", lambda: standard_rows(node, database),
                     lambda rows: texts(rows) == expected)
        require(rows["receiver"]["fields"]["author_id"] == author, "standard_verified_author")

    current = standard_rows(publisher, database)["receiver"]
    publisher.call(path, {**replace_comment(mine, current, "Moderated by publisher"), "metadata": {}})
    expected["receiver"] = "Moderated by publisher"
    rows = retry("standard_publisher_edit_visible", lambda: standard_rows(recipient, database),
                 lambda rows: texts(rows) == expected)
    edit = replace_comment(mine, rows["receiver"], "Edited by Ana")
    edit["record"]["author_id"] = "spoofed-editor"
    delete = {**edit, "mutation_type": "delete", "record": {}}
    cross_author = replace_comment(owner_comment, rows["publisher"], "Forbidden edit")
    queue_and_restart(publisher, recipient, remote, database, scope, 1, [cross_author, delete, edit])
    expected["receiver"] = "Edited by Ana"
    for node in (publisher, recipient):
        rows = retry("standard_comment_policy_applied", lambda: standard_rows(node, database),
                     lambda rows: texts(rows) == expected)
        require(rows["receiver"]["fields"]["author_id"] == author, "standard_edited_author_unchanged")
    report("Ana read both standard comments, created and edited her own, and kept her verified author ID. The publisher discarded her deletion and her edit to the publisher's comment.")


def exercise_upgrade(publisher, recipient, remote, database, previous, scope, sender):
    new = declare(publisher, database, COMMENT, expanded=True)
    require(new != previous, "circle_schema_upgrade_new_definition")
    seeded = mutation(COMMENT, new, "upgrade", "Before edit")
    seeded["record"]["caption"] = "Keep the optional field"
    seeded["source_file_name"] = "dev-comment-import.txt"
    seeded["metadata"] = {"import_origin": "publisher", "share_author": sender}
    publisher.call(f"/api/databases/{database}/mutation", seeded)
    wait_text(recipient, database, COMMENT, "upgrade", "Before edit", "circle_upgraded_record_received")
    observe_access(remote, database, "active", 1, role="submit", write_schemas=COMMENT_GRANT)
    current = query(recipient, database, COMMENT, "upgrade")[0]
    require(current.get("metadata") is None, "circle_publisher_metadata_private")
    update = mutation(COMMENT, new, "upgrade", "After edit")
    update.update(mutation_type="replace", expected={"type": "revision", "revision": current["revision"]},
                  record={**current["fields"], "text": "After edit"},
                  source_file_name="replacement.txt", metadata={"share_author": "replacement-author"})
    stale = {**update, "record": {**update["record"], "text": "Stale edit"}}
    queue_and_restart(publisher, recipient, remote, database, scope, 1, [update, stale])
    for node in (publisher, recipient):
        wait_text(node, database, COMMENT, "upgrade", "After edit", "circle_upgraded_record_edited")
        require(query(node, database, COMMENT, "upgrade")[0]["fields"] == update["record"],
                "circle_upgraded_fields_preserved")
        wait_text(node, database, COMMENT, "allowed", "From Ana", "circle_old_records_preserved")
    stored = query(publisher, database, COMMENT, "upgrade")[0]["metadata"]
    require(stored["source_file_name"] == seeded["source_file_name"] and stored["metadata"] == seeded["metadata"],
            "circle_publisher_metadata_preserved")
    report("Ana edited the upgraded Comment at the same sharing revision. The added field, older record, and publisher metadata remained. The publisher discarded the stale edit.")
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


def verify_results(recipient, database, queued):
    unknown = str(uuid.uuid4())
    results = sdk(recipient, "submission-status", database=database, ids=[*queued, unknown])
    require([row["status"] for row in results] == ["discarded", "published", "discarded", "unknown"],
            "circle_sdk_final_proposal_results")
    report("The receiving SDK read the final outcome of each of Ana's proposals by UUID.")


def verify_settled_retry(publisher, recipient, remote, database, scope, update, mutation_id):
    pending = inbox(publisher, scope, database, 1)
    require(not pending["entries"], "circle_retry_starts_with_settled_inbox")
    published = published_views(recipient, scope, database, 1)
    session = call(remote, "/api/pick", {"spec": SCOPE, "intent": "write", "databaseId": database})["databaseSession"]
    submit(recipient, database, session, update, mutation_id=mutation_id)
    results = sdk(recipient, "submission-status", database=database, ids=[mutation_id])
    require([row["status"] for row in results] == ["published"], "circle_retry_preserves_outcome")
    require(inbox(publisher, scope, database, 1) == pending, "circle_retry_does_not_enqueue")
    require(published_views(recipient, scope, database, 1) == published, "circle_retry_does_not_publish")
    report("Ana retried a settled mutation with the same UUID and operation. Its published outcome and both log positions stayed the same.")


def queue_and_restart(publisher, recipient, remote, database, scope, revision, updates, *, check_status=False):
    session = call(remote, "/api/pick", {"spec": SCOPE, "intent": "write", "databaseId": database})["databaseSession"]
    publisher.stop()
    try:
        require(not inbox(publisher, scope, database, revision)["entries"], "circle_empty_inbox")
        published_before = published_views(recipient, scope, database, revision)
        queued = [submit(recipient, database, session, update) for update in updates]
        if check_status:
            results = sdk(recipient, "submission-status", database=database, ids=queued)
            require(all(row["status"] == "submitted" for row in results), "circle_sdk_proposals_pending")
        through = pending_positions(publisher, recipient, scope, database, revision, queued, published_before)
        summary("ExeMem queued the mutations in the publisher's inbox and refused Ana's inbox read and settlement. The published log did not advance.")
    finally:
        publisher.start()
    retry("circle_publisher_consumed_submissions", lambda: inbox(publisher, scope, database, revision),
          lambda page: int(page["next"]) >= through and not page["entries"])
    return queued


if __name__ == "__main__":
    run_operation(transfer)
