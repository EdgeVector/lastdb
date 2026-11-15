"""Set up databases from the versioned schemas shipped with Social."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re

from lastdb import LastDBError, SocialError


SHARED_ALBUMS = "shared-albums"
VERSION = 1
ALBUM_SCHEMAS = ("Album", "AlbumItem", "Photo", "Comment")
CATALOG = Path(__file__).resolve().parents[2] / "spec" / "social"
SCHEMA = "social-graph/DatabaseSetup"
RECORD = {
    "name": "DatabaseSetup", "descriptive_name": "Database setup",
    "purpose_statement": "Progress for a database provisioned from a named Social setup.",
    "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
    "fields": ["database_id", "ready"],
    "field_types": {"database_id": "String", "ready": "Boolean"},
    "field_descriptions": {
        "database_id": "Stable ID of the database chosen for this setup.",
        "ready": "Whether Social confirmed the complete schema setup.",
    },
}
LABEL = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
DEFINITION_ID = re.compile(r"[0-9a-f]{64}\Z")
CONTRACT_FIELDS = ("descriptive_name", "purpose_statement", "schema_type",
                   "field_types", "field_descriptions")


class Setups:
    def __init__(self, node, sharing):
        self.node = node
        self.sharing = sharing

    def open(self, args):
        setup = args["setupName"]
        version = args.get("requiredVersion", VERSION)
        if setup != SHARED_ALBUMS or type(version) is not int or version != VERSION:
            raise SocialError("choose a supported Social database setup and version")
        selectors = set(args) & {"databaseId", "localName"}
        if len(selectors) != 1:
            raise SocialError("choose one database ID or one new local name")
        selector = selectors.pop()
        value = args[selector]
        if not isinstance(value, str) or not LABEL.fullmatch(value):
            raise SocialError("enter a valid database ID or local name")
        definitions = {f"social/{name}": json.loads((CATALOG / f"{name}.json").read_text())
                       for name in ALBUM_SCHEMAS}
        if selector == "databaseId":
            database = self.database(value)
            verified = self.verify(value, definitions)
        else:
            database, verified = self.provision(value, definitions)
        return {"databaseId": database["id"], "name": database.get("name"),
                "setupName": setup, "version": version, "definitions": verified}

    def database(self, database_id):
        row = next((row for row in self.sharing.databases() if row["id"] == database_id), None)
        if (row is None or row.get("sharingMode") != "whole_database"
                or row.get("kind") not in ("shareable", "received") or row.get("state") != "ready"):
            raise SocialError("select a ready whole-database share by ID")
        return row

    def provision(self, name, definitions):
        # Save the created ID before installing anything. A matching local name
        # alone is never authority to adopt or change an existing database.
        key = f"{SHARED_ALBUMS}:{VERSION}:{name}"
        record_definition = self.node.declare_private(RECORD)
        saved = self.node.get_record(SCHEMA, key)
        if saved is None:
            if any(row.get("name") == name for row in self.sharing.databases()):
                raise SocialError("that name is already in use; select an existing database by ID")
            database = self.sharing.create(name, whole=True)
            fields = {"database_id": database["id"], "ready": False}
            written = self.node.write_record(SCHEMA, record_definition, key, fields, None)
            saved = {"fields": fields, "revision": written["record_revision"]}
        else:
            database = self.database(saved["fields"]["database_id"])
            if database["kind"] != "shareable":
                raise SocialError("this setup belongs to an owned database")
        database_id = database["id"]
        if saved["fields"]["ready"]:
            return database, self.verify(database_id, definitions)
        installed = self.verify(database_id, definitions, allow_missing=True)
        missing = [request for schema, request in definitions.items() if schema not in installed]
        if missing:
            with ThreadPoolExecutor(max_workers=len(missing)) as pool:
                list(pool.map(lambda request: self.install(database_id, request), missing))
        verified = self.verify(database_id, definitions)
        fields = {"database_id": database_id, "ready": True}
        try:
            self.node.write_record(SCHEMA, record_definition, key, fields, saved)
        except LastDBError as error:
            if error.status != 409 or (self.node.get_record(SCHEMA, key) or {}).get("fields") != fields:
                raise
        return database, verified

    def install(self, database_id, request):
        try:
            self.node.call("POST", f"/api/databases/{database_id}/schemas/declare",
                           {**request, "install_if_missing": True})
        except LastDBError as error:
            if error.status != 409:
                raise
            # A concurrent install can win. The full catalog check verifies
            # what actually exists before declaring this setup ready.

    def verify(self, database_id, definitions, *, allow_missing=False):
        path = f"/api/databases/{database_id}/schemas?include_full=1&include_definition_ids=1"
        rows = {row["name"]: row for row in self.node.call("GET", path)["schemas"]}
        verified = {}
        for name, request in definitions.items():
            row = rows.get(name)
            if row is None and allow_missing:
                continue
            if not matches(row, request):
                raise SocialError(f"{name} does not match the shared-albums version 1 setup")
            verified[name] = row["definition_id"]
        return verified


def matches(row, request):
    if not row or row.get("state") != "Available" or row.get("owner_app_id") != request["namespace"]:
        return False
    definition = row.get("definition_id")
    expected = request["schema"]
    keys = row.get("record_keys") or {}
    return (isinstance(definition, str) and bool(DEFINITION_ID.fullmatch(definition))
            and row.get("file_required", False) == expected.get("file_required", False)
            and sorted(row.get("fields") or []) == sorted(expected["fields"])
            and keys.get("primary") == expected["record_keys"]["primary"]
            and keys.get("alternates", {}) == expected["record_keys"]["alternates"]
            and all(row.get(field) == expected[field] for field in CONTRACT_FIELDS))
