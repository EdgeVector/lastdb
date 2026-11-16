"""Keyed list of sharing requests awaiting an owner decision."""

import json
from urllib.parse import urlencode

from lastdb import LastDBError, SocialError, record_mutation


INDEX = "social-graph/PendingSharingRequest"
REQUESTS = "social-graph/SharingRequest"
PROGRESS = "social-graph/SharingRequestIndexProgress"
HASH = "awaiting"
# Revisit the keys once after 0.7.0 so proposals saved by an older writer
# after its first import are included. Older windows keep their own cursor.
PROGRESS_KEY = "inbox-v2"


def index_key(request_id):
    return {"hash": HASH, "range": request_id}


class RequestIndex:
    def __init__(self, node, reusable):
        self.node, self.reusable = node, reusable

    def add(self, request, definition=None):
        definition = definition or self.node.declare_private("PendingSharingRequest")
        fields = {"request_id": request["requestId"], "app_id": request["appId"],
                  "database_id": request["change"]["databaseId"]}
        return record_mutation(INDEX, definition, index_key(request["requestId"]), fields, None)

    def get(self, request_id):
        query = {"schema_name": INDEX, "filter": {"HashRangeKey": index_key(request_id)}}
        rows = self.node.call("POST", "/api/query", query)["results"]
        return rows[0] if rows else None

    def remove(self, request_id):
        definition = self.node.declare_private("PendingSharingRequest")
        row = self.get(request_id)
        return record_mutation(INDEX, definition, index_key(request_id), None, row) if row else None

    def ensure(self, request):
        try:
            self.node.call("POST", "/api/mutation", self.add(request))
        except LastDBError as error:
            if error.status != 409:
                raise

    def awaiting(self, cursor, *, recheck=False):
        definition = self.node.declare_private("PendingSharingRequest")
        importing = self.import_page(definition, recheck=recheck)
        query = {"schema_name": INDEX, "filter": {"HashKey": HASH}, "limit": 64}
        if cursor is not None:
            query["cursor"] = json.loads(cursor)
        page = self.node.call("POST", "/api/query", query)
        rows = {row["fields"]["request_id"]: row for row in page["results"]}
        pending, decided = self.read_pending(rows)
        self.prune(definition, rows, decided)
        return {
            "requests": [{"requestId": request["requestId"], "appId": request["appId"],
                          "databaseId": request["change"]["databaseId"]} for request in pending],
            "nextCursor": page.get("next_cursor"), "importing": importing,
        }

    def prune(self, definition, rows, decided):
        if decided:
            try:
                self.node.call("POST", "/api/mutations/batch", {"mutations": [
                    record_mutation(INDEX, definition, index_key(key), None, rows[key]) for key in decided
                ]})
            except LastDBError as error:
                if error.status != 409:
                    raise
                # Another owner window may have cleaned up these entries first.

    def import_page(self, definition, *, recheck):
        progress_definition = self.node.declare_private("SharingRequestIndexProgress")
        progress = self.node.get_record(PROGRESS, PROGRESS_KEY)
        if not recheck and progress and progress["fields"]["complete"]:
            return False
        with self.reusable.circles.lock():
            progress = self.node.get_record(PROGRESS, PROGRESS_KEY)
            if progress and progress["fields"]["complete"] and not recheck:
                return False
            cursor = progress["fields"]["cursor"] if progress else ""
            pending, next_cursor = self.legacy_page(cursor)
            # Store the entries and their cursor together so an interrupted
            # upgrade can safely resume at the last acknowledged page.
            updates = [self.add(request, definition) for request in pending]
            fields = {"cursor": next_cursor or "", "complete": not next_cursor}
            updates.append(record_mutation(PROGRESS, progress_definition, {"hash": PROGRESS_KEY, "range": None},
                                           fields, progress))
            self.node.call("POST", "/api/mutations/batch", {"mutations": updates})
            return bool(next_cursor)

    def legacy_page(self, cursor):
        # The old app kept no index. Walk its existing keys once, in pages.
        self.node.declare_private("SharingRequest")
        params = {"schema": REQUESTS, "limit": 32}
        if cursor:
            params["cursor"] = cursor
        page = self.node.call("GET", "/api/list?" + urlencode(params))["list"]
        next_cursor = page.get("next_cursor") if page["has_more"] else None
        if page["has_more"] and (not next_cursor or next_cursor == cursor):
            raise SocialError("LastDB did not advance the sharing request import")
        request_ids = [key["hash"] for key in page["keys"]]
        if not request_ids:
            return [], next_cursor
        existing = self.read_batch(INDEX, [{"HashRangeKey": index_key(request_id)} for request_id in request_ids])
        pending, _ = self.read_pending([request_id for request_id, rows in zip(request_ids, existing) if not rows])
        return pending, next_cursor

    def read_pending(self, request_ids):
        if not request_ids:
            return [], []
        # Read operations first. A newer operation saves the previous decision
        # to its request before replacing the directory entry.
        operations = self.reusable.load()[2]["operations"]
        records = self.read_batch(REQUESTS, [{"HashKey": request_id} for request_id in request_ids])
        pending, decided = [], []
        for rows in records:
            if not rows:
                continue
            saved = json.loads(rows[0]["fields"]["request"])
            request = saved["declaration"]
            operation = operations.get(request["change"]["databaseId"])
            if "receipt" in saved or (operation and operation["requestId"] == request["requestId"]):
                decided.append(request["requestId"])
            else:
                pending.append(request)
        return pending, decided

    def read_batch(self, schema, filters):
        batch = self.node.call("POST", "/api/queries/batch", {
            "queries": [{"schema_name": schema, "filter": key} for key in filters],
        })["results"]
        if len(batch) != len(filters) or any(item["status"] != 200 for item in batch):
            raise SocialError("LastDB could not read the sharing request page")
        return [item["response"]["results"] for item in batch]
