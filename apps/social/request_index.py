"""Keyed list of sharing requests awaiting an owner decision."""

import json
from urllib.parse import urlencode

from lastdb import SocialError, record_mutation


INDEX = "social-graph/PendingSharingRequest"
REQUESTS = "social-graph/SharingRequest"
PROGRESS = "social-graph/SharingRequestIndexProgress"
HASH = "awaiting"
PROGRESS_KEY = "inbox"


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

    def remove(self, request_id):
        definition = self.node.declare_private("PendingSharingRequest")
        query = {"schema_name": INDEX, "filter": {"HashRangeKey": index_key(request_id)}}
        rows = self.node.call("POST", "/api/query", query)["results"]
        if rows:
            return record_mutation(INDEX, definition, index_key(request_id), None, rows[0])
        return None

    def awaiting(self, cursor):
        definition = self.node.declare_private("PendingSharingRequest")
        importing = self.import_page(definition)
        query = {"schema_name": INDEX, "filter": {"HashKey": HASH}, "limit": 64}
        if cursor is not None:
            query["cursor"] = json.loads(cursor)
        page = self.node.call("POST", "/api/query", query)
        fields = [row["fields"] for row in page["results"]]
        return {
            "requests": [{"requestId": row["request_id"], "appId": row["app_id"],
                          "databaseId": row["database_id"]} for row in fields],
            "nextCursor": page.get("next_cursor"), "importing": importing,
        }

    def import_page(self, definition):
        progress_definition = self.node.declare_private("SharingRequestIndexProgress")
        progress = self.node.get_record(PROGRESS, PROGRESS_KEY)
        if progress and progress["fields"]["complete"]:
            return False
        with self.reusable.circles.lock():
            progress = self.node.get_record(PROGRESS, PROGRESS_KEY)
            if progress and progress["fields"]["complete"]:
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
        records = self.read_batch(REQUESTS, [{"HashKey": request_id} for request_id in request_ids])
        existing = self.read_batch(INDEX, [{"HashRangeKey": index_key(request_id)} for request_id in request_ids])
        operations = self.reusable.load()[2]["operations"]
        pending = []
        for rows, indexed in zip(records, existing):
            if not rows or indexed:
                continue
            saved = json.loads(rows[0]["fields"]["request"])
            request = saved["declaration"]
            operation = operations.get(request["change"]["databaseId"])
            if "receipt" not in saved and (not operation or operation["requestId"] != request["requestId"]):
                pending.append(request)
        return pending, next_cursor

    def read_batch(self, schema, filters):
        batch = self.node.call("POST", "/api/queries/batch", {
            "queries": [{"schema_name": schema, "filter": key} for key in filters],
        })["results"]
        if len(batch) != len(filters) or any(item["status"] != 200 for item in batch):
            raise SocialError("LastDB could not read the sharing request import page")
        return [item["response"]["results"] for item in batch]
