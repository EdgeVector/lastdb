"""Keyed list of sharing requests awaiting an owner decision."""

import json

from lastdb import LastDBError, SocialError, record_mutation


INDEX = "social-graph/PendingSharingRequest"
REQUESTS = "social-graph/SharingRequest"
HASH = "awaiting"


def index_key(request_id):
    return {"hash": HASH, "range": request_id}


class RequestIndex:
    def __init__(self, node, reusable):
        self.node, self.reusable = node, reusable

    def add(self, request):
        definition = self.node.declare_private("PendingSharingRequest")
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

    def awaiting(self, cursor):
        definition = self.node.declare_private("PendingSharingRequest")
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
            "nextCursor": page.get("next_cursor"),
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
                # Another Social command may have cleaned up these entries first.

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
