"""One implementation of POST /v1/read and POST /v1/write for CI scripts.

A keyed read is one read item. Up to 64 items share one request. A key listing
is a keys_only read, one page at a time. A write is create, replace, or delete.
Create does not read first. Replace and delete read the revision when the
caller did not already name one, then write. Delete does not send alternates.
"""

READ_LIMIT = 64


def reply_body(reply):
    """Benchmarks return (body, elapsed, receipt). Other clients return the body."""
    return reply[0] if isinstance(reply, tuple) else reply


def _item_error(path, index, item):
    error = item.get("error") if isinstance(item, dict) else None
    if error:
        raise RuntimeError(f"{path} item {index}: {error}")


def read_items(node, items, *, strict=True):
    """POST /v1/read. `items` holds 1 to 64 reads.

    A bad whole reply raises. An item error raises when strict is true.
    The app API proof sets strict false and reads the error on that item.
    """
    if not items:
        return []
    if len(items) > READ_LIMIT:
        raise ValueError(f"/v1/read accepts at most {READ_LIMIT} items")
    body = reply_body(node.call("/v1/read", {"reads": list(items)}))
    results = body.get("results")
    if not isinstance(results, list) or len(results) != len(items):
        raise RuntimeError(f"/v1/read returned an unexpected reply: {body!r}")
    if strict:
        for index, item in enumerate(results):
            _item_error("/v1/read", index, item)
    return results


def read_all(node, items):
    """Every read item, in order, in requests of at most 64."""
    found = []
    for start in range(0, len(items), READ_LIMIT):
        found.extend(read_items(node, items[start:start + READ_LIMIT]))
    return found


def flat_records(results):
    rows = []
    for page in results:
        rows.extend(page["records"])
    return rows


def write_items(node, mutations):
    """POST /v1/write. Return the reply. Do not send an empty list."""
    if not mutations:
        return {"written": 0, "revisions": []}
    body = reply_body(node.call("/v1/write", {"mutations": list(mutations)}))
    revisions = body.get("revisions") if isinstance(body, dict) else None
    if (not isinstance(body, dict) or body.get("written") != len(mutations)
            or not isinstance(revisions, list) or len(revisions) != len(mutations)
            or any(not isinstance(value, str) or not value for value in revisions)):
        raise RuntimeError("a write did not report one revision for each mutation: " + repr(body))
    return body


def key_input(key):
    """A request key. A missing or empty range stays omitted, so a hash key stays a hash key."""
    if not isinstance(key, dict):
        return {"hash": key}
    item = {}
    if "hash" in key:
        item["hash"] = key["hash"]
    if key.get("range"):
        item["range"] = key["range"]
    return item


def _with(item, *, alternates=None, database=None, file=None):
    if alternates:
        item["alternates"] = alternates
    if database is not None:
        item["database"] = database
    if file is not None:
        item["file"] = file
    return item


def create_item(schema, key, record, *, alternates=None, database=None, file=None):
    return _with(
        {"schema": schema, "op": "create", "key": key_input(key), "record": record},
        alternates=alternates, database=database, file=file,
    )


def replace_item(schema, key, record, revision, *, alternates=None, database=None, file=None):
    return _with(
        {"schema": schema, "op": "replace", "key": key_input(key), "record": record, "revision": revision},
        alternates=alternates, database=database, file=file,
    )


def delete_item(schema, key, revision, *, database=None):
    return _with(
        {"schema": schema, "op": "delete", "key": key_input(key), "revision": revision},
        database=database,
    )


def _options(item, options):
    for name in ("via", "limit", "cursor", "keys_only", "file", "database"):
        if options.get(name) is not None:
            item[name] = options[name]
    return item


def point(schema, key, **options):
    return _options({"schema": schema, "key": key_input(key)}, options)


def range_read(schema, span, **options):
    return _options({"schema": schema, "range": span}, options)


def key_list(schema, *, limit=None, cursor=None, database=None):
    return _options({"schema": schema, "keys_only": True}, {"limit": limit, "cursor": cursor, "database": database})


def commit(node, mutations):
    """Fill a missing revision from /v1/read, then POST /v1/write.

    Create skips the read. An item that already names its revision skips the
    read, so a replay keeps the revision the caller saved. Delete drops record,
    file, and alternates: the node loads the stored alternates.
    """
    if not mutations:
        return []
    pending = [dict(item) for item in mutations]
    needed = [index for index, item in enumerate(pending) if item.get("op") != "create" and "revision" not in item]
    if needed:
        reads = []
        for index in needed:
            item = pending[index]
            read = {"schema": item["schema"], "key": item["key"]}
            if "database" in item:
                read["database"] = item["database"]
            reads.append(read)
        for index, page in zip(needed, read_all(node, reads)):
            rows = page["records"]
            if len(rows) != 1:
                raise RuntimeError("a replace or delete names a key that is not one record")
            pending[index]["revision"] = rows[0]["revision"]
            if pending[index].get("op") == "delete":
                pending[index].pop("record", None)
                pending[index].pop("alternates", None)
                pending[index].pop("file", None)
    return write_items(node, pending)["revisions"]


def benchmark_batch(decoded):
    mutations = decoded.get("mutations") if isinstance(decoded, dict) else None
    return bool(mutations) and isinstance(mutations[0], dict) and "mutation_type" in mutations[0]


def _benchmark_key(mutation):
    return key_input(mutation["key_value"])


def benchmark_current(send, mutations):
    """Revision reads for updates and deletes. Creates are not read. Chunks of 64."""
    targets = [item for item in mutations if item.get("mutation_type") != "create"]
    if not targets:
        return {}
    schema = targets[0]["schema"]
    if any(item["schema"] != schema for item in targets):
        raise ValueError("benchmark batch must target a single schema")
    found = {}
    for start in range(0, len(targets), READ_LIMIT):
        chunk = targets[start:start + READ_LIMIT]
        reads = [{"schema": schema, "key": _benchmark_key(item)} for item in chunk]
        # /v1/read holds 64 items, so a batch of 128 is two revision reads.
        reply = reply_body(send("/v1/read", {"reads": reads}, receipt=True))
        results = reply.get("results")
        if not isinstance(results, list) or len(results) != len(chunk):
            raise RuntimeError(f"/v1/read returned an unexpected reply: {reply!r}")
        for index, page in enumerate(results):
            _item_error("/v1/read", index, page)
            for row in page["records"]:
                found[(row["key"].get("hash"), row["key"].get("range"))] = row
    return found


def benchmark_items(mutations, current):
    converted = []
    for mutation in mutations:
        key = _benchmark_key(mutation)
        prior = current.get((key.get("hash"), key.get("range")))
        kind = mutation["mutation_type"]
        if kind == "purge":
            raise RuntimeError("benchmark purge has no /v1 write")
        if kind == "delete":
            if not prior:
                raise RuntimeError("benchmark delete target is absent")
            converted.append(delete_item(mutation["schema"], key, prior["revision"]))
            continue
        fields = dict(prior["record"]) if prior else {}
        fields.update(mutation["fields_and_values"])
        if prior:
            converted.append(replace_item(mutation["schema"], key, fields, prior["revision"]))
        else:
            converted.append(create_item(mutation["schema"], key, fields))
    return converted


def benchmark_write(send, mutations, on_conflict):
    """Turn field patches into one /v1/write. The elapsed time of `send` includes the revision reads."""
    for attempt in range(9):
        converted = benchmark_items(mutations, benchmark_current(send, mutations))
        try:
            reply = send("/v1/write", {"mutations": converted})
        except RuntimeError as error:
            if getattr(error, "status", None) != 409 or attempt == 8:
                raise
            on_conflict()
            continue
        body = reply_body(reply)
        revisions = body.get("revisions") if isinstance(body, dict) else None
        if (not isinstance(body, dict) or body.get("written") != len(converted)
                or not isinstance(revisions, list) or len(revisions) != len(converted)
                or any(not isinstance(value, str) or not value for value in revisions)):
            raise RuntimeError("benchmark response has incomplete revisions")
        return reply
    raise RuntimeError("benchmark write did not commit")
