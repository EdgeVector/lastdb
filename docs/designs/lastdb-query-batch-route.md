# `POST /api/queries/batch` — several independent reads in one call

Status: shipped with this change. Owner: Tom. Written 2026-10-07.

## What it is

One request that carries several queries. Each query is a complete
`POST /api/query` body, so each names its own `schema_name`. One request can
therefore read from several schemas. It is the read twin of
`POST /api/mutations/batch`, which already spans schemas.

It is **not** a join. The items are independent: no item reads another item's
result, and LastDB does not match records across schemas. To follow a key from
one result into another query, the app reads first and then sends a second
batch (one batch per wave).

## Wire

Request (owner and app sockets, like `/api/query`):

```json
{ "queries": [
  { "schema_name": "Card",      "fields": ["title"], "filter": { "HashKey": "card-a" } },
  { "schema_name": "Milestone", "fields": ["state"], "filter": { "HashKey": "ms-1" } },
  { "schema_name": "Papercut",  "fields": ["title"],
    "filter": { "HashRangeKeys": [["p", "1"], ["p", "2"]] } }
] }
```

Reply (`200`):

```json
{ "ok": true, "count": 3, "user_hash": "...",
  "results": [
    { "status": 200, "response": { "ok": true, "results": [ ... ], "...": "..." } },
    { "status": 400, "response": "Invalid data: Schema 'Nope' not found" },
    { "status": 200, "response": { "...": "..." } }
] }
```

- `results` has one entry per query, **in request order**.
- `response` is exactly the body `POST /api/query` returns for that query.
  `status` is exactly its HTTP status. An error body is whatever the single route
  sends: a string (`"Invalid data: Schema 'Nope' not found"`) or an object
  (`{"ok":false,"kind":"malformed_body","error":"request body must be a JSON object"}`). Pagination keys (`limit`, `offset`,
  `cursor`, `expected_total_count`) go inside the item, as on the single route.
- One item can fail while the others succeed. The request itself returns `400`
  only for a malformed body, a missing or unknown top-level key, an empty batch,
  or more than **64** queries.

## How it runs

- Each item runs through the same code as `/api/query` (as a sub-request), so
  parse, schema resolve, access checks, limits, and error bodies are identical
  by construction. A test compares each item to the single route.
- Items run **4 at a time**, inside the request task. Each item takes its own QoS
  permit, as a single query does; the batch holds none. The width keeps one
  request from taking the global budget (64 permits by default) while the cold
  loads of its items overlap. Items may finish in any order (a slow item does not
  hold back the next one); each carries its request index and the reply is sorted
  by it, so the order never depends on completion order.
- The `x-lastdb-allow-full-scan` header is **not** passed to items. An unkeyed
  item is refused, as on a product route.
- Telemetry: the request is one sample of kind `query_batch`, labelled with the
  schema of the first query. Request phases add up over all items.

## What it buys (measured)

Measured on 2026-10-07 and 2026-10-08 on synthetic nodes with the file layout of
the primary (brain `design-lastdb-batch-reads-and-composable-queries-20261007`).
Loads are the node's own counters, with the cost of the status sampler taken out.
Each row is the same in all three repeats.

**The route does not reduce loads.** Each item runs as its own query and pays its
own loads. One batch uses the same loads as the same items sent as separate calls:

| Items | Separate calls | One batch call |
|---|---|---|
| 3 items, 3 schemas, 1 key each | 37 cold, 40 loader | 37 cold, 40 loader |
| 30 items, 3 schemas | 344 cold, 382 loader | 344 cold, 382 loader |
| 20 keys in one hash of one schema | 289 cold, 304 loader | 289 cold, 304 loader |

Each item pays the loads of its own query. An earlier version of this page said
that cost is paid once for each request. That was wrong: it is paid once for each
query. Most of it is the first read of that key's data, which the node keeps after
that: a node that has just restarted pays 9 loader loads for one key of a Hash schema
and 13 to 17 for a HashRange schema, and the same query a second time pays 2 and 5.
Those 2 to 5 are a floor that every query pays and the node never keeps (see below).

**A multi-get inside one query does share loads.** The 20 keys above as one
`HashRangeKeys` query cost 159 cold and 20 loader loads: 1.8 and 15 times fewer
than 20 queries or 20 batch items. For many keys of one schema, send one query with
`HashRangeKeys`, inside a batch or not.

**Time.** On a debug build the batch took about as long as separate calls (3 items:
31 and 30 ms; 30 items: 318 and 433 ms; 20 keys in one hash: 247 and 215 ms).
Eight clients that sent the 30 separate calls at the same time took 80 ms. The
"4 at a time" above is concurrency of futures inside one task. In this measurement
it did not give parallel speed. The host was busy and the data small, so treat the
times as a lead and not as a result.

**So today the route saves round trips, not loads.** The cost of a round trip was
not measured here.

### Open: what would make the route cut loads

- Merge items of one batch that share a schema and a key shape into one
  `HashRangeKeys` query, and split the results back per item.
- Run items on separate tasks for real parallelism (the task-local request phases
  need care).
- Stop re-reading what the node already knows. Every query pays a floor of 2 (Hash)
  to 5 (HashRange) loader loads that the node does not keep. Four of the five read
  records that do not exist: `hcu:mols` (the conflict index) in two collections,
  `mgp:v1:` (a generation pointer), and `rdel:v2:` (a delete barrier). The fifth
  reads the molecule header, which exists but is sealed at rest, and the resident set
  never admits a sealed body. Measured 2026-10-08 with a diagnostic build that logs
  each loader call (brain `design-lastdb-batch-reads-and-composable-queries-20261007`).
  This needs no API change and helps every call.

## Not in this change

- No composition: an item cannot take a key from another item.
- No change to how a single query loads its hash groups.
- No sharing of loads between items (see "What it buys").
- No client moved to the route yet (brain, kanban, loom, situations).
