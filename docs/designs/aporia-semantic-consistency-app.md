# Aporia: Semantic Consistency App

Status: draft
Owner: product/platform
Created: 2026-07-10

## Summary

Aporia is a LastDB app that reviews meaningful writes after indexing and
surfaces semantic conflicts, stale records, and downstream implications across
Brain, Kanban, schemas, SOPs, repo docs, and other app-owned knowledge.

The product is a consistency inbox, not an automatic truth engine. It should
detect unresolved tension, explain why it matters, and propose repairs. It may
auto-write non-destructive derived annotations, but it must not silently rewrite
canonical decisions, schemas, SOPs, task bodies, or operational policy.

## Current System Fit

The current app model already supports most of the outer shell:

- Apps live under `apps/*` with a `folddb.toml`, schemas, command surface,
  and local registry entry. The `ingestion` app is the closest zero-UI scaffold.
- Runtime apps use `@lastdb/app-sdk`, which exposes `connect`, `query`,
  `queryAll`, `mutate`, `changes`, and scoped `search`.
- `POST /api/app/changes` is an ordered change feed, scoped to the schemas that
  the app can read. An optional `target` can only narrow the scope.
- Mini has no search index. `POST /api/app/search` keeps its scope checks, but
  it returns `search_plane_required`. LastSeek owns search.
- Mutation responses already report whether background convergence drained:
  `background_tasks_drained` and `convergence_pending`.
- The schema service already has semantic reuse machinery via dual-signal
  canonicalization: structural similarity plus purpose-statement similarity.

Those pieces are enough for a conservative first version that polls the change
feed. A robust cross-app product also needs a scoped search through LastSeek
and SDK batch writes.

## Product Goals

1. After a meaningful write, determine whether new information creates,
   refines, supersedes, contradicts, or makes stale existing knowledge.
2. Surface evidence, confidence, provenance, authority rationale, and suggested
   repairs.
3. Track review decisions so dismissed conflicts do not repeatedly return.
4. Support Brain, Kanban, schemas, and app records through a shared event model.
5. Make safe automatic updates only to derived/non-destructive material:
   backlinks, stale-candidate markers, rollups, generated summaries, and
   Aporia-owned records.

## Non-Goals

- Aporia does not decide truth globally.
- Aporia does not silently rewrite authored records.
- Aporia does not bypass app isolation or use owner-wide search from an app.
- Aporia does not replace schema-service canonicalization.
- Aporia does not require every app to adopt a new SDK immediately.

## Core Flow

1. A write or batch mutation commits.
2. The node indexes the write and reports whether convergence drained.
3. Aporia reads the changed records from the change feed.
4. Aporia classifies each record as incidental or meaningful.
5. Aporia converts each meaningful record into smaller semantic assertions.
6. Each assertion is used to retrieve candidates with hybrid recall:
   vector search, lexical/BM25, structured filters, graph links, and schema
   metadata.
7. A model classifies each candidate relationship:
   `agrees`, `duplicate`, `refines`, `supersedes`, `contradicts`,
   `stale`, or `unrelated`.
8. Aporia applies authority rules and creates reviewable consistency events.
9. A human or agent resolves each event by dismissing, linking, marking stale,
   creating follow-up work, or approving a proposed edit.

## Data Model

Aporia should own its state in `aporia/*` schemas.

### `aporia/Run`

One invocation or automatic check.

Fields:

- `run_id`
- `trigger_kind`: `manual`, `post_mutation`, `post_batch`, `routine`, `backfill`
- `trigger_ref`: mutation id, batch id, cursor, or manual query
- `status`: `queued`, `running`, `succeeded`, `failed`, `cancelled`
- `started_at`
- `finished_at`
- `changed_record_count`
- `assertion_count`
- `event_count`
- `error_id`

### `aporia/Assertion`

A normalized claim extracted from an authored record.

Fields:

- `assertion_id`
- `source_schema`
- `source_key`
- `source_app_id`
- `source_mutation_id`
- `source_updated_at`
- `subject`
- `predicate`
- `object`
- `negative_object`
- `scope`
- `claim_text`
- `authority_hint`
- `confidence`
- `status`: `active`, `superseded`, `dismissed`
- `created_at`

The assertion is the unit searched and compared. Whole documents are too broad:
most contradictions happen at claim level.

### `aporia/ConsistencyEvent`

The inbox item.

Fields:

- `event_id`
- `run_id`
- `new_assertion_id`
- `candidate_assertion_id`
- `candidate_source_schema`
- `candidate_source_key`
- `relationship`
- `severity`: `info`, `review`, `important`, `blocking`
- `confidence`
- `authority_rationale`
- `evidence`
- `impact`
- `suggested_actions`
- `status`: `open`, `accepted`, `dismissed`, `resolved`, `snoozed`
- `resolution`
- `created_at`
- `updated_at`

### `aporia/ResolutionMemory`

Deduplication and alert-fatigue control.

Fields:

- `memory_id`
- `fingerprint`
- `event_id`
- `decision`: `dismissed`, `accepted`, `resolved`, `snoozed`
- `reason`
- `expires_at`
- `created_at`

## Authority Rules

Aporia needs explicit policy, not newest-wins. Initial rules:

- Human-confirmed decision beats AI-generated summary.
- Schema registry beats generated docs about schema shape.
- Repo-local `AGENTS.md` beats workspace policy inside that repo unless marked
  global.
- Workspace `AGENTS.md` beats old task notes.
- Live Kanban state beats stale completed cards for current work status.
- Incident notes may temporarily override SOPs until the incident is resolved.
- App-owned canonical records beat derived rollups.

Authority should be represented as data so rules can evolve without prompt-only
behavior. The classifier may infer an `authority_hint`, but the resolver should
apply deterministic policy where possible.

## App Architecture

Aporia starts as a zero-UI app under `apps/aporia`:

```text
apps/aporia/
  folddb.toml
  package.json
  bin/aporia-app.mjs
  src/manifest.mjs
  src/run.mjs
  src/assertions.mjs
  src/retrieve.mjs
  src/classify.mjs
  src/events.mjs
  schemas/*.schema.json
```

Commands:

- `list`: list local app registry entries.
- `inspect`: print manifest, schemas, and command contract.
- `status`: check node target, capability/dev trust, schema availability, and
  unresolved event count.
- `watch`: automatic worker loop; poll the scoped change feed, wait for index
  readiness, process changed records, and persist the cursor.
- `check-record --schema --key`: debugging escape hatch for one existing record.
- `check-mutation --mutation-id`: debugging escape hatch for records written by
  one mutation id.
- `check-batch --batch-id`: debugging escape hatch for a batch.
- `inbox`: list open consistency events.
- `resolve --event --decision`: mark accepted, dismissed, resolved, or snoozed.

The first implementation must be automatic. `check-record`, `check-mutation`,
and `check-batch` are debug commands only; they are not the product path.
The scoped change feed lets Aporia discover writes without humans or caller
apps passing schema/key handoffs.

## SDK Integration

Existing SDK support:

- `connect` handles HTTP or UDS transport and capability/dev-trust posture.
- `queryAll` drains paginated reads and returns full record envelopes.
- `mutate` writes app-owned records.
- `changes({ since, limit, target })` reads one page of the scoped change
  feed.
- `search` sends the scoped search request. Mini answers
  `search_plane_required`, so retrieval must come from LastSeek.

Needed SDK additions:

1. A `batchMutate(ops)` wrapper for `POST /api/mutations/batch`. The node
   serves that route on both sockets; the SDK does not call it yet. Aporia
   can then write several event and assertion records in one request.
2. `getMutation(mutationId)` or `getMutationRecords(mutationId)` if mutation
   id addressing is preferred over a cursor feed.
3. A scoped search client for LastSeek with `minScore` and multiple `target`
   schemas. A `target` must only narrow the scope; that security property
   must remain.
4. Surface convergence fields in `MutationResult`. The node already returns
   these concepts; the SDK result carries `mutationId` plus the optional
   `backgroundTasksDrained` and `convergencePending` flags.

## Node Changes

### 1. App-Readable Change Feed

The node runs no transforms, so the change feed is the capture path.
`POST /api/app/changes` returns ordered mutation metadata for the schemas that
the caller can read.

Request:

```json
{
  "since_cursor": "v1-0000000000000000",
  "limit": 100,
  "target": "optional/schema"
}
```

The `limit` counts the feed entries that the node examines. The default is 100,
and the node clamps it to 1 through 500. A page can hold fewer visible changes.

Response:

```json
{
  "changes": [
    {
      "cursor": "v1-000000000000002a",
      "mutation_id": "uuid",
      "schema_name": "fbrain/Concept",
      "key": { "hash": null, "range": "semantic-consistency-app" },
      "operation": "Create",
      "committed_at_ms": 1783641600000,
      "background_tasks_drained": true,
      "convergence_pending": false
    }
  ],
  "next_cursor": "v1-000000000000002a",
  "has_more": false,
  "gap": false
}
```

The reply has no writer identity and no field values. Aporia reads each changed
record by its key after it gets the page.

Security:

- The node derives the readable scope of the app. The owner socket uses the owner scope.
- A `target` can only narrow, never widen.
- Forbidden schemas do not reveal existence.
- The route returns metadata for changed records, not whole owner history
  outside the grant.

### 2. Batch Mutation Route for App Callers

`POST /api/mutations/batch` writes several mutations in one request, on both
sockets. It must keep the same app write-confinement and read-only LINK behavior
as `/api/mutation`. The SDK wrapper is the remaining work (SDK item 1).

### 3. Index Epoch / Readiness

Mutation responses already distinguish drained versus pending background work.
Aporia needs a way to wait for a specific write to be searchable:

```text
GET /api/app/index-status?mutation_id=...
```

or include an `index_epoch` in mutation/change responses and expose:

```text
POST /api/app/wait-index { "epoch": "..." }
```

This prevents false negatives where Aporia searches before the new claim or its
neighbors are in the LastSeek index.

### 4. Assertion-Friendly Search

Mini has no search index. Aporia retrieves candidates through LastSeek, scoped
to the schemas the app can read. Better results require:

- multiple target schemas, intersected with `S(A)`;
- optional lexical/BM25 mode or hybrid mode;
- result explanations or matched fields/fragments;
- deterministic minimum-score filtering.

The existing search security model should not change: scope by traversal, not
output filtering.

## Schema-Service Integration

Schema-service remains the canonical schema registry. Aporia should integrate
in two ways:

1. Register `aporia/*` schemas with strong `purpose_statement` values so
   dual-signal canonicalization keeps them distinct from Brain/Kanban schemas.
2. Consume schema similarity as one signal when the changed artifact is a schema
   or schema-like contract.

Required schema-service changes:

- Add an endpoint or client method that returns semantic reuse/conflict evidence
  for proposed schemas, not only add/reuse outcomes. The existing
  `batch-check-reuse` path is close, but Aporia needs evidence suitable for a
  consistency event.
- Preserve immutable-schema semantics. Aporia may propose a new schema,
  migration, compatibility note, or stale marker; it should not update an
  approved schema in place.

## Brain And Kanban Integration

Aporia should not special-case the CLIs internally, but it should understand
their app schemas and conventions.

Brain:

- Reads `fbrain/*` records when granted.
- Treats `decision`, `sop`, `preference`, `concept`, `reference`, and
  `project` records as high-signal sources.
- Uses record type and status as authority hints.
- May write Aporia events that link to Brain slugs.
- May propose Brain edits, but should not apply them without review.

Kanban:

- Reads `fkanban/Card` and `fkanban/Board` when granted.
- Treats card `column`, `status`, dependencies, `block_status`, `repo`, `base`,
  and body headers as impact signals.
- Can create follow-up review cards only when explicitly approved or when the
  policy says Aporia may auto-create low-risk review work.
- Should not silently alter card body, dependencies, or column.

Cross-app access requires ordinary app consent/grants. Aporia should request
explicit read grants for `fbrain/*`, `fkanban/*`, and any other source app the
owner wants it to monitor, plus write grants for `aporia/*`.

## AI Responsibilities

Use AI for bounded classification, not unbounded mutation:

- classify content importance;
- extract assertions;
- classify relationships;
- summarize impact;
- draft suggested repair actions.

Do not let the model directly decide writes to non-Aporia schemas. All such
actions become proposed patches or follow-up cards.

Each model output should be stored with:

- prompt version;
- model/provider;
- input source ids;
- output JSON;
- confidence;
- parse/validation status.

## MVP

MVP must be automatic from day one. The scoped change feed
(`/api/app/changes`, SDK `changes()`) makes automatic discovery real.

1. Add `apps/aporia` scaffold with schemas and commands.
2. Add `aporia watch`, which stores a durable cursor and polls changed records
   across the schemas Aporia is granted to read.
3. For each changed record, wait for index readiness or honor
   `convergence_pending` by retrying later.
4. Query the record via SDK, classify it, extract assertions, and write
   `aporia/Assertion`.
5. Use a LastSeek search scoped to granted sources to retrieve candidates.
6. Classify relationships and write `aporia/ConsistencyEvent`.
7. Provide `inbox` and `resolve`.
8. Require human review for every non-Aporia mutation.

MVP limitations:

- `watch` can start as polling; push/subscription delivery is not required.
- `check-record` remains available only for debugging and backfills.
- Search is record-level, not assertion-level.
- Hybrid retrieval may need local lexical fallback per granted schema.
- The change feed gives record-level changes only. Batch identity and mutation
  provenance can follow.

## Platform Follow-Ups

1. Add SDK `batchMutate` for `/api/mutations/batch`.
2. Expose explicit index epoch/readiness beyond the initial convergence flag.
3. Give the LastSeek scoped search hybrid options and keep scope checks on
   the server side.
4. Add schema-service conflict-evidence endpoint for proposed schemas.
5. Add app registry metadata for background/routine apps so Aporia can be
   scheduled after mutation batches.
6. Add grants UX for multi-app read access.
7. Add push/subscription delivery for changes if polling proves too expensive.

## Proof

Run `aporia watch` against an owned DEV node. Write a record with a
contradictory claim. Proof is one open consistency event with evidence and no
source mutation outside `aporia/*`.

## Open Questions

- Should Aporia events be an app-only inbox, or should accepted events also
  create F-Kanban review cards?
- Should assertions be searchable as first-class records so future Aporia checks
  compare assertion-to-assertion instead of record-to-record?
- What is the first owner-approved grant bundle for Tom's daily node?
- Should dismissed events expire automatically when either source changes?
- Which model/provider should run the lightweight classifier locally versus
  remotely?
