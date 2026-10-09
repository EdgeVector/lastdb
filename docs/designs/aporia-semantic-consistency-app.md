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

- Apps live under `fold/apps/*` with a `folddb.toml`, schemas, command surface,
  and local registry entry. The `ingestion` app is the closest zero-UI scaffold.
- Runtime apps use `@folddb/app-sdk`, which exposes `connect`, `query`,
  `queryAll`, `mutate`, and scoped `search`.
- App search is node-authoritative: `POST /api/app/search` ranks only over the
  app's granted scope, and an optional `target` can only narrow the scope.
- Mutation responses already report whether background convergence drained:
  `background_tasks_drained` and `convergence_pending`.
- The schema service already has semantic reuse machinery via dual-signal
  canonicalization: structural similarity plus purpose-statement similarity.

Those pieces are enough for a conservative first version that polls or is
invoked after known writes. They are not enough for a robust cross-app,
post-batch product because Aporia needs an app-readable change feed and richer
search/batch APIs.

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
3. Aporia receives or discovers the changed rows.
4. Aporia classifies each row as incidental or meaningful.
5. Meaningful rows are converted into smaller semantic assertions.
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

A normalized claim extracted from an authored row.

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
- App-owned canonical rows beat derived rollups.

Authority should be represented as data so rules can evolve without prompt-only
behavior. The classifier may infer an `authority_hint`, but the resolver should
apply deterministic policy where possible.

## App Architecture

Aporia starts as a zero-UI app under `fold/apps/aporia`:

```text
fold/apps/aporia/
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
  test/*.test.mjs
```

Commands:

- `list`: list local app registry entries.
- `inspect`: print manifest, schemas, and command contract.
- `status`: check node target, capability/dev trust, schema availability, and
  unresolved event count.
- `watch`: automatic worker loop; poll the scoped change feed, wait for index
  readiness, process changed rows, and persist the cursor.
- `check-record --schema --key`: debugging escape hatch for one existing row.
- `check-mutation --mutation-id`: debugging escape hatch for records written by
  one mutation id.
- `check-batch --batch-id`: debugging escape hatch for a batch.
- `inbox`: list open consistency events.
- `resolve --event --decision`: mark accepted, dismissed, resolved, or snoozed.

The first implementation must be automatic. `check-record`, `check-mutation`,
and `check-batch` are test/debug commands only; they are not the product path.
The MVP depends on a minimal scoped change feed so Aporia can discover writes
without humans or caller apps passing schema/key handoffs.

## SDK Integration

Existing SDK support:

- `connect` handles HTTP or UDS transport and capability/dev-trust posture.
- `queryAll` drains paginated reads and returns full row envelopes.
- `mutate` writes app-owned rows.
- `search` performs scoped native-index recall and returns hydrated row
  envelopes with `schemaName`, `schemaDisplayName`, and `score`.

Needed SDK additions:

1. `mutateBatch(schemaName, ops)` or a general `batchMutate(ops)` wrapper once
   `/api/mutations/batch` exists for app callers. Aporia itself can write
   multiple event/assertion rows without N sequential round trips.
2. `changes({ since, schemas?, limit? })` for an app-readable change feed.
   This should return records the app is allowed to read, plus mutation id,
   schema, key, operation, timestamp, author, and convergence/index status.
3. `getMutation(mutationId)` or `getMutationRecords(mutationId)` if mutation
   id addressing is preferred over a cursor feed.
4. Search options for `minScore`, multiple `target` schemas, and possibly
   lexical/native keyword mode. The current `target` accepts one schema and is
   intentionally narrowing-only; that security property should remain.
5. Surface convergence fields in `MutationResult`. The node already returns
   these concepts, but the SDK currently normalizes mutation results to
   `written`, `mutationIds`, and `firingsObserved`.

## Node Changes

### 0. Transform-Assisted Automatic Capture

Aporia can use node transforms for the automatic capture layer, but not for the
full AI review loop.

Current transforms are deterministic WASM view functions. The node assembles an
input envelope, runs the transform, and writes derived mutations back through
the derived-mutation path. That is a good fit for:

- detecting that a source row changed;
- projecting changed rows into a compact `aporia/ChangeCandidate` or
  `aporia/AssertionCandidate` schema;
- normalizing source metadata such as schema, key, source app, updated time,
  and selected text fields;
- creating a durable queue item that `aporia watch` consumes.

It is not a good fit for:

- calling an LLM;
- doing network retrieval;
- running long semantic searches;
- applying authority policy that may change independently of the transform;
- writing repair actions outside Aporia-owned derived schemas.

The recommended architecture is therefore hybrid:

1. Node transform or trigger creates an Aporia-owned candidate row
   automatically after relevant source writes.
2. Aporia's app worker consumes candidates, waits for index convergence, runs
   AI classification/retrieval, and writes review events.
3. Human-approved repairs remain ordinary app/CLI mutations, not transform
   side effects.

If the transform system cannot subscribe across arbitrary granted app schemas,
the scoped change feed below is still required. If transforms gain a safe
cross-schema source declaration with node-enforced read grants, they can become
the first implementation of the feed.

### 1. App-Readable Change Feed

Aporia needs to know what changed without owner-wide scans. Add a scoped route:

```text
GET or POST /api/app/changes
```

Request:

```json
{
  "since_cursor": "opaque",
  "limit": 100,
  "target": "optional/schema"
}
```

Response:

```json
{
  "changes": [
    {
      "cursor": "opaque",
      "mutation_id": "uuid",
      "schema_name": "fbrain/Concept",
      "key": { "hash": null, "range": "semantic-consistency-app" },
      "operation": "Create",
      "author_pub_key": "...",
      "committed_at": "2026-07-10T00:00:00Z",
      "background_tasks_drained": true,
      "convergence_pending": false
    }
  ],
  "next_cursor": "opaque",
  "has_more": false
}
```

Security:

- The node derives the app's readable scope exactly like scoped search.
- A `target` can only narrow, never widen.
- Forbidden schemas do not reveal existence.
- The route returns metadata for changed rows, not whole owner history outside
  the grant.

Implementation likely uses the existing mutation-event storage in core. The
current code has `get_mutation_events` by molecule UUID; Aporia needs either a
global ordered feed or a mutation-id/batch-id index that maps back to row keys.

### 2. Batch Mutation Route for App Callers

The dev surface has `/dev/mutations` and core has batch execution seams, but the
runtime SDK currently exposes one-row `POST /api/mutation`. Aporia will create
multiple assertions and events per checked write, so an app-facing batch route
should exist:

```text
POST /api/mutations/batch
```

It must enforce the same app write-confinement and read-only LINK behavior as
`/api/mutation`.

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
neighbors are in the vector index.

### 4. Assertion-Friendly Search

Current scoped search is row-oriented and vector/native-index backed. Aporia can
start with that. Better results require:

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

- Reads `fbrain/*` rows when granted.
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

MVP must be automatic from day one. It therefore includes the smallest platform
change that makes automatic discovery real: a scoped app-readable change feed.

1. Add `fold/apps/aporia` scaffold with schemas and commands.
2. Add minimal scoped `/api/app/changes` support and SDK `changes()`.
3. Add `aporia watch`, which stores a durable cursor and polls changed rows
   across the schemas Aporia is granted to read.
4. For each changed row, wait for index readiness or honor
   `convergence_pending` by retrying later.
5. Query the row via SDK, classify it, extract assertions, and write
   `aporia/Assertion`.
6. Use current SDK `search` over granted sources to retrieve candidates.
7. Classify relationships and write `aporia/ConsistencyEvent`.
8. Provide `inbox` and `resolve`.
9. Require human review for every non-Aporia mutation.

MVP limitations:

- `watch` can start as polling; push/subscription delivery is not required.
- `check-record` remains available only for debugging and backfills.
- Search is row-level, not assertion-index-level.
- Hybrid retrieval may need local lexical fallback per granted schema.
- The first change feed can expose row-level changes only; richer batch lineage
  and mutation provenance can follow.

## Platform Follow-Ups

1. Add app-facing `/api/mutations/batch` and SDK `batchMutate`.
2. Expose explicit index epoch/readiness beyond the initial convergence flag.
3. Extend scoped search with hybrid options while preserving node-authoritative
   scoping.
4. Add schema-service conflict-evidence endpoint for proposed schemas.
5. Add app registry metadata for background/routine apps so Aporia can be
   scheduled after mutation batches.
6. Add grants UX for multi-app read access.
7. Add push/subscription delivery for changes if polling proves too expensive.

## Testing

Unit tests:

- assertion extraction JSON schema validation;
- relationship classifier output validation;
- authority-rule resolution;
- event fingerprinting/deduplication;
- safe-action gate.

SDK/app tests:

- app scaffold contract like `apps/ingestion/test`;
- mocked SDK query/search/mutate paths;
- no writes outside `aporia/*` without explicit approval.

Node tests:

- change feed returns only granted schemas;
- change feed `target` narrows only;
- empty scope returns no changes, not owner firehose;
- batch mutation enforces same write confinement as single mutation;
- index readiness waits for native-index tasks or reports pending.

End-to-end tests:

- ephemeral dev node;
- register Aporia, Brain-like, and Kanban-like schemas;
- write a record with a contradictory claim;
- run Aporia;
- assert one open consistency event with evidence and no unauthorized source
  mutation.

## Open Questions

- Should Aporia events be an app-only inbox, or should accepted events also
  create F-Kanban review cards?
- Should assertions be indexed as first-class rows so future Aporia checks
  compare assertion-to-assertion instead of row-to-row?
- What is the first owner-approved grant bundle for Tom's daily node?
- Should dismissed events expire automatically when either source changes?
- Which model/provider should run the lightweight classifier locally versus
  remotely?
