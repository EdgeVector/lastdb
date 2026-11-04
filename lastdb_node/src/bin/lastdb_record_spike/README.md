# Whole-record mutations with explicit keys

Isolated branch: spike/whole-record-mutations-20261104.
This executable does not change the daemon API or LastDB home format.
Use only a newly created spike home; existing directories are refused by init.
All compilation and actual operations run in CI, never on the primary.

The schema declares addresses, with no binding to payload fields:

~~~json
{
  "primary": "hash",
  "alternates": {
    "by_team": "hash",
    "by_order": "hash_range"
  }
}
~~~

Each put supplies the complete record and exactly every declared alternate:

~~~json
{
  "schema": "notes",
  "key": {"hash": "note-17"},
  "expected": {"type": "absent"},
  "alternates": {
    "by_team": {"hash": "engineering"},
    "by_order": {"hash": "open", "range": "2026-11-04"}
  },
  "record": {"title": "Investigate", "done": false}
}
~~~

For replacement, use expected type revision with the returned revision token.
An omitted payload field disappears. An empty object remains a present record.
Unknown or missing alternate names and incorrect key shapes are rejected.
Key components need not appear in the record. Alternate values are authoritative
caller input; the database does not infer or check a relation to payload fields.
Alternates are nonunique: one address can point to several primary records.

Get returns the complete payload, alternate addresses and opaque revision.
Lookup returns primary keys and those same complete records. Every successful
replacement mints a fresh record revision, even when the values are unchanged.
Stale revisions reject the entire mutation. There are no per-field stamps,
field updates, field merging or field-level conditions.

Deletion takes schema, primary key, every alternate key and revision. Supplied
alternates must match the current record. It uses the stored addresses to remove
pointers. Delete and recreate cannot reuse an old token.

## Components and sequence

1. lastdb_model::whole_record contains pure request, schema, key and record data.
2. main.rs selects PLAIN or NESTED and injects one format definition into Store.
3. writer.rs owns the sole native handle and serializes each conditional write
   from old-record read through commit. Native ownership excludes another process.
4. The format checks the immutable schema and whole-record expectation. It asks
   its nested encoder for a payload and composes the record plus pointer changes.
5. Admission checks every encoded key/body, unique address, batch size and count,
   then returns an opaque Batch. Lower components cannot mint native writes.
6. The owner consumes that Batch and publishes one durable native commit before
   returning a receipt. Replaced field values are never merged with old values.
7. Alternate lookup holds one native snapshot for pointers and primary reads.

The formats share that owner and differ in physical collection/data names,
record version and injected payload functions. NESTED composes a versioned child
envelope. The format grammar and limits live in format/mod.rs; nested families
live in its children. Only writer.rs may use raw native capabilities.

## Running the CLI in CI

~~~text
lastdb-record-spike --home <new-ci-home> --format nested init
<schema-json>   | lastdb-record-spike --home <home> schema notes
<mutation-json> | lastdb-record-spike --home <home> put
lastdb-record-spike --home <home> get notes --hash note-17
lastdb-record-spike --home <home> lookup notes by_team --hash engineering
<delete-json> | lastdb-record-spike --home <home> delete
~~~

The manually dispatched whole_record_spike job runs both formats in fresh runner
homes, including independent records, two named alternates, pointer moves,
rejection before commit, empty replacement, deletion/recreate and hash-range keys.
Each invocation reopens the native home. CI summary/artifacts retain the source
revision and transcript. Owned temporary homes are cleaned on exit.

## What this spike does not establish

This is a storage/API slice, not a primary replacement or app/SDK migration.
It has no payload field schema, schema edits, sparse alternates, unique-alternate
reservations, multi-record requests, files, history, backup, cloud replay,
distributed conflict policy, idempotent lost-ack retries or core queue/cache.
Revision checks provide same-node conditional writes, not a cross-device lock.
Native deletion has no distributed replay tombstone. A production version must
define delete/replay ordering before accepting replicated mutations.

Lookups support exact hash/hash-range addresses, at most 64 results, with no
pagination or ordered range API. The native read budget bounds resource use.
Record encoding is capped at 64 KiB including its envelope and alternate keys,
with at most eight alternates. No production latency or disk-size claim follows
from the small CLI demonstration; it is not a workload comparison.
