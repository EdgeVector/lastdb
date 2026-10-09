# Strict startup progress

Set `LASTDB_STARTUP_PROGRESS_JSON=1` to request aggregate JSON events on stderr.
The default is off. The flag does not change the strict check or its result.
The event schema is `lastdb.startup.strict_progress.v1`.

The reporter emits one initial snapshot, a snapshot every five seconds, and a
terminal snapshot. Each snapshot has a total record and records for nonempty
collection and key-family counters. Each record is one JSON line. A snapshot
has at most 43 records. Each line is below the proof parser's 8 KiB cap.

The reporter shares the restore reporter's bounded queue and worker. A stalled
stderr sink cannot block a page read, predicate, or process exit. Final delivery
waits at most 100 milliseconds. Output is best effort; it is not a success proof.

Every label comes from a fixed source list. Unknown labels become `other`.
Events contain no raw key, identifier, value, path, or error text. All enum
fields remain present when their value is null. Counters saturate at u64::MAX.

| Field | Meaning |
|---|---|
| `scope` | `total`, `collection`, or `key_family` |
| `collection` | The named collection scope; in the total record, the last fetched page's collection |
| `key_family` | The named key-family scope; otherwise null |
| `phase` | `start`, `physical_page`, `presence`, `fallback`, `predicate`, `complete`, or `failed` |
| `result` | null, `clean`, `plaintext`, or `error` |
| `elapsed_ms` | Time since this strict check starts |
| `phase_elapsed_ms` | Time since the current operation starts |
| `rows_seen` | Physical rows fetched |
| `rows_completed` | Resolved rows in pages that produce an outcome; this is not a predicate-evaluation count |
| `pages_completed` | Pages that produce an outcome; a family count includes each page that contains that family |
| `canonical_direct` | First-form, first-collection values reused directly |
| `absence_proven_direct` | Page values reused after all higher candidates are absent |
| `preferred_present` | Rows that require a logical read because a higher candidate exists |
| `source_outside_order` | Rows whose physical source is outside their logical candidate order |
| `untrusted_source` | Rows without native source provenance |
| `fallback_batches` | Logical batches; a family count includes each batch that contains that family |
| `fallback_keys` | Keys sent to those batches |
| `presence_batches` | Native presence batches, in the total record only; scoped records use zero |
| `preferred_collection` | Last observed first-present candidate in this scope; it can belong to an earlier page |
| `preferred_form` | That candidate's `anchored`, `colon`, or `single` form; otherwise null |

The counters retain fixed arrays for 15 collections and 27 key families.
They do not retain a key set or value body. The existing 16-value page bound,
group inventory, backend cache budgets, read order, and marker rules stay intact.
The predicate still stops at its first plaintext value. A presence or fallback
error still propagates before a plaintext veto.

The diagnostics identify work. They do not skip a lower-priority physical row
or assume that another page will prove its logical winner.
# Bounded startup presence cache

The strict check uses a startup-only native existence method. It admits a complete validated sidecar ID set into the existing key-index cache. Repeated presence probes can then use that set without another full sidecar read and decode.

The method requires the pre-writer startup phase. No concurrent native write or authority mutation can occur during admission. Ordinary existence reads retain their existing behavior. Later writes invalidate the admitted set through the normal handle-load path.

The existing cache byte budget controls admission and eviction. Disabled caches and groups above the budget receive no admission. Partial physical-page windows never enter the cache. Sidecar refusal still uses the authoritative path. The 16-value bound, logical precedence, complete-page error behavior, and strict marker rules remain unchanged.
