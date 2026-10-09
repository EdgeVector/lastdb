# CoW proof: legacy_blob_refs inventory + safe purge gate

Card: `lastdb-cow-purge-legacy-blob-refs`  
North Star: `north-star-lastdb-ideal-storage-shape`  
Date: 2026-08-05

## Policy (code)

`lastdb db purge-ref-blobs` (and `POST /api/db/purge-ref-blobs`) now classifies every
`ref:{M}` key in the cold `legacy_blob_refs` collection:

| Class | Criterion | Action |
|-------|-----------|--------|
| **safe** | molecule already has per-key coverage (`mh:{M}` or any `mk:{M}:…`) | dry-run counts; `--execute` deletes |
| **blocked** | no per-key coverage (sole-copy residue) | never force-deleted; samples returned |

`purge_complete` is true only when `keys_blocked == 0` and (dry-run or all safe keys deleted).

Product writers already refuse new `ref:` blobs (dep PR #1213 / card
`lastdb-stop-writing-legacy-ref-molecule-blobs`).

## CoW surface (primary untouched)

| Item | Value |
|------|-------|
| CoW home | `/tmp/lastdb-cow-purge-legacy-blob-refs-16681` |
| CoW socket | `…/data/folddb.sock` (ephemeral; not `~/.lastdb`) |
| CoW `legacy_blob_refs` du | **7.4M** |
| Primary `legacy_blob_refs` du | **7.4M** (unchanged; no purge executed against primary) |
| Primary writes this run | **none** |

Captured plane map (prior session, `cow-status.txt`): cold/sync plane lists
`legacy_blob_refs` among cold collections (~80.8 MiB cold/sync aggregate on that
snapshot; collection file tree remains 7.4M on disk).

## Inventory outcome (this fire)

- Running CoW Mini at inventory time did **not** yet expose `/api/db/purge-ref-blobs`
  (installed bottle older than this PR) — dry-run via live API returned Not Found.
- Offline / plane evidence: non-empty `legacy_blob_refs` (7.4M). Without the new
  per-key safety classifier on the running binary, **execute was not run** on CoW
  (card rule: do not force-delete non-rehydratable residue).
- Unit tests in this PR prove: dry-run no-delete; execute deletes only safe keys;
  blocked sole-copy keys remain; complete when all safe.

## Follow-up after merge

1. Ship Mini build that includes this gate.
2. On a fresh CoW of `~/.lastdb`: `lastdb --data-dir <cow> db purge-ref-blobs --json`
   then `--execute` only when report shows acceptable `keys_blocked` (prefer 0).
3. If `keys_blocked > 0`, file rehydrate follow-up — do not force-delete.

## Primary hygiene

Never point `purge-ref-blobs --execute` at live `~/.lastdb` as the first proof surface.
