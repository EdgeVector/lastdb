# Spike: query-defined `lastdb.slice.v1` multi-schema round-trip

**Card:** `spike-lastdb-query-defined-slice-roundtrip`  
**Date:** 2026-07-14  
**Verdict:** **GO for values + first-class file blobs** (Mini UDS / Discovery wiring still product work)

## Question

Can a **query** define a real LastDB data slice (many schemas × fields × rows,
with **content-addressed photo/file bytes in the slice**) that packages as
`lastdb.slice.v1`, moves to another database, imports, and re-queries
successfully — including loading the photo bytes on the recipient?

## What we ran

```bash
cargo nextest run -p fold_db -- query_defined_multi_schema_slice_roundtrips
# PASS
```

## First-class files (landed in this spike)

| Piece | Status |
|-------|--------|
| `payload.blobs[]` | First-class; CAS over **raw** file bytes |
| File field atoms | `$lastdb_file` **pointer only** (no embedded `content_b64`) |
| Legacy `$file` embed | Auto-promoted to pointer + blob on materialize |
| Local CAS on import | `sharing/blob_cas` sled tree `cas_blobs` |
| Recipient load path | `blob_cas::get_blob_bytes` / `resolve_file_bytes` |

So when you share a data slice that includes photos, **the photo bytes are in
the sealed slice** (`blobs[]`) and still available after import.

## Still open (product, not kernel proof)

1. Mini UDS stage/import endpoints (no product socket surface yet).
2. Discovery `approve_deliver_request` still delivers JSON field bags — must
   call `materialize_query_slice` + transport sealed `lastdb.slice.v1`.
3. Import is mutation rewrite (not full signed molecule transplant).
4. Multi-schema still = multi query legs.

## Library surfaces

- `delivery_wire::{ContentAddressedBlob, content_addressed_blob, decode_blob_bytes, lastdb_file_pointer, validate_blob_refs}`
- `blob_cas::{put_blob, get_blob, get_blob_bytes, put_blobs}`
- `query_slice::{materialize_query_slice, import_slice_via_mutations, resolve_file_bytes}`

## Recommended next product PRs

1. Discovery approve → materialize query legs from DiscoverableSet → sealed slice.
2. Mini owner-socket deliver stage + peer import.
3. Optional: signed molecule transplant when provenance is required.
