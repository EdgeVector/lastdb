# Design: Native Index Posture — SUPERSEDED

| Field | Value |
|-------|--------|
| Status | **Superseded.** Mini has no native index. Search is LastSeek on top of LastDB (2026-08-08). |
| Product owner | Tom |
| Repo | LastSeek (`https://github.com/EdgeVector/lastseek`), not inside `lastdbd` |
| North Star | `north-star-lastdb-strip-native-index` (PASS 2026-08-01) |

**Do not implement this file.** Mini `GET /api/native-index/search` must be 503 `search_plane_required`. Hits are a regression.

## Decision (current)

1. **Mini does not ship in-process semantic recall or a native index.** LastSeek is the engine — own process, on top of LastDB.
2. **Keep embedding off the Mini write hot path.** Mini must not run model inference.
3. **LastSeek owns ranking, embeddings, and rebuild.** Mini owns records.
4. Unsupported Mini search paths must **503**, never empty `ok: true`.

## Historical In-Process Pieces

| Piece | Location |
|-------|----------|
| Manager | `NativeIndexManager` on `DbOperations` for explicit compatibility/test paths |
| Tree | `native_index` (local-only / regenerable) |
| Product default | Disabled; no in-process Mini semantic recall |
| Model | Not shipped in the Mini product graph; FastEmbed belongs to Search/schema tooling |
| Sink contract | Historical `IndexSink::apply_change_batch(IndexChangeBatch)` seam |

These pieces remain only where compatibility, migration, or explicit tests need
to reason about old local index state. Operator docs and product paths should
point to Search-app delivery instead.

## App vs kernel

| Approach | Use when |
|----------|----------|
| **In-node optional subsystem** | Historical compatibility only; not a Mini product path |
| **Hybrid `IndexSink` trait** | Migration seam for emitting typed change batches while staying off-path |
| **Search worker app + change feed** | Product path: ship/update embedder without folding model/runtime dependencies into `fold_db` |

`IndexChangeBatch` is schema-scoped and ordered. Each change carries the source
mutation id, normalized key, `upsert` or `tombstone`, upsert fields, and the
schema's searchable-field allowlist when present. The current
`NativeIndexManager` remains a compatibility implementation for old local index
state and targeted tests. New product recall should use Search app delivery.

**Not accepted:** reintroduce a default Mini FastEmbed/native semantic product
path. Search ownership is external to the Mini kernel.

## Work items (not all in this PR)

- [x] Document posture (this file)  
- [x] Indexing already spawned off critical path (`index.rs`)  
- [x] `IndexSink` + typed schema-scoped change batch  
- [ ] Optional: stronger metrics (“index lag”, queue depth) on `/api/status`  
- [ ] Host delivery of change batches to the external Search app
- [x] Remove default Mini FastEmbed/native semantic packaging

## Interaction with minimal tips

Thin tips do not remove the need for field **text** at Search indexing time:
the Search app still needs resolvable values. Search quality is independent of
tip fatness. Minimal tips may make convergence clearer because writes return
faster while indexing remains off-path.
