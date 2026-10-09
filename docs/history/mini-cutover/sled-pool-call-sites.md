# Mini cutover — SledPool residual (REMOVED)

**Status:** 2026-07-22 — sled crate and `SledPool` removed from product path.

## Last Store product path (done)

| Area | Status |
|------|--------|
| Primary document store | Last Store collections only |
| Node config | NamespacedStore `node_config` only |
| Share / blob CAS / org targets | `*_in_ops` / DbOperations |
| FoldDB.sled_pool() | **Removed** |
| Factory | Laststore only; no SledPool |
| `StorageEngine` | `Laststore` only; `LASTDB_ENGINE=sled` fails parse |
| Crate dep `sled` | Removed from fold_db / lastdb_node / workspace |
| Offline sled tools | Removed; `force_cloud_snapshot` binary deleted; `lastdb_local_maintain` is Last Store residue drains only |

## Ops notes

- Leftover on-disk `~/.lastdb/data/db` (old sled file) can be deleted after soak; product no longer opens it.
- Cloud restore: `lastdb restore --into` (not legacy personal bootstrap).
- **P5 residual capstone checklist:** `docs/history/mini-cutover/p5-remove-sled-residual.md`
  (inventory, safe delete order, verification for `mini-cutover-p5-remove-sled`).
