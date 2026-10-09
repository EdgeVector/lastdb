# Mini cutover P5 — remove sled residual (capstone)

**Status:** 2026-08-05 — product path is **laststore-only**. This document is the
P5 residual closeout checklist for operators and the `mini-cutover-p5-remove-sled`
card. Code removal landed earlier (p5a/p5b, PR #773 / #776 / #1055 and the
2026-07-22 sled crate rip); P5 is the residual guard + ops proof, not a re-flip.

North Star: `north-star-lastdb-no-sled-document-store`  
Related: `docs/history/mini-cutover/sled-pool-call-sites.md`,
`decision-2026-08-04-mini-cutover-p4-done-p5-remove-sled`.

## Code guarantees (already on main)

| Guard | Where |
|-------|--------|
| `StorageEngine` is Laststore-only | `fold_db/.../storage/config.rs` |
| `LASTDB_ENGINE=sled` fails parse | `StorageEngine::from_env` / `FromStr` |
| Factory default is Laststore | `resolve_storage_engine` + factory tests |
| Config JSON `engine: "sled"` fails serde | unit test in `config.rs` |
| No `sled` crate in product Cargo graph | workspace / lockfile |
| Legacy `conf`+`db` still counts as "existing store" | `lastdb_node::host::data_dir_has_existing_store` — refuse fresh boot over old backup trees; does **not** open sled |

## Live inventory (read-only; 2026-08-05)

On Tom's primary Mini home (`~/.lastdb`):

- Layout marker present: `~/.lastdb/data/laststore-layout-v1`
- Collections under `~/.lastdb/data/data/*` (Last Store planes)
- No classic Mini sled tree at `~/.lastdb/db` or `~/.lastdb/data/db`
- Live daemon reports Last Store planes; dual-read `legacy_hits=0`
- LaunchAgent / host path uses laststore product binary (`lastdbd`)

Re-check anytime:

```bash
test -f "$HOME/.lastdb/data/laststore-layout-v1" && echo laststore-layout=yes
ls "$HOME/.lastdb/data/data" | head
test -e "$HOME/.lastdb/db" && echo WARN classic-home-db || echo no-classic-sled-db
lastdb status | head -20
kanban ping
```

## Operator checklist — deleting leftover sled **bytes** (if any appear)

**Never** delete live primary data as step 1. Order is mandatory:

1. **Confirm product is laststore-only** (code on the running binary + env).
2. **Durable offline backup** of the whole home (retain through any soak window).
   Prefer the same durable-copy path used by `lastdb-safe-upgrade` / cutover
   backups — not an ad-hoc `rm` of live trees.
3. **CoW / ephemeral proof** on a **copy** of the real data:
   - Boot candidate/laststore-only node on the copy
   - Representative `brain get` / `kanban list` / write on throwaway data
   - Prove no process opens a sled tree under the copy
4. **Only then** remove leftover classic sled files on the live home if inventory
   still shows them (historical note: `~/.lastdb/data/db` or sibling `conf`+`db`).
   Do not touch Last Store collection dirs under `data/data/`.
5. If a LaunchAgent restart is required, use **`lastdb-safe-upgrade`** discipline
   (ephemeral probe first). Never kill/restart primary as the first experiment.

If inventory shows **no** classic sled tree (current primary as of 2026-08-05),
disk delete is a no-op — record that in the completion checkpoint and stop.

## Verification for the P5 card

- Unit: `cargo test -p fold_db storage_engine_rejects_sled` (and sibling config tests)
- Live: `kanban ping` + `lastdb status` healthy; laststore layout marker present
- Brain: after merge/ops proof, append
  `F-Kanban completion checkpoint: mini-cutover-p5-remove-sled` on
  `north-star-lastdb-no-sled-document-store`
- Health: `mini-cutover-health-check --json` should stop listing P5 as missing
  evidence once the checkpoint exists and the card is done

## OUT OF SCOPE (still)

- Re-running P4 primary flip
- Cloud-sync pin-mode / off-machine backup product
- Repairing read-integrity dangling tips
- Forgejo retirement / fold LastGit-primary
