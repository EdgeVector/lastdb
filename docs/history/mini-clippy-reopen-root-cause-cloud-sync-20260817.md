# Investigation: Fold Mini CI reopen root cause (cloud-sync parents)

**Date:** 2026-08-17  
**Card:** `hga-invest-fold-mini-clippy-reopen-root-cause`  
**Parents:** `file-blob-known-ref-reupload`, `cloud-sync-log-peer-apply-bootstrap`  
**Investigator:** routine `last-stack-fkanban-pickup-w2` run `2026-08-17T12-05-04-291Z`

## Question

Why did Fold Mini / `ci-required` repeatedly reject thin-slice reopen attempts for these cloud-sync cards, and is the agent anti-churn `needs_human` hold still warranted?

## Evidence collected

### Parent A — `file-blob-known-ref-reupload`

| Fact | Source |
|------|--------|
| Merged Forgejo PR **#1440** "Re-upload known file blobs when remote CAS is missing" | `merged_at=2026-08-10T18:27:24-07:00`, merge `a1cacc577` |
| Merge commit is on current `main` | `git merge-base --is-ancestor a1cacc577 HEAD` → true |
| Product code on main | `file_blob.rs` has `reupload_known_file_blob` + "known file blob missing from remote CAS" path; tests `known_file_blob_reuploads_same_dek_when_remote_object_missing` |
| CI self-heal history on the branch | commits `3ed9b5758` / `fe9411817` `fix(ci): rustfmt for file-blob known-ref reupload`, `178c7f759` style format — classic Mini fmt fix, then merge |
| Card body empty + still `backlog` / `needs_human` waiting this investigation | board show 2026-08-17 (stale placement after merge) |

**Conclusion A:** The Mini gate did **not** permanently block this card. It needed fmt/style fixes, then landed. **Anti-churn hold is obsolete.** Close the parent to `done` against #1440.

### Parent B — `cloud-sync-log-peer-apply-bootstrap`

| Fact | Source |
|------|--------|
| Forgejo **#1268** and **#1287** closed unmerged (same branch `kanban/cloud-sync-log-peer-apply-bootstrap`) | forge API; reaper comments |
| Reaper reason text | `ci-required` RED (`fmt+clippy Mini`); age SLA close; reopen-churn park |
| Pipeline-health finer label | `fold#1268:lib-test-fail`, later `fold#1287:lib-test+unmergeable` (heartbeats 2026-08-05) — **lib tests**, not only rustfmt/clippy |
| Preserved tip | `6d3d7ec4f` — single commit +567/−9, almost all `pin_log.rs` |
| Local `cargo fmt --all -- --check` on tip **2026-08-17** | **GREEN** (rc=0) |
| Peer-apply symbols on current main | `run_mutation_log_peer_apply` / `MutationLogPeerApplyReport` **absent** from `sync/engine/` |
| pin_log size drift | tip branch ~2498 lines vs main ~5690 lines — monopr cannot be reopened cleanly without rebase rewrite |
| Misleading heartbeat | 2026-08-11 claimed parent `result=merged` on fold **#1437**, but #1437 is "Prove copied-home S0 plus post-base log restore" (`f2978ab7a`) — related restore plane, **not** the peer pull+apply monopr |

**Conclusion B:** Failures were **real Mini-lane RED on a large monopr** (pipeline: lib-test; reaper: coarse "fmt+clippy Mini"). They were **not** a broken Mini infrastructure ban on cloud-sync work. Reopen-churn of the **same red monopr** was correctly parked. The hold should lift only for **new thin slices rebased onto current main**, not for reopening #1268/#1287 as-is.

### What Mini actually gates (current main)

From `.forgejo/workflows/ci.yml` job `fmt + clippy (lastdb_node Mini lane)`:

1. `cargo fmt --all -- --check`
2. `cargo clippy -p lastdb_node --lib --bins -- -D warnings`
3. `cargo test -p fold_db --lib` **and** `--features cloud-sync`
4. Plus additional fold_db / laststore / lastdb_node test steps in the same required job

So a reaper string of "fmt+clippy Mini" can hide **lib-test** failures (as pipeline-health recorded for #1268/#1287).

## Root cause (durable)

1. **Primary:** oversized single-PR surface (`pin_log.rs` +567) that could not go green inside the 1h reaper SLA while Mini was also red on lib-tests / mergeability.
2. **Secondary:** reopen-churn of the **same branch tip** without splitting or fixing the failing lib-tests → correct `needs_human` anti-churn park.
3. **Not causal:** permanent Mini clippy ban on thin cloud-sync slices; file-blob proves thin fmt-fixed work still merges.
4. **Stale board state:** parent cards not closed after #1440 merge / not refreshed after false #1437 close association.

## Recommendations (2026-08-17)

### `file-blob-known-ref-reupload`

- **Anti-churn hold: obsolete.**
- **Action:** close `done` with PR `http://localhost:3300/EdgeVector/fold/pulls/1440` (or 100.109.94.59 equivalent). No reopen, no CI repair, no Tom gate.

### `cloud-sync-log-peer-apply-bootstrap`

- **Do not reopen monopr #1268/#1287 or push the preserved tip as-is.**
- **Reopen as split slices on fresh `origin/main`** (already sketched on the card):
  1. **Slice 1:** peer list/pull of mutation-log segments only (+ unit tests); no apply, no S0.
  2. **Slice 2:** apply path for pulled segments onto cold/empty home.
  3. **Slice 3:** optional bootstrap S0 when home empty.
- **CI repair required per slice:** green Mini including `cargo test -p fold_db --lib --features cloud-sync` before reaper age SLA; keep each diff small enough to fix fmt/clippy/lib-test in one fire.
- **Anti-churn hold:** lift for **new** thin-slice PRs only; keep the ban on reopening the reaped monopr heads.

### Invest card closeout

Record this proof under `~/.last-stack/feature-proofs/` and mirror a short history note in fold so `Kind: pr` has a merged artifact.
