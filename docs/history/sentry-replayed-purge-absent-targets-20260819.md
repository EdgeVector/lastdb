# Investigation: Sentry 7677292803 replayed-purge-absent-targets

**Date:** 2026-08-19
**Card:** `hga-invest-sentry-replayed-purge-20260819`
**Parent:** `sentry-replayed-purge-absent-targets-20260818`
**Investigator:** routine `last-stack-fkanban-pickup` run `2026-08-19T19-48-56-477Z`

## Question

Keep a human gate for an authorized Sentry re-query of issue 7677292803, or ungate agent-owned closeout of the already-shipped idempotency fix?

## Evidence

| Fact | Source |
|------|--------|
| Product fix merged as Forgejo **#1578** | merge `1dd52bcc2` from `kanban/lastdb-purge-replay-refuses-on-absent-targets` |
| Fix commit | `eabf898b4` `fix(sync): a replayed purge must converge on absent targets, not pin the backup cursor` |
| Live primary contains the fix | `lastdb` `0.23.3-901-g187699c29`; `git merge-base --is-ancestor eabf898b4 187699c29` → true |
| `origin/main` contains the fix | `git merge-base --is-ancestor eabf898b4 refs/heads/main` → true |
| Replay policy on main | `WriteOrigin::Replay` in `fold_db/.../mutation_manager/write.rs`; test `replayed_purge_of_an_already_purged_key_succeeds` present |
| Live Cloud Sync 2026-08-19 ~19:58Z | no `replay_blocker`; `degraded_reasons=mutation_log_lag` only (not `sync_failing`); RPO ~1–2m; thousands of purges this process |
| Authorized Sentry GET | macOS keychain `sentry-auth-token` / `edge-vector`; `GET /api/0/issues/7677292803/` HTTP 200 |
| Issue state | `RUST-3T`; `status=unresolved`; `count=7`; `userCount=0`; `firstSeen=2026-08-17T21:59:06.664568Z`; `lastSeen=2026-08-18T00:27:28Z` |
| Original triage | same count=7 and same lastSeen; event volume is **flat** for ~44h |
| Sentry mutation | none (read-only GET; issue left unresolved) |
| Unrelated live noise | Backup FAILING on unbackable snapshot chunks — **not** this cursor-pin defect |

Brain papercut `papercut-lastdb-replayed-purge-refuses-on-absent-targets-and-pins-the-backup-cursor` is `fixed` (Fold #1578). Its remaining live bar was: primary binary contains `eabf898b4`, and a later telemetry purge does not re-pin `replay_blocker`. Both are now true.

## Recommendation 2026-08-19

**B — ungate and close the parent.** Do not implement a second purge-idempotency PR. The Kind: pr work is #1578. The remaining Sentry VERIFY line is satisfied by a flat event count (`count` still 7, `lastSeen` unchanged). Leave the Sentry issue unresolved; a UI Resolve click is optional hygiene, not a board gate.

**A (rejected):** keeping `needs_human` only for a second human Sentry look would re-block a merged, live fix whose issue volume has not moved.

## Invest card closeout

Durable proof also lives at `~/.last-stack/feature-proofs/hga-invest-sentry-replayed-purge-20260819.md`. This history note is the merged `Kind: pr` artifact for the investigation child.
