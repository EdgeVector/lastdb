# Release pipeline (historical design — superseded)

> **Status:** historical design artifact (Tom Tang · 2026-05-21).
> **Not the current release process.**
>
> Current operator process for shipping LastDB Mini (Homebrew only):
> **[`RELEASING.md`](RELEASING.md)** and
> [`.github/workflows/release.yml`](../.github/workflows/release.yml).
>
> The desktop / Tauri / DMG release train is **gone** (Mini-only cutover
> 2026-07-12). Restore point: branch `archive/desktop-dmg-pre-removal`.
> Agents must **not** treat this file as instructions to ship a DMG,
> build a Tauri app, or target `fold_db_node` as a live package.

## What ships today

| Artifact | Source crate | Venue |
| --- | --- | --- |
| `lastdb` + `lastdbd` tarballs | `lastdb_node` | `EdgeVector/homebrew-lastdb` releases |
| Formula bump | `scripts/release/bump-homebrew-formula.rb` | PR on `EdgeVector/homebrew-lastdb` |

Trigger: annotated tag `vX.Y.Z` pushed to the **GitHub** remote of
`EdgeVector/fold` (see RELEASING.md). No PR, no DMG, no desktop updater.

## Why this file still exists

It recorded the 2026-05 monorepo brew-relight design (first monorepo tag
`v0.5.0`, public mirror on `homebrew-lastdb`, etc.). Much of that intent
landed, then the product narrowed to Mini-only. Keep this stub so old
links resolve; do not expand it with desktop instructions.

For the archived full design text (including pre-Mini `fold_db_node`
build matrix notes), use git history of this path or
`archive/desktop-dmg-pre-removal`.
