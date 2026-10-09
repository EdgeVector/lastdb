# macOS code signing (Mini / Homebrew only)

> **Desktop DMG path is archive-only.** The LastDB.app / Tauri / `.dmg`
> product and its signing walkthrough were removed in the Mini-only
> cutover (2026-07-12). Restore point:
> branch `archive/desktop-dmg-pre-removal`.
>
> Do **not** follow any historical DMG or Tauri signing steps as the
> current release process. There is no desktop UI in this tree.

## Current product

LastDB Mini ships **`lastdb` + `lastdbd`** CLI/daemon binaries via
Homebrew (`edgevector/lastdb/lastdb`). Release train:

- Operator doc: [`RELEASING.md`](RELEASING.md)
- Workflow: [`.github/workflows/release.yml`](../.github/workflows/release.yml)
- Public assets: `EdgeVector/homebrew-lastdb` releases

CLI binaries may still be Developer ID–signed in CI so keychain ACLs stay
stable across `brew upgrade`. That is a **binary** concern, not a DMG
or app-bundle pipeline.

## Historical archive

Full pre-removal macOS DMG / notarization / Tauri updater material lives
only on `archive/desktop-dmg-pre-removal`. If you need that content for
forensics, check out that branch — do not reintroduce it on `main`.
