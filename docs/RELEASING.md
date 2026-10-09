# Releasing LastDB (Mini / Homebrew only)

> **Current process ships only LastDB Mini** (`lastdb` + `lastdbd` from
> `lastdb_node`) via Homebrew. There is **no** desktop UI, Tauri app, or
> DMG release train. Pre-Mini desktop material lives on
> `archive/desktop-dmg-pre-removal` only — do not reintroduce it.
>
> Related: short signing pointer in
> [`macos-code-signing.md`](macos-code-signing.md); historical monorepo
> design stub in
> [`release-pipeline-monorepo.md`](release-pipeline-monorepo.md).

## Current path: the last-stack canary chain (automatic)

Stable releases ship from the host canary chain in `EdgeVector/last-stack`,
not from a tag push. Proved end to end on 2026-09-24 (fold `b734bebff`
→ brew `v0.23.6`).

| Step | Runs | Tool (last-stack `bin/`) |
| --- | --- | --- |
| Build Forge `main` → staged bundle | routine `lastdb-canary-candidate-set`, daily 02:47 PT | `last-stack-canary-build-main` |
| Candidate set + isolated install smoke on the candidate `lastdbd` | same run | `last-stack-canary-candidate-set`, `skills/llms-txt-install-smoke/run.sh` |
| Primary cutover (Loom safe-upgrade, CoW bars first) | same run | `last-stack-lastdb-canary-dogfood --cutover` → `last-stack-safe-upgrade-loom` |
| Proved rows → registry `next` (tap PR) | same run | `last-stack-registry-publish-next` |
| 1 h quiet window on the primary (24 h before 2026-09-24) | routine `lastdb-canary-soak-watch`, hourly | `last-stack-canary-reconcile-gate` (`LAST_STACK_CANARY_V2_WINDOW_SECONDS`) |
| Green window → brew stable tag + formula PR + registry `stable` rows | same tick | `last-stack-canary-promote-material` → `last-stack-release-publish --if-needed` |

`last-stack-release-publish` calls `scripts/release/forge-promote-homebrew-stable.sh`
from this repo's GitHub `main`. Dry run:
`last-stack-release-publish --lastdb-version <build> --dry-run`.

## Legacy: GitHub tag push

Before the 2026-09-06 venue move, a release was one tag push:

```bash
git tag -a v0.5.0 -m "v0.5.0" main
git push origin v0.5.0
```

EdgeVector/fold is canonical on GitHub again since 2026-09-29
(`decision-2026-09-29-fold-venue-back-to-github`), but `release.yml` and
`nightly-canary.yml` are **disabled** in the GitHub Actions UI. A tag push
therefore builds nothing today. Do not enable either without Tom: the canary
chain above creates the public homebrew-lastdb release itself, so an enabled
`release.yml` would race it on every pushed `v*` tag, and `nightly-canary.yml`
would cut a real tag and public prerelease every day. The sections below
describe that legacy `release.yml` path and its gates.

## Nightly canary (automatic prerelease — does NOT bump brew)

Workflow: [`.github/workflows/nightly-canary.yml`](../.github/workflows/nightly-canary.yml)

| | |
| --- | --- |
| **When** | Daily 07:00 UTC, or Actions → **Nightly Mini Canary** → Run workflow |
| **What** | If `main` has new commits since the latest `v*` tag, cut `vX.Y.Z-canary.YYYYMMDD` (next patch after the latest **stable** homebrew-lastdb release) and push it to GitHub |
| **Then** | Existing `release.yml` builds, runs gates, publishes a **prerelease** on `EdgeVector/homebrew-lastdb` |
| **Brew formula** | **Not updated** (tags containing `-` skip `bump-tap`) |
| **Skip** | Same-day canary already exists, or no new commits (unless `force` on dispatch) |
| **Discord** | Cut/skip/failure from the canary workflow; build/publish outcome from `release.yml` notify jobs (`DISCORD_WEBHOOK_RELEASE` or `DISCORD_WEBHOOK` org secret) |

Promote a green canary to stable brew:

```bash
# same commit as the canary tip (or current main once green)
git tag -a v0.22.8 -m "v0.22.8" <sha>
git push github v0.22.8
```

**Upgrade-decrypt gate (2026-07-13; hard-break 0.23):** script
`scripts/release/validate-mini-upgrade-decrypt.sh` still exists for local/
manual checks. **`release.yml` skips it for the 0.23 line** (Tom 2026-07-24):
public `0.22.9` homes are still **sled**; post-cutover Mini is **Last Store
only**, and there are no other production users. **No in-place upgrade path
from 0.22.x sled is promised.** Fresh-install gate remains required.

Local repro (when re-enabled against a laststore-era previous tag):

```bash
cargo build --release -p lastdb_node --bin lastdb --bin lastdbd
bash scripts/release/validate-mini-upgrade-decrypt.sh \
  --previous-tag v0.22.9 \
  --candidate-built target/release
```

This is the gate that would have blocked v0.22.6 (incident-lastdbd-0226).

**Semantic-search release posture:** Homebrew LastDB Mini ships the default
`lastdb` and `lastdbd` binaries fastembed/ONNX-free. Shadow-pack/FastEmbed
coverage stays in the explicit non-shipping semantic smoke lane, guarded by
`scripts/release/validate-mini-semantic-release-test.sh`.

**Fresh-install first-run gate (2026-07-14):** same job also runs
`scripts/release/validate-mini-fresh-install.sh --built …`, which boots the
candidate on a pristine throwaway home and exercises `/api/schemas/declare`,
`/api/apps/declare-schema` (brain/kanban init path), mutation/query, semantic
search, and restart persistence. Local repro:

```bash
bash scripts/release/validate-mini-fresh-install.sh --built target/release
```

**Paired bundle manifest (2026-07-22):** packaging runs
`scripts/release/package-lastdb-bundle.sh`, which refuses to publish unless
both `lastdb` and `lastdbd` are present, executable, and report the same
version. It emits the Homebrew tarball plus `lastdb-<triple>.manifest.json`.
The sidecar manifest records the source git OID, tarball SHA-256, and each
binary's version and SHA-256 so the safe-upgrade path can prove it is promoting
a matched CLI/daemon bundle, not a daemon-only or skewed candidate. Local
cheap repro:

```bash
bash scripts/release/package-lastdb-bundle-test.sh
```

**Venue (2026-09-29):** `EdgeVector/fold` and `EdgeVector/homebrew-lastdb` are on GitHub.
Decision: `decision-2026-09-29-retire-lastgit-all-repos-to-github`.
LastGit (`lastdb:///`) and the Forgejo copies are retired.
Do not push a tag or a formula to those remotes.

The legacy tag path, when [`release.yml`](../.github/workflows/release.yml) is enabled, pushes the tag to GitHub.
That workflow ships the LastDB Mini channel (`lastdb` + `lastdbd` from `lastdb_node`, Apple Silicon only).
The formula bump is a GitHub PR from `scripts/release/forge-promote-homebrew-stable.sh --publish`.
The release train produces the LastDB Mini artifacts:

1. **LastDB Mini** public release `vX.Y.Z` on `EdgeVector/homebrew-lastdb` with
   binary tarballs (e.g. `lastdb-aarch64-apple-darwin.tar.gz`), matching
   `lastdb-<triple>.manifest.json` sidecars, and `SHA256SUMS.txt`. This is the
   single release object for the version and the canonical Homebrew/tarball
   download venue. Each tarball contains exactly `lastdb` and `lastdbd`; the
   back-compat `folddb` command is a formula-side symlink, not another shipped
   binary in the tarball.
2. Auto-PR `bump: folddb → vX.Y.Z` on `EdgeVector/homebrew-lastdb`,
   updating the formula's version + sha256 hashes (URLs point at the
   release from (1)).
3. Tag itself, immutably pinned to the squash commit on `main`. `EdgeVector/fold`
   has no release object for Mini; the tag is the fold-side provenance record.

## Formula edits land on the tap; the release only bumps version/url/sha256

The Homebrew formula at `EdgeVector/homebrew-lastdb/Formula/folddb.rb` is
the **source of truth for the formula body**. The `bump-tap` job in
[`release.yml`](../.github/workflows/release.yml) does a **surgical**
in-place edit on each release — it rewrites *only* the `version` line, the
`url` version segment(s), and the `sha256` line(s), via
[`scripts/release/bump-homebrew-formula.rb`](../scripts/release/bump-homebrew-formula.rb).
Everything else (the `service do` block, caveats, install steps, comments)
is preserved byte-for-byte.

This used to be a from-scratch heredoc regeneration that clobbered any
hand-edit on the tap: PR #26 (the `service do` block making
`brew services start folddb` work) was wiped one day later by PR #27 (the
v0.5.1 auto-bump), invisible until a dogfooder tried `brew services start
folddb` after upgrading. The surgical bump kills that whole regression
class — see the fold PR that switched it over.

**Where to land formula changes now:**

- Adding / changing a `service do` block, caveat, install step, test, or
  any tap-only comment — edit `Formula/folddb.rb` **directly on
  `EdgeVector/homebrew-lastdb`** (open a PR there). It stays; the next
  release's surgical bump won't touch it.
- Adding / removing a binary that ships in the tarball — edit the
  `Build release binaries` step (in `release.yml`'s build jobs) *and* the
  `def install` block on the tap formula.
- Never edit the version/url/sha256 lines on the tap by hand — those are
  the release's to own; the bump overwrites them every tag.

There's a guard step inside `bump-tap`: after the surgical edit it diffs
`Formula/folddb.rb` and **fails the release** if any changed line is
outside `version` / `url` / `sha256`. So if the bump script ever mangles
the formula (a bug, or a structural change its matchers don't anticipate —
e.g. a fourth platform arm), the release fails loudly instead of shipping a
broken formula. Resolution is in the failing job's logs: inspect the diff,
then fix `scripts/release/bump-homebrew-formula.rb` (and its fixture test
[`bump-homebrew-formula-test.sh`](../scripts/release/bump-homebrew-formula-test.sh))
so it handles the new shape, and re-tag.

## Where the version comes from

The tag is the only Mini version source. There's no `Cargo.toml` `version =
"X.Y.Z"` to keep in sync and no commit-message regex.

- `Cargo.toml`'s `version` field is purely cosmetic. [`build.rs`](../build.rs)
  reads `GITHUB_REF_NAME` at compile time and stamps `FOLDDB_BUILD_VERSION`
  into every binary, overriding `CARGO_PKG_VERSION`. The verify-versus-tag
  step in the GitHub `release.yml` enforces this.

So `Cargo.toml` may show an old version number — that's fine, it is not
load-bearing. **Don't bump it as part of releasing.**

## Pre-release: dry-run

Before tagging, you can test the CLI release pipeline against any commit without
burning a version: in GitHub Actions for `EdgeVector/fold`, run
**Release -> Run workflow**, optionally enter a `ref` (default: `main`), and
click Run. The `release` / `bump-tap` jobs are gated to skip on non-tag
triggers, so dispatch builds and smokes the artifacts but publishes nothing.

To exercise the publish path without touching the stable formula, cut an rc tag
such as `vX.Y.Z-rc.1`. The workflow creates exactly one prerelease object on
`EdgeVector/homebrew-lastdb`, skips `bump-tap`, and leaves `EdgeVector/fold`
with only the git tag.

For a local package check with test binaries, run:

```bash
bash scripts/release/package-lastdb-bundle-test.sh
```

### Local-canonical stable cut (preferred)

Canonical public brew cut does **not** require fold GitHub Actions. One wrapper
chains tag → release build → package → promote:

```bash
# Dry-run (validate package + formula bump; no network writes):
bash scripts/release/cut-local-stable.sh \
  --version-tag vX.Y.Z \
  --dry-run \
  --tap-dir /path/to/homebrew-lastdb   # full clone or linked worktree OK

# Publish (public bottle + GitHub formula PR):
GH_TOKEN="$(gh auth token)" \
bash scripts/release/cut-local-stable.sh \
  --version-tag vX.Y.Z \
  --publish \
  --push-tag
```

Venues (do not mix them up):

| Step | Venue |
|------|--------|
| fold tag / source | GitHub `EdgeVector/fold` |
| build + package | this machine |
| bottle CDN | GitHub Releases on `EdgeVector/homebrew-lastdb` |
| formula SoT | GitHub `EdgeVector/homebrew-lastdb` PR (default) |
| formula brew reads | GitHub tap `main` (merged formula PR) |

Primary Mini on this host is **not** upgraded by the cut — use
`lastdb-safe-upgrade` separately.

### Forge-built stable promotion

Forge-built (or local) release artifacts can be promoted to the public Homebrew
boundary without running GitHub Actions in the private `EdgeVector/fold` source
repo. The promotion entrypoint consumes a directory containing
`lastdb-*.tar.gz` plus matching `lastdb-*.manifest.json`, verifies the paired
bundle metadata, writes `SHA256SUMS.txt`, publishes the bottle to GitHub
Releases (CDN), and opens a **GitHub formula PR** on `EdgeVector/homebrew-lastdb`.

Dry-run against a local tap checkout (full clone **or** linked worktree):

```bash
bash scripts/release/forge-promote-homebrew-stable.sh \
  --dry-run \
  --version-tag vX.Y.Z \
  --artifact-dir /path/to/lastdb-release-artifacts \
  --tap-dir /path/to/homebrew-lastdb \
  --source-git-oid <fold-commit> \
  --proof-report /tmp/forge-homebrew-promotion.json
```

Live stable promotion:

```bash
# Bottle CDN still needs GH_TOKEN. The formula PR uses the GitHub tap.
# If you supply --tap-dir, its origin must be EdgeVector/homebrew-lastdb on GitHub.
GH_TOKEN=<existing secure GitHub token> \
bash scripts/release/forge-promote-homebrew-stable.sh \
  --publish \
  --version-tag vX.Y.Z \
  --artifact-dir /tmp/lastdb-release-vX.Y.Z \
  --source-git-oid <fold-commit> \
  --proof-report /tmp/forge-homebrew-promotion.json
```

The live path creates the public `EdgeVector/homebrew-lastdb` release, bumps
both `Formula/lastdb.rb` and `Formula/folddb.rb` with the same surgical
`version` / `url` / `sha256` guard, opens a PR on the GitHub tap
and enables squash auto-merge on it. It refuses
prerelease tags; canaries still publish without moving the stable formula.

## When something fails

Look at the failing job in GitHub Actions for `EdgeVector/fold`.

For public release failures, `gh` is still useful against
`EdgeVector/homebrew-lastdb`:

```bash
gh release view vX.Y.Z --repo EdgeVector/homebrew-lastdb
```

Common failure modes and where they surface:

| Failure | Job | Symptom |
|---|---|---|
| Removed binary still listed in `release.yml` | `Release / build-*` | `error: no bin target named X` |
| `secrets.GH_PAT` expired | Any `Configure git for private dependencies` | `failed to authenticate when downloading repository` |
| Public release already exists | `Release / Create public EdgeVector/homebrew-lastdb release` | refuses to clobber; cut a new tag |

If a tag has already been pushed and the release fails:

- **Don't try to recover the orphan tag.** Bump the patch version and tag
  again. Orphan tags are cheap; chasing a broken release is expensive.
  See historical orphan tags on the archived predecessor desktop repo —
  both are orphans from failed release attempts that a later patch
  superseded cleanly.

## Rolling back a release

If a release ships and you need to retract it:

1. Mark the `EdgeVector/homebrew-lastdb` release as a prerelease or delete it.
2. Close the auto-bump PR on `homebrew-lastdb` if it hasn't merged yet.
   If it has merged, open a revert PR there.
3. Delete the tag: `git push origin :refs/tags/vX.Y.Z`. Note that anyone
   who already pulled the tag locally still has it.
4. Tag a fix forward (vX.Y.Z+1) rather than republishing vX.Y.Z under a
   different commit.

## macOS CLI signing (optional, not a desktop product)

Release CI may Developer ID–sign the **Mini CLI/daemon binaries** so
keychain ACLs survive `brew upgrade`. That is unrelated to any desktop
app or DMG pipeline (which do not exist on `main`). See the short pointer
in [`macos-code-signing.md`](macos-code-signing.md). Rotate Apple
Developer secrets only as needed for the Mini binary path; do not
reinstate Tauri updater or DMG secrets as part of shipping Mini.

## Auth setup

GitHub-side workflow auth uses `secrets.GH_PAT` (org-level Actions secret) for
the public `EdgeVector/homebrew-lastdb` release and formula bump PR. Do not
create private `EdgeVector/fold` release objects for Mini; the fold git tag is
the provenance record.
The predecessor org-deps token was retired 2026-04-30 after a silent expiration
broke every PR's CI; do not reintroduce it under that or any new name. See
`feedback_never_use_private_deps_token` in agent memory for the incident
details.

## Why brew downloads from a separate repo

`brew install edgevector/lastdb/lastdb` downloads tarballs from
`EdgeVector/homebrew-lastdb/releases/...`, not from
`EdgeVector/fold/releases/...`. The reason is hostnames + visibility, not
preference: `EdgeVector/fold` is private, and Homebrew
downloads anonymously. Pointing the formula at the private repo
broke `brew install` for every external user (incident: 2026-05-21
dogfood). The `release` job creates the one public tap release and uploads
`SHA256SUMS.txt` plus the tarball there; the formula `url` lines point there.

Never use `--clobber` to repair a published asset. If a release asset is wrong
or the formula sha does not match the live tarball, cut a new tag and let the
formula bump derive sha256 from the newly uploaded asset.

## Backfilling an older tag

Old tags that predate the public tap stored tarballs on a private repo or on the retired Forgejo forge.
Current releases publish directly on `EdgeVector/homebrew-lastdb`.
A historical tag that 404s needs a GitHub release upload from a saved tarball.
Do not download it from `http://localhost:3300`.
After the upload, bump `Formula/folddb.rb` on `EdgeVector/homebrew-lastdb` so the
`url` lines say `EdgeVector/homebrew-lastdb/releases/download/$TAG/...`.
The sha256 values stay the same when the tarball bytes stay the same.
