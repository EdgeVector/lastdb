# Publishing `@lastdb/app-sdk`

This package is **publish-ready but NOT yet published**. The irreversible public
`npm publish` is a human-gated milestone (the `sdk-publish-public` card). This
doc is the one-step release runbook + the checklist that gates it, so the actual
release is a single approved command.

> **Scope decided: `@lastdb`.** The package is named `@lastdb/app-sdk`,
> resolving the earlier `@folddb` vs `@lastdb` brand call in favor of the
> brand-forward name (fbrain `lastdb-brand-forward-public-naming`). As of the
> prep pass both `@folddb` and `@lastdb` scopes had **zero published packages**
> on the npm registry (`@folddb/app-sdk` and `@lastdb/app-sdk` both 404), so the
> `@lastdb` scope is claimable at publish time.

## The release command (gated — do NOT run without the unblock checklist below)

From `lastdb_app_sdk/`:

```bash
# 0. Be on a clean, merged-to-main checkout of the SDK.
git fetch origin main && git status   # clean, on main

# 1. Log in as the npm account that owns (or will create) the @lastdb scope.
npm whoami            # confirm the right account
# npm login           # if not already logged in

# 2. Prove the tarball one more time (no publish):
npm ci                # reproducible install from package-lock.json
npm run lint && npm run typecheck && npm test
npm publish --dry-run # review the file list; confirm dist/ + LICENSE + README only

# 3. PUBLISH (irreversible — this is the gated step):
npm publish --access public
#   `publishConfig.access: "public"` already forces public scope access,
#   so the flag is belt-and-suspenders. `prepublishOnly` runs
#   `clean && build`, so dist/ is always freshly compiled from the
#   committed source — never a stale local build.

# 4. Verify the published artifact:
npm view @lastdb/app-sdk version
npm pack @lastdb/app-sdk@<version>   # download + inspect the public tarball
```

`prepublishOnly` (`npm run clean && npm run build`) guarantees a from-source
`dist/` is what ships, so you cannot accidentally publish a stale or
hand-edited build.

## `sdk-publish-public` unblock checklist

Tick every box before running `npm publish`:

- [ ] **Scope name decided** (`@folddb` vs `@lastdb`) and `package.json` `name`
      updated to match. Re-check the chosen scope is unclaimed / owned by us:
      `npm view @<scope>/app-sdk` (404 = free).
- [ ] **npm account + scope ownership.** The publishing account owns the scope
      (or will create it on first publish). For an org scope, create the npm org
      and add the publisher as a member with publish rights.
- [ ] **2FA / automation token** in place for the account, per npm policy.
- [ ] **Version is correct.** First public release stays `0.1.0` (or bump per
      semver if the surface changed since this prep). `npm version` if bumping.
- [ ] **Tarball reviewed** — `npm publish --dry-run` shows ONLY `dist/`,
      `README.md`, `LICENSE`, `package.json`; no `src/`, `test/`, `e2e/`,
      `*.tsbuildinfo`, lockfile, or secrets. (Verified in the prep pass:
      35 files, dist + README + LICENSE + package.json only.)
- [ ] **CI green on the merge** that lands the final name/version.
- [ ] **Packaged-artifact round-trip re-proven** against a node serving `/api/*`
      (see `e2e/` and the prep PR) — install the tarball into a scratch app and
      confirm connect → consent → mutate → query round-trips.
- [ ] **README version requirement still accurate** (the "build from main / a
      release after v0.1.0" caveat in `README.md`).
- [ ] **License confirmed** — `LICENSE` (MIT) ships in the tarball.
- [ ] **Human approval recorded** (the `sdk-publish-public` card / decision log).

## What ships in the tarball

The `files` whitelist in `package.json` restricts the published tarball to:

- `dist/` — compiled JS, `.d.ts` types, and source maps (built by
  `prepublishOnly`).
- `README.md`
- `LICENSE` (MIT)
- `package.json`

Everything else — `src/`, `test/`, `e2e/`, `tsconfig.json`, lint config, the
lockfile — is excluded. Source maps are shipped (they degrade gracefully without
`src/`; they aid stack traces for consumers).

## Consuming the SDK before it's on public npm

Until the gated publish lands, consumers use one of:

- A **packed tarball**: `npm pack` here, then `npm install /path/to/folddb-app-sdk-0.1.0.tgz`.
- A **vendored copy** (what fbrain does today — switching fbrain off the vendored
  copy is explicitly out of scope for the prep card).
