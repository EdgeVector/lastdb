# app-starter — build an app on lastdb

A turnkey starting point for building an app on LastDB. Some package and
command names still use `folddb` for compatibility; use them exactly as shown
in the examples. Clone, follow this README, and have an app reading and
writing its **own namespace** on a lastdb node within an afternoon — no tribal
knowledge required.

The app itself is ~40 lines ([`src/index.ts`](src/index.ts)): the whole
lifecycle is **connect → request consent → mutate → query**, using the existing
runtime SDK [`@lastdb/app-sdk`](../../lastdb_app_sdk/). This
directory adds the packaging that's otherwise missing: a one-command run, an
example schema, and a scripted end-to-end run against an ephemeral node.

```
examples/app-starter/
├── src/index.ts            # the app: connect -> consent -> mutate -> query
├── schemas/Note.schema.json # the app-starter/Note schema you register
├── e2e/roundtrip.mjs       # scripted run against an ephemeral node
├── package.json            # path-deps @lastdb/app-sdk; `npm run dev` / `e2e`
└── tsconfig.json
```

It path-deps `@lastdb/app-sdk` from the in-repo SDK
(`../../lastdb_app_sdk`), so the starter keeps building as the SDK
churns — no vendored snapshot to drift.

---

## TL;DR (the whole loop, one command)

From this directory, with `cargo`, `node` (≥18), and `npm` on PATH:

```bash
npm install            # path-installs @lastdb/app-sdk
npm run e2e            # builds lastdbd (lastdb_node Mini) from main, boots an ephemeral
                       # socket-only node, declares the schema, runs the app,
                       # and asserts the round-trip. ~5-10 min first run.
```

`npm run e2e` is the reproducible "does it actually work" proof: it never
touches your real node or port 9001. It pins `LASTDB_HOME`, `FOLDDB_HOME`, and
the SDK capability store to a temp directory, starts a throwaway socket-only
`lastdbd`, declares `app-starter/Note`, runs the app against that socket,
and tears everything down. Expected tail:

```
[e2e] RUN PASSED: the starter created a row and read it back through the SDK.
[e2e] DONE — e2e passed.
```

The four manual steps below are the same flow, broken out so you understand each
piece and can point the app at your own node.

---

## The one real sharp edge: your node must serve `/api/*`

The SDK speaks a node's **`/api/*` data path**. The default local node is
socket-first: it serves `/api/*` over `<LASTDB_HOME>/data/folddb.sock`, while
production/shared nodes may also expose a TCP HTTP surface for owner-approved
app consent. You need either:

- a local/test **`lastdbd`** (Mini) owner socket, or
- a production/shared node HTTP base URL (when one is exposed).

Build the Mini daemon from the repo root:

```bash
cd ../..                          # fold workspace root
cargo build -p lastdb_node --bin lastdbd
```

`npm run e2e` builds this binary for you.

---

## Step 1 — get a node serving `/api/*`

Pick the transport that matches your node. **They differ — read this.**

### Option A: a local/test node — Unix socket

For a local node, connect with the node's Unix socket. The e2e creates a
throwaway `LASTDB_HOME` and uses `<LASTDB_HOME>/data/folddb.sock`; for manual
testing, point the app at the socket for the node you started.

```bash
# from the fold workspace root, with the binary you built above:
export LASTDB_HOME="$(mktemp -d)"
export FOLDDB_HOME="$LASTDB_HOME"
target/debug/lastdbd --data-dir "$LASTDB_HOME"
# (or target/release/lastdbd if you built --release)
```

Point the app at that socket:

```bash
export APP_STARTER_SOCKET_PATH="$LASTDB_HOME/data/folddb.sock"
# Owner hash: lastdbd self-inits identity at boot. `npm run e2e` reads it from
# GET /api/system/auto-identity and exports APP_STARTER_USER_HASH.
```

Socket-local runs are owner-local and skip the production consent flow.

### Option B: a production/shared node — TCP `/api/*` + consent

When a node exposes `/api/*` over TCP HTTP with the consent flow, point the
app at its base URL:

```bash
export APP_STARTER_BASE_URL=http://127.0.0.1:9001   # your node's HTTP port
```

(Default is `http://127.0.0.1:9001` if unset.)

---

## Step 2 — wire the SDK

The SDK is path-deped from the monorepo, so `npm install` links it. Because the
SDK ships TypeScript that compiles to `dist/`, build it once (it's gitignored):

```bash
npm install
( cd ../../lastdb_app_sdk && npm install && npm run build )  # -> dist/
```

In code it's a plain import (already done in `src/index.ts`):

```ts
import { connect } from '@lastdb/app-sdk';

const fold = await connect({ socketPath, appId: 'app-starter' });   // local socket node
// or: await connect({ baseUrl, appId: 'app-starter' });            // production
```

See [the SDK README](../../lastdb_app_sdk/README.md) for the full
API (pagination, scoped `search`, the typed error taxonomy, capability storage).

> **Which socket? Owner vs app.** This starter uses **`connect`** — the
> **app-socket** path for an app you *publish* for other people: it runs as an
> attributed app on the consumer's node, reading any consented schema but
> writing **only its own `app-starter/*` namespace**, gated by a one-time
> consent grant. An app you *develop and trust yourself* (your own tooling)
> instead uses **`ownerClient`** — the **owner-socket** path, which runs as
> NodeOwner with full read/write and no consent flow. Pick by *who runs the
> app*. Full comparison + code sketches:
> [Choosing a socket](../../lastdb_app_sdk/README.md#choosing-a-socket-your-own-apps-vs-published-apps).

---

## Step 3 — enroll as a developer + publish the app namespace and schemas

Your app needs a namespace (`app-starter/*`) and its schemas registered so the
node will isolate and serve them.

### 3a. On a local/test node — declare the schema

```bash
# The e2e posts this over the owner Unix socket:
POST /api/schemas/declare
{
  "namespace": "app-starter",
  "schema": <schemas/Note.schema.json>
}
```

That loads `app-starter/Note` directly into the throwaway node. The scripted e2e
does this for you before it runs the app.

### 3b. For a real / shared node — enroll as a developer and publish

Materialize your developer keypair and publish your app + schemas to the
registry via the `folddb` CLI:

```bash
folddb login            # authenticate to the registry
folddb init             # materialize your local developer keypair
folddb push             # publish the app namespace + its schemas
```

Developer enrollment may require a dev API key before `folddb push` can
publish. Use the current app-identity enrollment workflow if `folddb push`
errors with `not_a_developer`.

> The SDK is the **runtime data path**; `folddb` is the **publishing path**.

---

## Step 4 — owner grants consent (production node only)

On a production node the owner approves the app's request for `app-starter/*`.
On first run the app prints the exact command; the owner runs it in their own
terminal:

```bash
folddb consent grant app-starter
```

Once granted, the SDK stores the capability in the OS keychain (with a file
fallback) keyed by `(appId, node)`, so subsequent runs skip the prompt. A
socket-local test node skips this production consent step.

---

## Run the app

```bash
npm run dev    # tsc -> dist/, then node dist/index.js
```

Expected output (local socket node):

```
connected to unix:/path/to/<session>.sock as app 'app-starter'
mutate: accepted create for app-starter/Note
query: app-starter/Note returned 1 row(s):
  key=note-1 fields={"id":"note-1","text":"hello from app-starter @ ..."} author=...
round-trip complete: created a row and read it back through the SDK.
```

That's an app reading and writing its own namespace on lastdb. From here:
add fields and schemas, page large reads with `query({ limit, offset })` or
`queryAll`, run associative `search` — all documented in the
[SDK README](../../lastdb_app_sdk/README.md).

---

## Where to take it next

- **Update/delete a row:** pass the `keyValue` from a queried row back as
  `MutationOp.key` to address that exact row.
- **More schemas:** add JSON files under `schemas/`, register each, and the
  app's namespace covers them all.
- **Production:** publish via `folddb push` and let the owner grant consent —
  the same code path, just `baseUrl` instead of `socketPath`.
