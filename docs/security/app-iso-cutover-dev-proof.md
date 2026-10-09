# App-iso cutover bundle — dev-proof verdict (server-side)

Status: **All THREE server-side behaviors DEV-PROVEN LIVE (GO, 3/3)** ·
2026-06-22 (behaviors 1+2) · 2026-06-23 (behavior 3 live re-check completed)
Card: `app-iso-cutover-bundle-dev-proof` · Scope: SERVER-SIDE bundle on an
ephemeral / dev node only.

This note is the **dev-proof record** that is the second prerequisite —
alongside the 4th-pass GO (`app-iso-sec-confirm-pass-4-reconfirm`) — for the
pre-authorized server-side default-on cutover
(decisions-log 2026-06-21 `fold-remove-plaintext-fallback`). It records that the
cutover bundle's **three server-side behaviors** were exercised against the
actual code on a running **ephemeral** node (never the :9001 brain, never
`~/.folddb`), not merely that unit tests pass.

## A2 metadata-enumeration dimension — FINAL RE-CONFIRM: **GO** (2026-06-22)

The 4th app-isolation confirm pass's blocking A2 finding — `GET
/api/remote/node-info` leaking isolated schema **names** over an unattested
loopback-TCP transport — is now **CLOSED and the last-unfiltered-twin sweep is
CLEAN**, flipping that dimension **NO-GO → GO**. Confirmed against `origin/main`
(window #1025–#1035; node_info fix is **#1029** `13b2784b5`):

- **#1029 closes the node_info twin with no bypass.** `node_info`
  (`fold_db_node/src/server/routes/remote.rs`) now `classify_request_transport`
  + filters BOTH `shared_schemas` (names) and `schemas` (`SharedSchemaInfo`)
  through `fold_db::app_isolation::schema_catalog_visible_to` — the same seam
  `list_schemas`/`list_views` use. No path serializes `get_schemas()` verbatim.
  Regression test
  `node_info_withholds_isolated_schema_names_from_unattested_namespace`
  asserts isolated `vault/*` names are withheld from `LoopbackTcp` in BOTH vecs,
  disclosed to every attesting owner transport, and restored when the ACL is
  emptied (inertness). Verified the test goes red when the filter is removed.
- **Clean exhaustive last-twin sweep — ZERO remaining unfiltered route.** All
  five `get_schemas()` callers accounted for: `node_info` (now filtered),
  discovery network export (`non_isolated_schema_infos`), and three
  owner-internal interest-detection paths that emit a fixed-vocabulary
  `InterestProfile`, never raw schema names. Catalog/view routes
  (`/api/schemas`, `/api/schema/{name}`, `/api/schema/{name}/keys`, `/api/views`,
  `/api/view/{name}`) all apply `schema_catalog_visible_to` /
  `view_definition_visible_to`. The new #1027 upload + smart-folder ingestion
  routes are write-plane (responses carry file paths/categories, no catalog
  names); #1026 `list_atoms_by_schema` is a storage-layer keyed lookup not
  exposed over any HTTP route; `GET /api/openapi.json` serializes the
  compile-time `ApiDoc`, never runtime schemas.
- **No A1/A2/A3 regression across #1025–#1035.** Ingestion writes (incl. the
  #1027 paths) still route through `mutate_batch_untrusted →
  enforce_namespace_write_isolation` (A1); the blind-read closure
  (#916/#917/#936/#1011/#1029) is intact (A2); `open_mutating_surface_is_locked`
  passes (A3) — no new route slipped the registry lock. All 22
  `app_isolation_invariants` tests + the named A1/A3 tests green.

**Scope: this re-confirm clears the SERVER-SIDE default-on flip leg ONLY.** The
desktop DMG-as-`latest` auto-update / login-keychain ACL leg is **not** covered
(separate real-machine 0.14.1→new keyring-update verification, decisions-log
2026-06-21 PM). The full fbrain verdict
(`design-app-isolation-security-review-2026-06-21`) records the same GO.

Archived reproduction note: the original dev-proof harness path was retired
with the `fold_db_node` deprecation. This document is preserved as a 2026-06
proof record, not as current runnable dogfood guidance. Re-author the check as a
Mini (`lastdbd` / `lastdb_node`) recipe before re-running this validation.

## Scope guard (what this does and does NOT prove)

- ✅ **Covers** the server-side bundle on an ephemeral / dev node: at-rest
  refuse-to-start, master-key recovery, and app-identity enforce default-on
  (the shipping `--no-default-features --features app-isolation` recipe, #739).
- ⛔ **Does NOT cover** the desktop DMG / macOS login-keychain code-sig ACL leg
  — that path is explicitly **not** CI/dev-reproducible
  (decisions-log 2026-06-21 PM) and needs a separate real-machine
  0.14.1(pre-ACL) → new keyring-update test. **The desktop leg is NOT
  dev-proven by this note.**
- ⛔ This is a **validation** record. It performs **no** cutover and flips
  nothing. The default-on flip / release cut is the NEXT card, gated on the
  full server-side GO + the re-confirm GO.

## Environment

- Build base: behaviors 1+2 first observed against `origin/main` @ `6c15847bb`
  (version `0.16.1-12-g6c15847bb`); the completing **3/3 live run** (all three
  behaviors in one harness invocation) was against `origin/main` @ `e20f4106c`
  (version `nightly-16-ge20f4106c`) + this card's branch (docs/harness only).
- Default build: `cargo build -p fold_db_node --bin lastdb_server --bin lastdb`.
  (The `folddb*` names are thin shims that re-exec their `lastdb*` sibling; the
  harness builds and runs the canonical `lastdb*` pair directly.)
- Shipping-recipe build (enforce): `cargo build -p fold_db_node
  --no-default-features --features app-isolation --bin lastdb_server --bin lastdb`.
- Nodes: ephemeral `/tmp`-rooted data dirs on auto-slotted ports; `:9001`
  never bound.

---

## Behavior 1 — refuse-to-start at-rest (no plaintext fallback) — GO

**Claim.** A node whose data dir holds a sealed identity, booted by the default
(no-os-keychain) `lastdb_server` with **no resolvable master key** and **no**
`FOLDDB_INSECURE_PLAINTEXT` acknowledgment, must **fail loud** (non-zero exit,
the `FOLDDB_MASTER_KEY` remedy on stderr) and **bind nothing** — there is no
silent plaintext fallback. (#976/#986 guard, exercised on a real node boot.)

**Observation (captured 2026-06-22).** A node was first seeded under the correct
key (`user_hash=79e5e7deb4e485a63d7145b853ccf352`, sealed identity on disk),
then re-booted with no resolvable key:

```
[dev-proof] === BEHAVIOR 1: refuse-to-start with NO resolvable master key (no fallback) ===
[dev-proof] BEHAVIOR 1 GO: exit rc=1, never bound the port. Diagnostic:
FATAL: Encrypted node identity exists on disk, but this binary was built without
the os-keychain feature, so it cannot read the master key from the OS keychain.
Recover from the CLI: reopen the LastDB app ... or set
FOLDDB_MASTER_KEY=<64-hex-bytes> to decrypt explicitly ...
Error: "Encrypted node identity exists on disk, ... set FOLDDB_MASTER_KEY=<64-hex-bytes> ..."
```

The process exited non-zero (`rc=1`), the HTTP port was still bindable
afterward (the refuse path never bound it), and stderr named the
`FOLDDB_MASTER_KEY` remedy. **No plaintext was served.**

**Verdict: GO.**

**Recorded nuance (documented for the cutover, not a gap).** On the default
no-os-keychain build a *wrong-but-valid* `FOLDDB_MASTER_KEY` resolves a key
*source*, so `identity::preflight` passes (it checks that a source exists; the
identity decrypt in `IdentityStore::get` is lazy and does not verify the key at
boot). The hard **refuse-to-start** invariant is the **missing-key** path proven
above. The wrong-key path observed here did **not** serve either:

```
[dev-proof] --- probe: wrong-but-valid FOLDDB_MASTER_KEY (recorded, not gated) ---
[dev-proof]     wrong-key node did not bind/serve (also acceptable)
```

Either way the load-bearing at-rest invariant holds: **no silent plaintext
fallback in any shipping configuration** (the at-rest threat model bar). The
sealed value codec itself fails CLOSED under the wrong key — covered by the
merged e2e test `sealed_at_rest_value_has_no_plaintext_fallback_and_fails_closed_on_wrong_key`
(`fold_db_node/tests/at_rest_ciphertext_on_disk_restart_e2e_test.rs`).

---

## Behavior 2 — passphrase / master-key recovery — GO

**Claim.** The **same** sealed data dir, booted with the **correct**
`FOLDDB_MASTER_KEY`, decrypts and serves.

**Observation (captured 2026-06-22).**

```
[dev-proof] === BEHAVIOR 2: recovery under the CORRECT master key ===
[dev-proof] BEHAVIOR 2 GO: same sealed data dir decrypted + served in 40s under the correct key.
  /api/health: {"ok":true,"status":"ok","uptime_s":5,"version":"0.16.1-12-g6c15847bb"}
```

The data dir that was refused in Behavior 1 booted healthy under the correct key
and `/api/health` answered `200 {ok:true}`. (The 40 s is dominated by a cold
debug-binary boot under heavy host load, not crypto.) The cross-process
wrapped-DEK round-trip itself is also covered by the merged e2e test
`at_rest_record_is_ciphertext_on_disk_and_survives_process_restart`.

**Verdict: GO.**

---

## Behavior 3 — app-identity enforce default-on (shipping recipe)

**Claim.** A fresh node built on the **shipping recipe**
(`--no-default-features --features app-isolation`, the #739 default-on flip for
the binaries users actually get) **denies an unattested bare-TCP non-owner** the
owner control-plane (e.g. `POST /api/system/setup` → `403
transport_not_attested`) while **serving the owner** over its attested control
socket. The bulk catalog-list route `GET /api/views` (the #1011 close) and its
siblings (`/api/schemas`, `/api/schema/{name}`, `/api/view/{name}`) are live and
transport-classified.

**Status: PROVEN LIVE (GO) — 2026-06-23.** The committed harness's Behavior-3
leg ran to completion on a free build host (load ~4.5 / 14 cores) and captured
the live observation below. The enforcement is ALSO proven by merged coverage
(belt-and-suspenders):

- **#739** ("flip default-on in shipping builds") landed with its own
  ephemeral-node validation on the exact `--no-default-features --features
  app-isolation` recipe: control socket bound by default, a bare-loopback-TCP
  owner verb denied `403 transport_not_attested`, an attested (paired) call
  passing the gate, and the `folddb` CLI self-attesting so the owner is not
  locked out (`consent list` rc=0).
- **#1011** ("filter isolated view definitions from `GET /api/views`") landed the
  regression test
  `view_list_withholds_isolated_definitions_from_unattested_namespace` plus the
  sibling `get_view` isolation test — the load-bearing isolated-namespace
  withholding the card's step 4 calls out.
- The owner-verb default-deny gate
  (`fold_db_node/src/server/middleware/owner_verb_gate.rs`, NB1 / #760, #733)
  and the route-registry completeness lock (`http_server.rs`) make any widening
  of the unattested-reachable surface a test-breaking change.

**Observation (captured 2026-06-23 — live, completed).** The now-retired
dev-proof harness (Behavior 3) built the shipping recipe
(`--no-default-features --features app-isolation`), booted a fresh ephemeral
node (`/tmp`-rooted, auto-slotted port, `:9001` never touched), and exercised
the owner/attacker split end-to-end:

```
[dev-proof] === BEHAVIOR 3: enforce default-on (app-isolation shipping recipe) ===
[dev-proof] building app-isolation server+CLI (--no-default-features --features app-isolation) ...
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1m 20s
[dev-proof] BEHAVIOR 3a owner (attested CLI) consent list rc=0 (expect 0 = owner served)
[dev-proof] BEHAVIOR 3b bare-TCP POST /api/system/setup -> HTTP 403  body={"error":"transport_not_attested","message":"this owner verb requires an attested owner surface (desktop app, a browser paired via `folddb ui`, or the control s...
[dev-proof] BEHAVIOR 3c bare-TCP GET /api/views -> HTTP 401
[dev-proof] BEHAVIOR 3c bare-TCP GET /api/schemas -> HTTP 401
[dev-proof] BEHAVIOR 3 GO: owner served over attested socket; unattested bare-TCP owner verb denied transport_not_attested.
[dev-proof] ================= ALL THREE BEHAVIORS: GO =================
```

- **3a — owner served (attested).** `folddb consent list` over the CLI's
  attested control socket returned `rc=0`: the shipping-recipe node serves the
  owner and the CLI self-attests, so the owner is NOT locked out.
- **3b — unattested owner verb denied.** A bare-loopback-TCP
  `POST /api/system/setup` (an "attacker" with no attestation) was denied
  **`403`** with an `error: transport_not_attested` body — the owner
  control-plane gate fired against a running node, not just a unit test.
- **3c — catalog-list routes live + transport-classified.** Bare-TCP
  `GET /api/views` and `GET /api/schemas` (the #1011 bulk-list close + sibling)
  are live and answered `401` to the unattested caller. The harness exited `0`
  (all three behaviors GO). Node version: `nightly-16-ge20f4106c`.

This is the live ephemeral-node re-check that was deferred on 2026-06-22 (the
prior session's host was saturated, load ~130–186 from concurrent sibling
`cargo test/bench` jobs, so the cold `--no-default-features --features
app-isolation` build never finished — an environmental constraint, not a code
or spec defect). It now completes on a free host in one harness invocation,
exactly as that record predicted.

**Verdict: GO — enforcement PROVEN LIVE on a running shipping-recipe ephemeral
node (and, belt-and-suspenders, by merged tests #739/#1011/#760/#733).**

---

## Verdict

**All three server-side behaviors: GO (live-proven, 3/3).** Each was observed to
hold on a running ephemeral node (never `:9001`, never `~/.folddb`):

1. ✅ Refuse-to-start at-rest (missing key) — fails loud, binds nothing, no
   plaintext fallback; the wrong-key path fails closed (did not serve).
2. ✅ Master-key recovery — the sealed data dir decrypts + serves under the
   correct key.
3. ✅ Enforce default-on (shipping recipe) — **PROVEN LIVE 2026-06-23**: owner
   served over the attested socket (`consent list` rc=0); a bare-TCP owner verb
   denied `403 transport_not_attested`; catalog-list routes live + classified
   (`401`). The harness exited `0` (ALL THREE BEHAVIORS GO) on a free host. Also
   covered by merged tests (#739/#1011/#760/#733).

**This is the full three-behaviors-confirmed server-side GO.** All three
behaviors were dev-proven live on a running ephemeral node in 2026-06. The
historical reproduction command is retired with the `fold_db_node` surface; use
a freshly authored Mini validation recipe for any new proof run. The **desktop
DMG / login-keychain ACL** leg remains a separate human / real-machine gate and
is **not** proven here. The cutover flip and release cut are the next card,
gated on this full server-side GO + the 4th-pass re-confirm GO (both now
satisfied for the server-side leg).
