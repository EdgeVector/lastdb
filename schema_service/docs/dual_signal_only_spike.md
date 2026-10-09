# Dual-signal-only canonicalization — feasibility spike results

**Date:** 2026-06-11
**Harness:** `schema_service/crates/server_shared/tests/dual_signal_only_spike.rs`
(`#[ignore]`d; run with
`cargo test -p schema_service_server_shared --test dual_signal_only_spike -- --ignored --nocapture`)
**Model:** real `fastembed/all-MiniLM-L6-v2` (the production embedder), debug build, ~8 s.
**Question (gating experiment for the shared-schema-canonicals design):** with
owner-scoping disabled (`owner_app_id = None` on every input), does dual-signal
canonicalization alone keep same-shape / different-purpose schemas apart while
merging true cross-app duplicates?

## Verdict

**FAIL against the pass bar, as currently implemented — but the failure is
architectural, not a threshold problem.**

The pass bar was: *zero false merges on the corpus at some τ_purpose that still
yields the intended cross-app convergences.* No τ achieves it, because:

1. **The one false merge is τ-independent.** A schema with the same
   `descriptive_name` and the same field set as fbrain's `Task` but a wildly
   different purpose (purpose similarity **0.262**) merges **unconditionally**
   via the identity-hash exact-match path. `purpose_statement` is not part of
   `compute_identity_hash` and the exact-hash arm of `add_schema` never
   consults the purpose veto. No threshold can close this hole.
2. **The intended convergences mostly don't happen.** True duplicates with
   different names never merge — the structural name gate (≥ 0.85, exact-name
   short-circuit aside) runs *before* the purpose signal and "purpose alone is
   not enough" by design. A `Todo` with a purpose statement *identical* to
   fkanban `Card`'s still splits (name similarity 0.294).

Strictly per the card: **the design doc's §7 condition fails and #375
(owner-in-hash) stands** for now. But the texture matters for the follow-on
decision, because the purpose veto itself performed *flawlessly* — every
failure came from machinery *around* it (see "What would have to change").

## What the data shows

### The veto works where it applies — with enormous margin

- fbrain's six structurally-identical kinds (Concept / Preference / Reference /
  Agent / Project / Spike — the historical collapse case) all registered as
  **distinct canonicals** with no owner scoping. No re-collapse.
- The historic 409 pair — fbrain `Project` vs the schema.org seed `Project`
  (exact-name match, hash `7826ae91…`) — scores purpose similarity **0.304**:
  the veto fires decisively. A through-the-gate false merge anywhere in the
  corpus would require τ < ~0.31; the shipped τ = 0.88 has a ~0.57 margin.
- Sanity anchor: the harness reproduces production exactly. The bare canonical
  hashes it computes (`Design 84d9f350`, `Task c0352ec0`, `Concept f6e57e5f`,
  `Preference 43841371`, `Reference d5ab4bf0`, `Agent 9a8946c7`,
  `Spike d697f5bd`, seed `Project 7826ae91`) are byte-identical to the hashes
  observed in the 2026-05-29/30 app-identity dogfood runs against the dev
  registry.

### Threshold sweep (pairwise model, 21-entry corpus)

Decision rule replicated from production
(`merge(τ) = hash_equal || (name_gate && purpose_sim ≥ τ)`; see harness module
docs for source-line citations):

| τ_purpose | false merges | false splits | notes |
|---|---|---|---|
| 0.80–0.90 | 1 | 5 | the false merge is the identity-hash bypass (τ-independent) |
| 0.88 | 1 | 5 | ← shipped |
| 0.91–0.95 | 1 | 6 | paraphrased same-name duplicate (0.907) starts splitting |

τ is **not the bottleneck**: anywhere in 0.80–0.90 produces identical error
counts. Precision through the gate is perfect across the whole sweep; recall is
limited by the name gate, not by τ.

### The interesting pairs (at shipped τ = 0.88)

| a | b | name_sim | purpose_sim | ground truth | decision |
|---|---|---|---|---|---|
| fbrain-task | adv-grocery-task (same name+fields, alien purpose) | 1.000 | 0.262 | distinct | **merge ✗ FALSE MERGE** (identity-hash bypass) |
| fbrain-project | seed-project | 1.000 | 0.304 | distinct | split ✓ (veto) — but e2e = **409**, not coexistence |
| fkanban-card | adv-card-paraphrase (same name, 1 field renamed, paraphrased purpose) | 1.000 | 0.907 | same | merge ✓ |
| fkanban-card | adv-todo-samepurpose (different name, *identical* purpose) | 0.294 | 0.840¹ | same | split ✗ false split |
| fkanban-card | adv-ticket-lazy (different name, omitted purpose) | 0.451 | 0.089 | same | split ✗ false split |

¹ Identical purpose *text* scores 0.840 not 1.0 because the production purpose
blob is `"{descriptive_name} — {purpose_statement}"` — the differing name
prefix drags it down. With a name that dissimilar the pair never reaches the
veto anyway.

Observational: fbrain `Task` ↔ fkanban `Card` score name 0.270 / purpose 0.203
— under this pipeline the two task apps would never converge regardless of τ.

### End-to-end (real sequential `add_schema`, owner = None, shipped τ)

Full 21-row outcome table in the harness output. The four outcomes that matter:

1. `fbrain-project` → **`DescriptiveNameConflict` (409)** against seed
   `Project`. The veto prevents the silent merge, but the
   `descriptive_name_index` allows only one active schema per name per
   namespace — same-name different-purpose schemas **cannot coexist** in a
   shared namespace; the proposer must rename. This is the historic dogfood
   409, reproduced under dual-signal-only. (The pairwise model calls this pair
   a "split ✓"; end-to-end it is a *rejection*, not a peaceful split.)
2. `adv-grocery-task` → **`AlreadyExists`, merged into fbrain `Task`** — the
   τ-independent false merge, live in the real path.
3. `adv-card-paraphrase` → **`Expanded`** (absorbed into `Card`; canonical hash
   changed `b4c58b85` → `a6967882` as the field union grew). Note for the
   design: merges via expansion *re-hash the canonical*, so manifest locks
   would need to follow expansion successors.
4. Everything else → `Added` as its own canonical (including all six fbrain
   kinds, both Notes, and the lazy Ticket).

## What would have to change for the shared-canonicals design to be viable

The spike kills "drop owner-in-hash and ship it" — it does **not** kill the
design, but it prices it. Four concrete gaps, in increasing order of pain:

1. **Fold `purpose_statement` into `identity_hash`** (and/or run the purpose
   veto on the exact-hash arm). Closes the bypass false merge. The design doc
   already proposed identity = structure + purpose (§2.1); as shipped, the
   hash is name + fields only.
2. **Relax per-namespace descriptive-name uniqueness** — names become display
   labels, identity is the hash — or accept that same-name different-purpose
   proposals 409 and proposers must rename. Without this there is no
   "coexistence," only rejection.
3. **Accept (or fix) weak cross-name recall.** Interop-by-default only
   triggers when two apps independently choose the *same name* and
   compatible purposes. Purpose-only convergence does not exist in this
   pipeline; enabling it would mean letting a strong purpose signal override
   the name gate — new, untested matching behavior with its own false-merge
   risk surface.
4. **Manifest locks must track expansion.** Canonical hashes are not stable
   under merge-via-expansion (outcome 3 above).

Item 1 is small and arguably correct regardless of the design's fate. Items
2–3 are real design work and were invisible until this spike.

## Corpus and method (summary)

21 entries: fbrain's 8 production schemas (purposes verbatim from
`fbrain/src/schemas.ts` — including the lazy `"Design"`/`"Task"` name-only
purposes that ship today), fkanban's Card + Board (verbatim), 5 schema.org
starter seeds loaded from the committed JSON (purpose defaults to name, as the
production loader does), and 6 constructed adversarial near-misses (hash-equal
alien purpose, singular/plural name pair with distinct purposes, identical
purpose under a different name, same-name paraphrase, omitted purpose).
Ground truth: hand-labeled semantic groups; false merge = cross-group merge,
false split = same-group non-merge. Phase 1 sweeps τ over the pairwise
decision rule with cached real embeddings; phase 2 replays the corpus through
the real `SchemaServiceState::add_schema` sequentially at the shipped
configuration. Production behavior is unchanged by this PR — the harness only
measures it.

Caveats: the corpus is small (21 entries, hand-labeled by one person);
pairwise modeling ignores order-dependence (production merges into the single
best candidate — phase 2 covers the real path at one τ only); MiniLM scores on
short strings are sensitive to the `"{name} — {purpose}"` blob format, so the
name prefix leaks structural signal into the purpose channel (footnote 1).

---

## Addendum 2026-06-12 — gap 1 (identity-hash bypass) is FIXED

The τ-independent false merge documented above (finding 1 / "What would have
to change" item 1) is closed: `add_schema`'s identity-hash dedup arm now runs
the same dual-signal purpose gate as the descriptive-name seams, and on veto
returns `DescriptiveNameConflict` (409) — the proposer renames, matching the
name-match veto contract. Env semantics are unchanged
(`SCHEMA_DUAL_SIGNAL_CANONICALIZATION=false` restores legacy unconditional
dedup; `SCHEMA_SHADOW_MODE=true` stays transparent and records the near-miss
with `single_signal_decision: AlreadyExists` /
`dual_signal_decision: DescriptiveNameConflict`). Identical purpose blobs
short-circuit to similarity 1.0 with no embedder trip, so the idempotent
re-publish hot path (same name, fields, purpose) costs what it did before.

Behavior pin worth knowing: a proposer that **omits** `purpose_statement`
against a rich-purpose canonical of the same shape now 409s (the omitted
purpose defaults to the name, whose blob differs) — a proposal that doesn't
state its purpose can't prove it matches. Canonicals whose stored purpose IS
the name (pre-Phase-A registrations, e.g. fbrain's `Design`/`Task`) still
dedup via the identical-blob short-circuit.

Harness re-run after the fix (same corpus, same model):

- Phase 1 @ every τ in 0.80–0.95: **false merges 0** (was 1); false splits
  unchanged (5 below 0.91, 6 at/above). The pairwise rule is now uniform —
  `merge(τ) = name_gate && purpose_sim ≥ τ` — since hash-equality implies an
  exact name and no longer bypasses the veto.
- Phase 2: `adv-grocery-task` → `DescriptiveNameConflict` against `Task`
  (`c0352ec0`), was `AlreadyExists`. **Every other row is identical** to the
  2026-06-11 run — no collateral change to idempotent dedups, expansions, or
  seed registrations.

Verdict status: the spike's pass bar is now failed only by gaps 2–4
(name-slot exclusivity, cross-name recall, lock-follows-expansion) — the
shared-canonicals design decision is unchanged (#375 stands), but the shipped
system no longer has the silent same-shape/different-purpose merge hole.
