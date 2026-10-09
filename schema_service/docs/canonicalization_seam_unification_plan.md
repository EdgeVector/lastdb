<!-- /autoplan restore point: /Users/example/.gstack/projects/EdgeVector-fold/canon-seam-unification-autoplan-restore-20260630-131055.md -->
# Canonicalization seam unification — collapse 4 sequential match seams into one ranked-candidate pipeline

**Status:** ✅ Reviewed via `/autoplan` (CEO + Eng, 2026-06-30). Implementation-ready
pending the design spike below. **Single-PR plan** — the original two-PR framing
(below, kept for the record) was wrong; see "CORRECTION" section.
**Author:** drafted with Claude during an architecture-survey conversation (no prior ticket).
**Scope:** `schema_service/crates/core/src/{state.rs, state_matching.rs, state_expansion.rs, state_compositional.rs}`.
**Supersedes nothing; complements** [`dual_signal_only_spike.md`](dual_signal_only_spike.md) and the dual-signal canonicalization design (fbrain `dual-signal-schema-canonicalization`).

## TL;DR — what to actually build (read this first)

One PR, gated on a design spike. Everything below this point is the review trail
that produced this scope — kept because it's evidence, not because it should be
re-read top to bottom.

**Step 0 — design spike (do first, before any implementation; time-box a few
hours, not days):**

**CORRECTION (2026-06-30, post-PR):** the original framing above ("re-invoked
under the write lock") mischaracterized today's actual concurrency pattern.
Verified by reading `state.rs:2437-2564` directly: there is **no long-held write
lock** spanning matching/gating anywhere today. It's optimistic concurrency —
every `self.schemas` touch is a short, scope-bounded `{ read_lock(...); ...; }`
or `{ write_lock(...); ...; }` block, acquired and dropped immediately (the
existing comment at `state.rs:2669` explains why: holding a guard across an
`.await` violates `Send`). The "race-condition re-check" is a **second full
pass** — re-read the index, re-run the same dual-signal check (a second
embedder round-trip), and only then take a fresh write lock for the insert.
It narrows the window between "decided safe" and "wrote it," it doesn't
eliminate it. The spike's real job is to reproduce this two-pass shape
*on purpose* in the unified pipeline, not to invent lock-holding that doesn't
exist today.

Concrete deliverable (half a page, written before any real implementation):

1. **Sequence diagram/pseudocode** of exactly when `generate_candidates → rank
   → gate` gets called: once as the speculative check, once again immediately
   before the write-locked insert as the race-recheck. Name both calls
   explicitly in the design — don't let the refactor accidentally collapse
   them into one call and silently widen the race window versus today.
2. **One explicit decision, stated in the design note:** is the second
   (race-recheck) call a literal repeat of the first — re-derive the full
   candidate set, including a second embedder round-trip, matching today's
   behavior exactly — or can it be cheaper (only re-validate that the
   previously-chosen winner is still the winner)? Either is acceptable, but
   the cheap version is a real behavior change versus today and must be
   called out as its own line item, not slipped in as an "optimization."
3. **A throwaway compile check, not a feature.** Write just the
   `MatchCandidate` enum and the gate function's signature, wire up one stub
   call site that awaits the embedder between two scoped lock blocks, and
   confirm `cargo check` is clean on `Send` before writing the real ~700
   lines. Catches a lock-across-await mistake in minutes instead of after
   the full implementation is written.

**Step 1 — implement, preserving all of today's behavior including the rescue
cascade:**
- `generate_candidates(incoming) -> Vec<MatchCandidate>` — side-effect-free. One
  pass over the four seam types (identity-hash, name-exact, name-semantic,
  field-overlap, purpose-reuse — model each as its own variant carrying its
  seam-specific preprocessing: namespace filtering, cross-schema_type guard,
  de-collision/system-schema exclusion for the purpose-reuse variant). Cross-
  schema_type 409s are a pre-candidate filter, not a gate outcome.
- `rank(candidates)` — by seam priority, then score within seam.
- Gate the winner **exactly once** via the existing `dual_signal_diagnostic`/
  `shadow_aware_dual_signal_check` machinery (reused verbatim, not rewritten).
- **On veto, do not just reject** — this is the part the original draft got
  wrong. A vetoed exact-name candidate must still get the same-name-reuse-rescue
  chance (today's seam D `allow_same_name` path), and if that also declines,
  de-collide into its own canonical (today's final-guard behavior). Model this
  as the gate returning one of three outcomes — `Merge`, `RescueRetry`,
  `DeCollideAndRegister` — instead of a boolean, so the cascade is explicit in
  the new code rather than reconstructed via fall-through.
- Apply renames/dedup (today's `semantic_field_rename_map` /
  `canonicalize_fields`) only on the winning candidate, after the gate decides,
  not during candidate generation.
- Add unit tests on `rank()` and the three-outcome gate directly (seam-priority
  ordering independent of raw score; cross-type filter fires pre-candidate; the
  de-collide path is independently testable) — the existing eval harness stays
  as the end-to-end regression gate but doesn't substitute for this.

**Step 2 — verify:**
- All 11+ existing dual-signal tests in
  `schema_service/crates/core/tests/dual_signal_canonicalization_test.rs` pass
  unmodified (they pin the exact behavior this refactor must preserve, including
  `exact_name_distinct_purpose_decollides_instead_of_409`).
- `schema_service/eval/run.mjs --stage1 --judge --baseline=<pre-refactor commit>`
  — identical scores on dup-explosion / reuse-rate / wrong-merges / field-fidelity.
  Confirm the corpus has a scenario exercising the rescue cascade specifically
  (the photo-dedup case from PR #1064 is a good candidate) before trusting a
  green run.
- Diff against the current `schema-decompose-apply-path` branch/PR for file-level
  overlap on `state.rs` before opening this PR.

**Net effect, corrected:** same matching power, same rescue cascade, ~750+ fewer
lines of duplicated gate/race-check/final-guard logic, and the cascade becomes an
explicit three-way return type instead of something reconstructed by reading
fall-through control flow.

## Why this exists

`add_schema_inner` (`state.rs:1623-2735`, ~1,112 lines) is the single entry point for
every schema registration on the platform. It tries four different ways of asking
"does this incoming schema match something that already exists?", **in sequence, with
fall-through on no-match**:

1. **Seam A — identity hash** (`state.rs:1940-2081`): exact `sha256(name+fields)` match.
2. **Seam B — descriptive-name match** (`state.rs:2085-2229`): exact name or embedding
   similarity ≥ `DESCRIPTIVE_NAME_SIMILARITY_THRESHOLD` (0.8).
3. **Seam C — field-overlap fallback** (`state.rs:2232-2364`): Jaccard ≥ 0.6 AND
   name-similarity ≥ 0.8.
4. **Seam D — reuse-before-new** (`state.rs:2366-2425` + `state_matching.rs:566-833`):
   a looser last-chance match — struct-sim ≥ `REUSE_STRUCT_FLOOR` (0.55) AND
   purpose-sim ≥ `REUSE_PURPOSE_THRESHOLD` (0.72) AND field-coverage ≥
   `REUSE_FIELD_FIDELITY_FLOOR` (0.5).

Each of seams A/B/C independently calls the same dual-signal "purpose veto" gate
(`shadow_aware_dual_signal_check`, gate threshold `PURPOSE_SIMILARITY_THRESHOLD` 0.88)
before committing to a merge — duplicated three times. A **race-condition re-check**
(`state.rs:2437-2564`) and a **final duplicate guard** (`state.rs:2566-2648`) then
re-run large chunks of the same matching logic again at the end. The purpose-embedding
similarity for a given candidate pair is computed twice in some paths: once inside the
gate (`state_matching.rs:486-494`), again inside seam D's matcher
(`state_matching.rs:743-753`).

This produces a real correctness hazard, not just a readability problem: a
purpose-veto inside seam C **falls through** to seam D instead of returning
(`state.rs:2310-2319`, comment references card `schema-canon-exact-name-veto-409`),
so the same incoming schema can be evaluated against a *different combination of
thresholds* depending on which earlier seam almost-but-didn't match it. Which
thresholds apply to a given schema today is a function of **how many seams it passed
through**, not of the schema's actual signal scores — that's emergent behavior from
sequential fall-through, not a designed property.

Total subsystem size: **~5,500 lines** across the 5 files, of which **~1,500 lines is
decision logic** (the seams + gates + race-check + final guard), gated by **7
independently-tunable similarity thresholds** and **5 env-var flags**
(`SCHEMA_DUAL_SIGNAL_CANONICALIZATION`, `SCHEMA_SHADOW_MODE`,
`SCHEMA_REUSE_BEFORE_NEW`, `SCHEMA_COMPOSITIONAL_DECOMPOSITION`,
`SCHEMA_COMPOSITIONAL_DECOMPOSITION=apply`).

## Proposed mechanism

Replace the sequential try-A-then-B-then-C-then-D control flow with a
**candidate-generation + single-gate** pipeline:

```
generate_candidates(incoming) -> Vec<MatchCandidate>
  // One pass, no early returns, no gate calls. Each candidate is tagged with
  // which seam found it (identity_hash | name_exact | name_semantic |
  // field_overlap | purpose_reuse) and its raw signal scores
  // (struct_sim, purpose_sim, field_coverage, jaccard — whichever apply).

best = rank(candidates)  // by seam priority, then by score within seam

if let Some(candidate) = best {
    verdict = dual_signal_gate(candidate)   // exactly once, regardless of which seam found it
    if verdict.merge { expand_schema(incoming, candidate.target) }
    else              { register_as_new(incoming) }
} else {
    register_as_new(incoming)
}
```

This:
- Deletes the three duplicated `shadow_aware_dual_signal_check` call sites — one gate
  call, period.
- Deletes the duplicated purpose-embedding computation between the gate and seam D's
  matcher (compute once, store on the candidate).
- Deletes the race-condition re-check and final duplicate guard as *separate* code
  paths — they become the same candidate-generation + single-gate call, re-invoked
  under the write lock instead of being a hand-maintained parallel copy of the seam
  logic.
- Fixes the order-dependent veto bug: which thresholds apply to a schema is now a
  property of *which candidate it matched*, not *how many seams it fell through*.
- Leaves `state_compositional.rs` untouched — it's correctly isolated already (reuses
  `dual_signal_diagnostic` verbatim, no persisted state until apply-mode rewrites
  `ref_fields`), and is out of scope for this refactor.

Net effect: same matching power (same 4 ways of finding a match, same thresholds),
roughly half the decision-logic LOC, one gate instead of three-plus-a-race-check-plus-
a-final-guard, and a real bug class removed rather than worked around.

## What already exists / what this builds on

- The matching primitives themselves (`find_matching_descriptive_name`,
  Jaccard computation, `find_purpose_reuse_target_allow_same_name`,
  `dual_signal_diagnostic`) are sound and stay as-is — only the orchestration around
  them changes.
- `schema_service/eval/` (the dup-explosion / reuse-rate eval harness, hourly via
  launchd `com.tomtang.schema-eval-routine`) already exists and can be run
  before/after this change to confirm zero behavioral regression on real corpus data.
  This is the primary verification mechanism — not new tests, an existing eval.
- The compositional decomposition track (`schema-decompose-apply-path` card,
  fbrain `schema-service-evolution-decomposition-track`) is unrelated and unaffected.

## NOT in scope

- Tuning or consolidating the 7 similarity thresholds themselves — that's a separate,
  independently-decidable simplification (a `Thresholds` struct/registry instead of
  scattered consts + env overrides). Flagged as a follow-on, not bundled here, because
  it changes *behavior tuning* surface, whereas this refactor changes *only*
  orchestration with identical seam definitions and identical thresholds.
- The compositional apply path (`schema-decompose-apply-path`) — separate active card,
  separate owner, unrelated mechanism.
- Any change to the actual similarity thresholds' values — this refactor must be
  behavior-preserving; the eval harness baseline is the regression gate.

## Risk

This is the hot path for every schema write on the platform (dev + prod Lambda). The
mitigation is the existing eval harness: run `schema_service/eval/run.mjs --stage1
--judge --baseline=<pre-refactor commit>` before and after, and require an
identical (or only-improved, given the order-dependency bug fix) score on
dup-explosion / reuse-rate / wrong-merges / field-fidelity metrics before merge.

## CEO Review Findings (Phase 1 — autoplan, 2026-06-30)

**Voices:** Claude subagent only. Codex CLI errored on this machine
(`service_tier` config value invalid) before producing output — tagged
`[subagent-only]`; flagged separately as a papercut (fkanban task spawned),
out of scope for this plan.

The subagent raised five findings. Auto-decided per the 6 principles below;
two are flagged as open taste/premise questions for the approval gate rather
than silently auto-decided, because they bear on whether the "bug" framing
itself is correct.

| # | Finding | Severity | Decision | Principle | Rationale |
|---|---|---|---|---|---|
| 1 | The plan jumps to "unify the plumbing" without first measuring, on the real eval corpus, how often seams C/D actually fire or diverge from A/B — a diagnostic-first pass could reveal a seam is dead weight rather than needing unification. | High | **Flagged for gate** (not auto-decided) | — | This is a genuine alternative direction (measure-then-decide vs. refactor-now), not a mechanical call — surfaced to the user below. |
| 2a | The order-dependent veto fall-through (`state.rs:2310-2319`) cites card `schema-canon-exact-name-veto-409` in its comment — evidence it may be an intentional patch for a real incident, not accidental drift. Treating it purely as "the bug" could be wrong. | High | **Flagged for gate** (not auto-decided) — added as a pre-implementation verification step | — | Materially affects whether the refactor's stated bug-fix framing is correct; resolving it doesn't require re-opening the premise gate, just verifying before implementation starts. |
| 2b/3 | No evidence the eval corpus actually exercises the seam-C-veto→seam-D-fallback path; if it doesn't, "green eval" after the refactor doesn't prove behavior-preservation on the one path that matters most. | Critical (if unmitigated) | **Accepted — added as a required pre-merge gate** | P1 (completeness) | A refactor on the platform's hottest write path needs its riskiest path actually covered by the regression gate it relies on, not just "the harness ran clean." |
| 4 | Bundling the known correctness bug fix with the full 1,500-line orchestration rewrite in one PR makes a clean revert impossible if the eval blind spot bites later. | Medium | **Accepted — resequenced into two PRs** | P6 (bias toward action, smallest safe unit) + P5 (explicit) | Shipping the narrow bug fix first, watching it for a few days, then following with the larger refactor is strictly lower-risk than one big PR, at near-zero extra cost. |
| 5 | No check yet that the compositional-decomposition track (`schema-decompose-apply-path`, also touching `state.rs`) doesn't conflict file-level with this rewrite. | Medium | **Accepted — added as a pre-flight step** | P3 (pragmatic) | Cheap to check before starting; expensive to discover via merge conflict after 1,000+ lines are rewritten. |

**Resulting resequencing (supersedes the single-PR framing above):**

- **PR 1 (ship first, narrow):** Fix the order-dependent fall-through at
  `state.rs:2310-2319` only — make seam C's purpose-veto return
  `DescriptiveNameConflict` instead of silently falling through to seam D.
  *Before writing this PR:* pull the history on card
  `schema-canon-exact-name-veto-409` (`fkanban show` / git blame /
  PR search) to confirm this fall-through is in fact unintentional drift and
  not a deliberate exception for a real prior incident. If it turns out to be
  intentional, this whole plan's bug-fix premise needs re-litigating before
  PR 1, not after.
- **PR 2 (follow-up, the seam-unification refactor):** The
  candidate-generation + single-gate redesign described above. Pre-flight:
  (a) confirm via `schema_service/eval/` corpus inspection that a scenario
  exists exercising the seam-C-veto→seam-D-fallback path — add one if not,
  before treating the harness as the regression gate; (b) diff against the
  current state of the `schema-decompose-apply-path` branch/PR for file-level
  overlap on `state.rs` before starting.

**Open question for the approval gate:** whether to do a short (~1 day)
diagnostic pass against the eval corpus — measuring actual seam hit-rates
on real data — before committing to PR 2's scope, in case it reveals a seam
is rarely/never hit and should be deleted rather than unified. This wasn't
auto-decided because it changes what PR 2 actually does, not just how
carefully it's executed.

## CORRECTION (2026-06-30, post-gate verification) — the "bug" premise was wrong

Per the CEO review's finding 2a (below), verified `schema-canon-exact-name-veto-409`'s
history via `git log -S` + `gh pr view`: it is
[PR #1064](https://github.com/EdgeVector/fold/pull/1064), merged 2026-06-24 (6 days
before this plan was drafted), titled "eliminate exact-name + purpose-veto 409
dead-end." The fall-through from seam C's purpose-veto to seam D is a **deliberate,
eval-verified, regression-tested rescue mechanism** — not accidental drift:

1. An exact-name match the strict dual-signal gate vetoes gets one more chance via
   the looser reuse-before-NEW gate (same-name reuse rescue).
2. If that ALSO declines, the schema de-collides into its own canonical
   (`"<name> (<Type>)"`) instead of hard-409ing.

This is pinned by `exact_name_distinct_purpose_decollides_instead_of_409` and
`identity_hash_distinct_purpose_conflicts_when_flag_on` in
`schema_service/crates/core/tests/dual_signal_canonicalization_test.rs`, and was
verified against the real-embedder eval harness before merge (errors=1→0 on the
photo-dedup scenario).

**This invalidates the original "PR1: fix the bug" framing below.** There is no
bug to fix — "fix the fall-through" would *regress* a 6-day-old, tested, intentional
behavior. **PR1 is cancelled.** The remaining valid work is the orchestration
unification alone (originally "PR2"), and it must now explicitly preserve the
same-name-reuse-rescue → de-collide-instead-of-409 cascade as a *designed* outcome
of the candidate-ranking/gate model — not eliminate it. The sections below are kept
for the record (they show how a single-voice review finding, taken seriously instead
of dismissed, caught a wrong premise before any code was written) but their "PR1"
references are superseded.

## Eng Review Findings (Phase 3 — autoplan, 2026-06-30)

**Voices:** Claude subagent only (Codex still unavailable — see CEO section).
Grounded in `state.rs:1623-2735` and `state_matching.rs`, not just this
plan's prose.

| # | Finding | Severity | Decision | Principle | Rationale |
|---|---|---|---|---|---|
| 1 | "Candidates" aren't symmetric `(target, score)` tuples — seam B/D mutate `schema.descriptive_name` and run field-rename mapping *during* matching, not after. A literal pure `generate_candidates` would lose this or duplicate it per-candidate. | High | **Accepted — design corrected** | P5 (explicit over clever) | `generate_candidates` must be side-effect-free; renames/dedup move into a post-gate "apply" step that runs once, on the winning candidate only. |
| 2 | Collapsing the race-condition re-check into "the same candidate+gate function under the write lock" is mechanically unspecified — the gate awaits the embedder, and you cannot hold a write lock across an `.await` (the existing code already avoids this at `state.rs:2669`). As stated this is a deadlock/Send-bound violation, not a design. | **Critical** | **Flagged for gate** — requires a short design spike before PR2 starts | — | This is a genuine open question about lock/async mechanics, not a mechanical fix — needs explicit resolution before implementation, not during. |
| 3 | "Run the eval harness" alone is insufficient verification for collapsing 3 duplicated gate sites + a hand-maintained race-check copy. Needs unit tests on the candidate-ranking function itself (seam-priority ordering independent of raw score, cross-type 409 as pre-filter not gate outcome, de-collision retry path). | High | **Accepted — added as required test additions** | P1 (completeness) | End-to-end eval scores can mask a seam-priority inversion that nets out to the same aggregate dup-rate; unit-level coverage on the ranking function catches what the corpus might not. |
| 4 | No security/trust-boundary issue — purely internal matching logic. Confirm the refactor doesn't relocate the `app:` prefix / separator-collision input validation (`state.rs:1640-1758`) to run after candidate generation instead of before. | None (confirm only) | **Accepted — added as an implementation constraint** | — | Validation must stay upstream of the seam pipeline; cheap to state explicitly, expensive to discover via a malformed-input regression. |
| 5 | Hidden complexity the pseudocode doesn't capture: namespace-aware filtering (`owner_app_id`) is seam-specific; cross-schema_type guards exist at 4 call sites firing at different points relative to the gate; the de-collision retry path (`is_system_schema`, decollision-suffix poisoning avoidance) is seam-D-specific stateful logic with no analog elsewhere. A single generic `MatchCandidate` struct risks losing seam-specific preprocessing. | High | **Accepted — design corrected** | P5 (explicit over clever) | Model candidates per-seam (an enum/variant carrying seam-specific preprocessing) rather than one generic struct; make cross-schema_type guards a uniform pre-candidate filter rather than folding them into the gate. |

**Resulting change to PR2 scope:** PR2 is now **gated on a short design spike**
resolving finding #2 (how candidate generation + the single gate interact
with the write lock given the embedder's async boundary) before
implementation starts. The candidate-generation design itself is corrected
per findings #1 and #5 (pure generation + post-gate apply step; per-seam
candidate modeling, not one generic struct). This is a real scope/timeline
change from the original plan, not a mechanical decision — surfaced below.

## Consensus Tables (degraded — Codex unavailable both phases)

```
CEO DUAL VOICES — CONSENSUS TABLE:
═══════════════════════════════════════════════════════════════
  Dimension                           Claude  Codex  Consensus
  ──────────────────────────────────── ─────── ─────── ─────────
  1. Premises valid?                   Doubt   N/A    N/A (single-voice, high-severity — flagged)
  2. Right problem to solve?           Yes*    N/A    N/A (*at corrected altitude — see finding 1)
  3. Scope calibration correct?        No      N/A    N/A (resequenced into 2 PRs)
  4. Alternatives sufficiently explored?No     N/A    N/A (measure-first alt surfaced)
  5. Internal-platform risk covered?   Partial N/A    N/A (compositional-track conflict check added)
  6. 6-month trajectory sound?         Doubt   N/A    N/A (eval-corpus coverage gap flagged)
═══════════════════════════════════════════════════════════════
Codex unavailable both phases (local CLI config error, unrelated to this repo).
Single-voice critical/high findings flagged regardless per autoplan's degradation rule.

ENG DUAL VOICES — CONSENSUS TABLE:
═══════════════════════════════════════════════════════════════
  Dimension                           Claude  Codex  Consensus
  ──────────────────────────────────── ─────── ─────── ─────────
  1. Architecture sound?               Partial N/A    N/A (candidate model needs correction)
  2. Test coverage sufficient?         No      N/A    N/A (unit tests on ranking fn required)
  3. Performance risks addressed?      N/A     N/A    Not evaluated this pass
  4. Security threats covered?         Yes     N/A    N/A (no issue found)
  5. Error paths handled?              Partial N/A    N/A (de-collision retry path under-modeled)
  6. Deployment risk manageable?       Critical N/A   N/A (lock/async mechanics unresolved — spike required)
═══════════════════════════════════════════════════════════════
```
