# Step 0 design-spike note — canonicalization seam unification

Card `schema-canon-seam-unification`; plan `canonicalization_seam_unification_plan.md` (fold PR #1272).
Verified against `state.rs` / `state_matching.rs` at `origin/main` (this is the live
shape of `add_schema_inner`, lines 1623-2735, and the matchers).

## 1. Lock / await call shape

Today's `add_schema_inner` is optimistic concurrency: every `self.schemas` touch is a
short scoped `read_lock`/`write_lock` block; the embedder `.await` (`shadow_aware_dual_signal_check`)
is never crossed by a guard (see the explicit comment around the descriptive-name index
write; a `RwLockWriteGuard` held across `.await` violates `Send`). The implemented
pipeline preserves that boundary: candidate generation is read-only, clones target schemas,
and drops all guards before the shared gate helper awaits.

```
add_schema_inner(incoming):
  validate + canonicalize + dedup_fields + compute_identity_hash   # unchanged, stays upstream

  cands  = generate_candidates(incoming)        # scoped read_lock, clone, DROP guard
  winner = rank(cands)                           # seam priority, then score-in-seam
  verdict = gate(incoming, winner).await         # embedder await; NO guard held
     -> Merge                : expand_schema(winner.target).await ; return
     -> RescueRetry          : retry reuse-before-new with allow_same_name
     -> DeCollideAndRegister : fall through to final revalidation / register-new

  # Final race revalidation remains at the name slot.
  # If a concurrent schema now owns the descriptive_name, route through the
  # same gate helper and then expand or fall through to de-collide/register.

  persist_schema(incoming).await                  # no guard
  { write_lock(schemas); insert }                 # scoped, no await inside
  { write_lock(descriptive_name_index); insert }  # scoped
```

The race revalidation is intentionally narrower than a second full candidate generation pass:
it preserves the current duplicate-name defense without holding a write lock across the
embedder await, and it centralizes the gate policy through the same helper used by the
normal ranked-candidate path.

## 2. Race-recheck decision

Decision: keep the final descriptive-name race guard as the concurrency defense, but route its
strict purpose check through `canonicalization_gate_outcome`. That means the normal path has
one ranked candidate pipeline, and the race path has one same-name revalidation. The guard still
does not take a write lock until after all async checks have completed.

## 3. Send / lock-across-await compile check — PASSED

The shipped implementation compiles and passes the full `schema_service_core` suite. The
important mechanical property is that `generate_canonicalization_candidates` returns owned
candidate data and `canonicalization_gate_outcome` awaits only after those locks are gone.
`cargo test -p schema_service_core` exercises the compiled future shape end to end.

## 4. Pre-flight: state.rs overlap with `schema-decompose-apply-path`

Checked with:

```
git diff --name-only origin/fkanban/schema-decompose-apply-path...HEAD -- schema_service/crates/core/src/state.rs
git diff --stat origin/fkanban/schema-decompose-apply-path...HEAD -- schema_service/crates/core/src/state.rs
```

There is file-level overlap in `schema_service/crates/core/src/state.rs` (stat at the time of
the check: 174 insertions / 39 deletions against that branch). The compositional apply hooks
already present on `origin/main` were preserved: the pre-seam `apply_compositional_reuse` call
and the terminal `Composed` return are still in place.

## 5. Eval corpus coverage

The small hand-authored `corpus.json` has two photo cases with different names ("Photo Library"
and "Camera Roll"). The generated eval corpora do cover same-name photo variants:
`corpus_generated.json` has 3 `expected_concept=photo` items named "Photos", and
`corpus_generated_64.json` has 6 such items. The exact same-name de-collide path is also pinned
by the existing unmodified `exact_name_distinct_purpose_decollides_instead_of_409` test.

The available local eval command was run before and after this refactor on the hand corpus:

```
node schema_service/eval/run.mjs --baseline=empty
```

Both runs produced identical scores and confluence. The run is locally degraded by the Ollama
`llama3.3` endpoint returning 404 for classification fallback, but that failure mode was identical
before and after.
