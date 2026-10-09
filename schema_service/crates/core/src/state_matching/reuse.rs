// lint:file-size-ok verbatim move from the original module; one long function remains, splitting it is separate work
use super::*;

impl SchemaServiceState {
    /// Reuse-before-NEW, but also consider an EXACT same-`descriptive_name`
    /// candidate as a reuse target.
    ///
    /// The default [`find_purpose_reuse_target`] deliberately skips a candidate
    /// whose `descriptive_name` exactly matches the proposal's (line in the
    /// inner fn) because that case is the exact/semantic-name merge seam's job
    /// upstream. But when that upstream seam *purpose-VETOED* an exact-name
    /// match under the strict whole-schema dual-signal τ
    /// ([`PURPOSE_SIMILARITY_THRESHOLD`] = 0.88), the proposal was NOT handled —
    /// it falls through every seam to the final duplicate guard and hard-409s
    /// ("same descriptive_name, different identity hash"), rejecting a
    /// well-formed ingestion outright. Two `Photos` proposals (purpose-blob
    /// 0.85 < 0.88) hit exactly this dead-end.
    ///
    /// This variant gives that vetoed exact-name proposal the SAME last-chance,
    /// looser purpose gate ([`REUSE_PURPOSE_THRESHOLD`] = 0.65) + field-fidelity
    /// floor that every *differently*-named purpose-coincidental concept already
    /// gets. It does NOT weaken the dual-signal split: the looser gate is still
    /// a real veto (Photos 0.85 ≥ 0.65 reuses → ONE canonical; a genuinely
    /// distinct same-name pair like Meeting-Notes discuss-vs-schedule 0.81 with
    /// disjoint fields fails the field-fidelity floor, and Hiking 0.69 fails the
    /// purpose gate — both register their OWN canonical instead of merging).
    /// Card `schema-canon-exact-name-veto-409`.
    pub(crate) fn find_purpose_reuse_target_allow_same_name(
        &self,
        incoming: &Schema,
    ) -> Option<(String, String)> {
        self.find_purpose_reuse_target_inner(incoming, true)
    }

    pub(crate) fn find_lexical_field_reuse_target(
        &self,
        incoming: &Schema,
    ) -> Option<(String, String)> {
        if !reuse_before_new_enabled() || self.is_seeding_in_progress() {
            return None;
        }
        let inc_desc = strip_decollision_suffix(incoming.descriptive_name.as_deref()?);
        let field_fidelity = reuse_threshold_override(
            "SCHEMA_REUSE_FIELD_FIDELITY_FLOOR",
            REUSE_FIELD_FIDELITY_FLOOR,
        );

        fn normalize(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        let target_owner = normalize(incoming.owner_app_id.as_deref());
        let schemas = read_lock(&self.schemas, "schemas").ok()?;

        let mut best: Option<(String, String, f32)> = None;
        for existing in schemas.values() {
            if existing.superseded_by.is_some() {
                continue;
            }
            if crate::builtin_schemas::is_schema_org_leftover(existing) {
                continue;
            }
            if normalize(existing.owner_app_id.as_deref()) != target_owner {
                continue;
            }
            if self.is_system_schema(&existing.name) {
                continue;
            }
            if super::super::state_expansion::is_cross_schema_type_expansion(incoming, existing) {
                continue;
            }
            // Different lookup keys ⇒ multi-key sibling, not field-reuse merge target.
            if super::super::state_expansion::is_cross_key_layout(incoming, existing) {
                continue;
            }
            let Some(ex_desc) = existing.descriptive_name.as_deref() else {
                continue;
            };
            let ex_desc_semantic = strip_decollision_suffix(ex_desc);
            if lexical_name_similarity(inc_desc, ex_desc_semantic) < REUSE_STRUCT_FLOOR {
                continue;
            }
            let candidate = BestReuseCandidate {
                hash: existing.name.clone(),
                desc: ex_desc.to_string(),
                purpose_sim: 0.0,
                fields: existing.fields.clone().unwrap_or_default(),
                field_descriptions: existing.field_descriptions.clone(),
            };
            let coverage = self.reuse_field_coverage(incoming, &candidate);
            if coverage < field_fidelity {
                continue;
            }
            if best
                .as_ref()
                .is_none_or(|(_, _, best_cov)| coverage > *best_cov)
            {
                best = Some((candidate.hash, candidate.desc, coverage));
            }
        }
        best.map(|(hash, desc, _)| (hash, desc))
    }

    /// **Reuse-before-NEW.** Last-chance, purpose-gated semantic-overlap check
    /// run just before a proposal that missed every structural merge seam
    /// (identity-hash, descriptive_name, semantic-name, field-overlap) would
    /// register a brand-new canonical. Scans the active, same-namespace
    /// canonicals for one whose *purpose* agrees with the proposal strongly
    /// enough to be the same concept under a different surface name, and
    /// returns its `(identity_hash, descriptive_name)` so the caller can
    /// `expand_schema` into it instead of spawning a near-duplicate.
    ///
    /// This is the dup-explosion fix (card
    /// `schema-canon-purpose-aware-matching`): semantically-equivalent inputs
    /// whose descriptive_names merely differ ("Financial Transactions" vs
    /// "Transactions") should collapse to one canonical. Schemas are
    /// identified by PURPOSE, not field shape
    /// (`feedback_schema_semantic_distinction`), so the gate is the existing
    /// dual-signal purpose signal.
    ///
    /// **Correctness invariant (wrong-merge HARD zero):** a candidate must
    /// clear BOTH the purpose gate ([`REUSE_PURPOSE_THRESHOLD`], = the
    /// whole-schema τ_purpose) AND a descriptive_name structural floor
    /// ([`REUSE_STRUCT_FLOOR`]). Purpose alone is deliberately not enough —
    /// two distinct concepts can phrase their purpose similarly, and merging
    /// on that would manufacture exactly the cross-concept wrong-merge the
    /// dual-signal gate exists to prevent. Requiring real descriptive_name
    /// overlap keeps reuse to genuine same-concept name variants. Because
    /// both thresholds are ≥ the corresponding structural-seam thresholds,
    /// this path can only *add* reuses the earlier seams missed; it can never
    /// merge a pair the dual-signal gate would have vetoed.
    ///
    /// Read-only: takes the `schemas` read lock briefly, never persists.
    /// Returns `None` when the feature is disabled, no candidate clears both
    /// gates, or the proposal carries no `descriptive_name`.
    // lint:fn-size-ok moved verbatim from the original module; splitting it is a separate change
    pub(super) fn find_purpose_reuse_target_inner(
        &self,
        incoming: &Schema,
        allow_same_name: bool,
    ) -> Option<(String, String)> {
        if !reuse_before_new_enabled() {
            return None;
        }
        // Authoritative seeding (built-ins, persona/schema.org seeds) must
        // register its committed schemas EXACTLY — never fuzzy-merge one into a
        // semantically-near committed neighbour. Skip the whole fuzzy path
        // while a seeder is running (keeps `builtin_schemas::seed`'s "built-ins
        // never expand" startup invariant and the deterministic seed registry).
        if self.is_seeding_in_progress() {
            return None;
        }
        // A proposal with no descriptive_name has no structural signal to floor
        // against — leave it to register as new (it can't safely reuse).
        incoming.descriptive_name.as_deref()?;

        let purpose_threshold =
            reuse_threshold_override("SCHEMA_REUSE_PURPOSE_THRESHOLD", REUSE_PURPOSE_THRESHOLD);
        let struct_floor =
            reuse_threshold_override("SCHEMA_REUSE_STRUCT_FLOOR", REUSE_STRUCT_FLOOR);
        let field_fidelity = reuse_threshold_override(
            "SCHEMA_REUSE_FIELD_FIDELITY_FLOOR",
            REUSE_FIELD_FIDELITY_FLOOR,
        );

        fn normalize(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        let target_owner = normalize(incoming.owner_app_id.as_deref());

        // Embed the incoming side ONCE up front (its descriptive_name for the
        // struct signal, its `"{desc} — {purpose}"` blob for the purpose
        // signal). Embed failure ⇒ no reuse (matches the
        // `dual_signal_diagnostic` "embed failure = veto" contract).
        //
        // Use the SEMANTIC name (cross-schema_type de-collision suffix stripped)
        // for both signals — the `" (<Type>)"` artifact is storage
        // disambiguation, not concept identity, and embedding it poisons the
        // match (see [`strip_decollision_suffix`]). The incoming proposal is
        // pre-de-collision here in the normal path, but a re-published already-
        // de-collided name could arrive, so strip it too for symmetry.
        let inc_desc = strip_decollision_suffix(incoming.descriptive_name.as_deref().unwrap_or(""));
        let inc_purpose = incoming.purpose_statement.as_deref().unwrap_or("");
        let inc_blob = format!("{inc_desc} — {inc_purpose}");
        let inc_desc_vec = self.embedder.embed_text(inc_desc).ok()?;

        let schemas = read_lock(&self.schemas, "schemas").ok()?;
        // The struct signal reads the SAME pre-computed `descriptive_name`
        // embedding cache that `find_matching_descriptive_name` uses — keyed by
        // `descriptive_name_key(owner, desc)`. This is the critical perf +
        // consistency choice: candidates whose embedding isn't cached are
        // SKIPPED (never embedded inline), so this last-chance scan never pays
        // a cold-embed-per-candidate cost in-request — exactly as the existing
        // semantic-name seam behaves (uncached schemas, e.g. un-warmed
        // schema.org seeds, simply aren't fuzzy-match candidates until
        // `warm-embeddings` runs). A user schema registered earlier in the same
        // process already has its descriptive_name embedding cached, so genuine
        // user→user dedup ("Transactions" ≈ "Financial Transactions") still
        // fires; we just don't block the request embedding 900+ seeds.
        let embeddings = read_lock(
            &self.descriptive_name_embeddings,
            "descriptive_name_embeddings",
        )
        .ok()?;

        // Best candidate by purpose similarity that clears BOTH gates, plus a
        // separately-tracked best *raw* candidate (gates ignored) purely for a
        // debug log — so threshold tuning has visibility into near-misses
        // without lowering the live gates.
        // `best` carries everything the field-fidelity guard (run after the
        // lock drops) needs about the chosen candidate: its identity hash, its
        // descriptive_name, the purpose score, and its field set + descriptions
        // (cloned so the guard can embed field names without re-taking the
        // schemas lock).
        let mut best: Option<BestReuseCandidate> = None;
        let mut best_raw: Option<(String, f32, f32)> = None;
        for existing in schemas.values() {
            // Active, same-namespace canonicals only — mirrors the superseded +
            // namespace filters at the structural merge seams so reuse can't
            // land on a retired or cross-app schema.
            if existing.superseded_by.is_some() {
                continue;
            }
            if crate::builtin_schemas::is_schema_org_leftover(existing) {
                continue;
            }
            if normalize(existing.owner_app_id.as_deref()) != target_owner {
                continue;
            }
            let Some(ex_desc) = existing.descriptive_name.as_deref() else {
                continue;
            };
            // Never reuse the proposal against an entry with its own
            // descriptive_name — that case is the exact-name / semantic-name
            // merge seam's job and was already handled (or vetoed) upstream.
            //
            // EXCEPTION (`allow_same_name`): when the upstream exact-name seam
            // *purpose-vetoed* the merge (strict τ 0.88) and fell through here,
            // the same-name candidate was NOT handled — without this exception
            // the proposal dead-ends at the final duplicate guard's 409. Let it
            // be reconsidered under the looser reuse gates so a same-name
            // same-concept pair (Photos, purpose 0.85) collapses to one
            // canonical instead of 409'ing. See
            // [`find_purpose_reuse_target_allow_same_name`].
            if !allow_same_name
                && normalize(Some(ex_desc)) == normalize(incoming.descriptive_name.as_deref())
            {
                continue;
            }
            // Never reuse INTO a system/infrastructure schema. The Phase-1
            // built-ins (Fingerprint, Edge, Identity, Persona, …) are
            // authoritative and are seeded through this same `add_schema`
            // path at startup; letting a fuzzy purpose match expand one
            // built-in into another would (a) break the
            // `builtin_schemas::seed` invariant that built-ins never expand
            // an existing schema — aborting service start — and (b) dissolve
            // a distinct system identity. A user schema that genuinely IS a
            // built-in concept already merges via the exact/semantic-name
            // seam upstream; this last-chance fuzzy path stays user→user.
            if self.is_system_schema(&existing.name) {
                continue;
            }
            // Cross-schema_type reuse would corrupt molecule reads (same guard
            // the structural seams apply before expanding) — skip it here so we
            // don't propose a merge the expansion path would only 409 on.
            if super::super::state_expansion::is_cross_schema_type_expansion(incoming, existing) {
                continue;
            }
            if super::super::state_expansion::is_cross_key_layout(incoming, existing) {
                continue;
            }

            // Compare on the candidate's SEMANTIC name — strip any
            // cross-schema_type de-collision suffix so a de-collided canonical
            // (`"Contacts (Hash)"`) is matched as the concept it actually is
            // (`"Contacts"`). See [`strip_decollision_suffix`].
            let ex_desc_semantic = strip_decollision_suffix(ex_desc);
            let was_decollided = ex_desc_semantic != ex_desc;

            // Struct signal first — the cheap pre-filter gate. Exact
            // case-insensitive name → 1.0 (mirrors `dual_signal_diagnostic`'s
            // short-circuit). For an un-mangled candidate read the pre-computed
            // descriptive_name embedding (keyed by the STORED name) and skip if
            // uncached (see the cache note above) — this keeps the hot path off
            // a cold-embed-per-seed cost. For the RARE de-collided candidate the
            // cached embedding is of the poisoned `" (<Type>)"` name, so embed
            // the stripped semantic name inline instead; de-collided canonicals
            // are few, so this stays cheap.
            let struct_sim = if inc_desc.eq_ignore_ascii_case(ex_desc_semantic) {
                1.0
            } else if was_decollided {
                let Ok(ex_vec) = self.embedder.embed_text(ex_desc_semantic) else {
                    continue;
                };
                cosine_similarity(&inc_desc_vec, &ex_vec)
            } else {
                let key =
                    crate::state::descriptive_name_key(existing.owner_app_id.as_deref(), ex_desc);
                let Some(ex_vec) = embeddings.get(&key) else {
                    continue;
                };
                cosine_similarity(&inc_desc_vec, ex_vec)
            };
            let struct_sim = struct_sim.max(lexical_name_similarity(inc_desc, ex_desc_semantic));
            if struct_sim < struct_floor {
                continue; // pruned cheaply — no purpose embed needed
            }

            // Only now (rare — struct floor already passed) pay for the purpose
            // signal. The incoming blob is embedded at most once here (cached
            // thereafter); each surviving candidate's blob likewise. Build the
            // candidate blob from its SEMANTIC name so the de-collision suffix
            // doesn't contaminate the purpose signal either.
            let ex_blob = format!(
                "{ex_desc_semantic} — {}",
                existing.purpose_statement.as_deref().unwrap_or("")
            );
            let purpose_sim = if inc_blob == ex_blob {
                1.0
            } else {
                let (Ok(a), Ok(b)) = (
                    self.embedder.embed_text(&inc_blob),
                    self.embedder.embed_text(&ex_blob),
                ) else {
                    continue;
                };
                cosine_similarity(&a, &b)
            };

            let cand_desc = ex_desc.to_string();
            if best_raw.as_ref().is_none_or(|(_, p, _)| purpose_sim > *p) {
                best_raw = Some((cand_desc.clone(), purpose_sim, struct_sim));
            }
            let candidate = BestReuseCandidate {
                hash: existing.name.clone(),
                desc: cand_desc,
                purpose_sim,
                fields: existing.fields.clone().unwrap_or_default(),
                field_descriptions: existing.field_descriptions.clone(),
            };
            let field_coverage = self.reuse_field_coverage(incoming, &candidate);
            if purpose_sim < purpose_threshold {
                // Field coverage may rescue a low purpose score (this is how
                // "Transactions" / "Financial Transactions" eval duplicates
                // collapse), but only when the shared fields are DESCRIBED
                // the same way. Coverage counts literal name matches, so on
                // names alone LastgitPackBlobIndex (purpose 0.57) expanded into
                // LastgitRepoIndex on DEV (2026-09-22): both are `key` /
                // `payload_json` / `updated_at` rollups, of pack blobs and of
                // repos.
                if field_coverage < field_fidelity {
                    continue;
                }
                if !self.shared_field_descriptions_agree(incoming, &candidate) {
                    tracing::info!(
                        target: "schema_service::schema",
                        incoming_desc = %incoming.descriptive_name.as_deref().unwrap_or(""),
                        candidate = %candidate.desc,
                        purpose_similarity = purpose_sim,
                        field_coverage,
                        "Reuse-before-NEW: field coverage cannot rescue a low purpose score — the shared fields are described differently",
                    );
                    continue;
                }
            }
            if best.as_ref().is_none_or(|b| purpose_sim > b.purpose_sim) {
                best = Some(candidate);
            }
        }
        drop(embeddings);
        drop(schemas);

        if best.is_none() {
            if let Some((desc, p, s)) = best_raw.as_ref() {
                tracing::debug!(
                    target: "schema_service::schema",
                    incoming_desc = %incoming.descriptive_name.as_deref().unwrap_or(""),
                    best_candidate = %desc,
                    purpose_similarity = p,
                    struct_similarity = s,
                    purpose_threshold,
                    struct_floor,
                    "Reuse-before-NEW: best candidate fell short of the gates — registering new",
                );
            }
        }

        let best = best?;

        // Field-fidelity guard (card `schema-eval-judge-recommendations`). The
        // purpose+struct gates above never inspected the field sets, and every
        // earlier seam that DID (exact-hash dedup, semantic-name merge, the
        // Jaccard ≥ 0.6 field-overlap fallback) already missed — so by
        // construction this candidate's fields do NOT strongly overlap the
        // proposal's. Reusing/expanding into it anyway is the over-generalization
        // the eval's LLM-judge flags as a field-fidelity failure: the proposal's
        // own fields get force-merged into (or dropped in favour of) a canonical
        // full of fields the input never had. Only honour the reuse when the
        // proposal genuinely IS an instance of the candidate concept — i.e. most
        // of its fields map into the candidate's field set (literal match or a
        // confident semantic rename). Otherwise this is a distinct concept that
        // merely phrases its purpose similarly; let it register its OWN canonical
        // (NEW) with exactly its input fields.
        let coverage = self.reuse_field_coverage(incoming, &best);
        if coverage < field_fidelity {
            tracing::info!(
                target: "schema_service::schema",
                incoming_desc = %incoming.descriptive_name.as_deref().unwrap_or(""),
                reuse_candidate = %best.desc,
                purpose_similarity = best.purpose_sim,
                field_coverage = coverage,
                field_fidelity_floor = field_fidelity,
                "Reuse-before-NEW: purpose+name matched but the proposal's fields don't correspond to the candidate (over-generalization) — registering NEW instead of reusing",
            );
            return None;
        }

        tracing::info!(
            target: "schema_service::schema",
            incoming_desc = %incoming.descriptive_name.as_deref().unwrap_or(""),
            reuse_target = %best.desc,
            purpose_similarity = best.purpose_sim,
            field_coverage = coverage,
            "Reuse-before-NEW: purpose-gated semantic overlap with an existing canonical — expanding instead of registering a near-duplicate",
        );
        Some((best.hash, best.desc))
    }
}
