use super::*;

/// Best reuse candidate so far plus the closest rejected `(name, score, threshold)`.
type ScanOutcome = (Option<BestReuseCandidate>, Option<(String, f32, f32)>);

pub(super) fn normalize(s: Option<&str>) -> Option<&str> {
    s.filter(|x| !x.is_empty())
}

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
        if !self.reuse_before_new_enabled() || self.is_seeding_in_progress() {
            return None;
        }
        let inc_desc = strip_decollision_suffix(incoming.descriptive_name.as_deref()?);
        let field_fidelity = self.reuse_field_fidelity_floor();

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
    pub(super) fn find_purpose_reuse_target_inner(
        &self,
        incoming: &Schema,
        allow_same_name: bool,
    ) -> Option<(String, String)> {
        if !self.reuse_before_new_enabled() {
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

        let scan = self.build_reuse_scan(incoming)?;
        let target_owner = normalize(incoming.owner_app_id.as_deref());
        let (best, best_raw) = self.scan_reuse_candidates(&scan, target_owner, allow_same_name)?;

        if best.is_none() {
            log_reuse_near_miss(&scan, best_raw.as_ref());
        }

        let best = best?;
        self.apply_field_fidelity_guard(incoming, best, scan.field_fidelity)
    }

    /// Read the live gate thresholds and embed the incoming side. `None` when
    /// the embedder fails.
    fn build_reuse_scan<'a>(&self, incoming: &'a Schema) -> Option<ReuseScan<'a>> {
        let purpose_threshold = self.reuse_purpose_threshold();
        let struct_floor = self.reuse_struct_floor();
        let field_fidelity = self.reuse_field_fidelity_floor();

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

        Some(ReuseScan {
            incoming,
            inc_desc,
            inc_desc_vec,
            inc_blob,
            purpose_threshold,
            struct_floor,
            field_fidelity,
        })
    }

    /// Walk the active schemas under the read locks and pick the best reuse
    /// candidate that clears both gates, plus the best raw near-miss (gates
    /// ignored) for the debug log. Returns `None` when a lock is poisoned.
    fn scan_reuse_candidates(
        &self,
        scan: &ReuseScan<'_>,
        target_owner: Option<&str>,
        allow_same_name: bool,
    ) -> Option<ScanOutcome> {
        let incoming = scan.incoming;
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
            if !self.is_reuse_eligible(incoming, existing, target_owner, allow_same_name) {
                continue;
            }
            let Some((candidate, struct_sim, accepted)) =
                self.score_reuse_candidate(scan, existing, &embeddings)
            else {
                continue;
            };
            if best_raw
                .as_ref()
                .is_none_or(|(_, p, _)| candidate.purpose_sim > *p)
            {
                best_raw = Some((candidate.desc.clone(), candidate.purpose_sim, struct_sim));
            }
            if accepted
                && best
                    .as_ref()
                    .is_none_or(|b| candidate.purpose_sim > b.purpose_sim)
            {
                best = Some(candidate);
            }
        }
        Some((best, best_raw))
    }

    /// Score one eligible reuse candidate against the scan inputs.
    ///
    /// Returns `None` when the candidate has no descriptive_name, no usable
    /// embedding, or falls below the struct floor. Otherwise returns the
    /// candidate, its struct similarity and whether it clears the live gates
    /// (purpose threshold, or field coverage rescue with agreeing field
    /// descriptions). Candidates that fail the gates are still returned so the
    /// caller can track the best raw near-miss for the debug log.
    fn score_reuse_candidate(
        &self,
        scan: &ReuseScan<'_>,
        existing: &Schema,
        embeddings: &HashMap<String, Vec<f32>>,
    ) -> Option<(BestReuseCandidate, f32, bool)> {
        let ex_desc = existing.descriptive_name.as_deref()?;
        // Compare on the candidate's SEMANTIC name — strip any
        // cross-schema_type de-collision suffix so a de-collided canonical
        // (`"Contacts (Hash)"`) is matched as the concept it actually is
        // (`"Contacts"`). See [`strip_decollision_suffix`].
        let ex_desc_semantic = strip_decollision_suffix(ex_desc);

        // Struct signal first — the cheap pre-filter gate. Exact
        // case-insensitive name → 1.0 (mirrors `dual_signal_diagnostic`'s
        // short-circuit). Un-mangled candidates read the pre-computed
        // descriptive_name embedding and are skipped when uncached; the rare
        // de-collided candidate embeds its stripped semantic name inline.
        let struct_sim = self.reuse_struct_similarity(
            scan.inc_desc,
            &scan.inc_desc_vec,
            existing,
            ex_desc,
            embeddings,
        )?;
        if struct_sim < scan.struct_floor {
            return None; // pruned cheaply — no purpose embed needed
        }

        // Only now (rare — struct floor already passed) pay for the purpose
        // signal, built from the SEMANTIC name so the de-collision suffix
        // doesn't contaminate it either.
        let purpose_sim =
            self.reuse_purpose_similarity(&scan.inc_blob, existing, ex_desc_semantic)?;

        let candidate = BestReuseCandidate {
            hash: existing.name.clone(),
            desc: ex_desc.to_string(),
            purpose_sim,
            fields: existing.fields.clone().unwrap_or_default(),
            field_descriptions: existing.field_descriptions.clone(),
        };
        let accepted = purpose_sim >= scan.purpose_threshold
            || self.field_coverage_rescues_purpose(scan, &candidate);
        Some((candidate, struct_sim, accepted))
    }

    /// Field coverage may rescue a low purpose score (this is how
    /// "Transactions" / "Financial Transactions" eval duplicates collapse), but
    /// only when the shared fields are DESCRIBED the same way. Coverage counts
    /// literal name matches, so on names alone LastgitPackBlobIndex (purpose
    /// 0.57) expanded into LastgitRepoIndex on DEV (2026-09-22): both are `key` /
    /// `payload_json` / `updated_at` rollups, of pack blobs and of repos.
    fn field_coverage_rescues_purpose(
        &self,
        scan: &ReuseScan<'_>,
        candidate: &BestReuseCandidate,
    ) -> bool {
        let field_coverage = self.reuse_field_coverage(scan.incoming, candidate);
        if field_coverage < scan.field_fidelity {
            return false;
        }
        if !self.shared_field_descriptions_agree(scan.incoming, candidate) {
            tracing::info!(
                target: "schema_service::schema",
                incoming_desc = %scan.incoming.descriptive_name.as_deref().unwrap_or(""),
                candidate = %candidate.desc,
                purpose_similarity = candidate.purpose_sim,
                field_coverage,
                "Reuse-before-NEW: field coverage cannot rescue a low purpose score — the shared fields are described differently",
            );
            return false;
        }
        true
    }
}

/// Inputs shared by every candidate in one purpose-reuse scan.
struct ReuseScan<'a> {
    incoming: &'a Schema,
    inc_desc: &'a str,
    inc_desc_vec: Vec<f32>,
    inc_blob: String,
    purpose_threshold: f32,
    struct_floor: f32,
    field_fidelity: f32,
}

/// Debug-log the best raw candidate that fell short of the gates, so
/// threshold tuning sees near-misses without lowering the live gates.
fn log_reuse_near_miss(scan: &ReuseScan<'_>, best_raw: Option<&(String, f32, f32)>) {
    if let Some((desc, p, s)) = best_raw {
        tracing::debug!(
            target: "schema_service::schema",
            incoming_desc = %scan.incoming.descriptive_name.as_deref().unwrap_or(""),
            best_candidate = %desc,
            purpose_similarity = p,
            struct_similarity = s,
            purpose_threshold = scan.purpose_threshold,
            struct_floor = scan.struct_floor,
            "Reuse-before-NEW: best candidate fell short of the gates — registering new",
        );
    }
}
