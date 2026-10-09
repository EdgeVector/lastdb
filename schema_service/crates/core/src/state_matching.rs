use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::embedder::cosine_similarity;
use crate::lock_helpers::read_lock;
use schema_types::FoldDbResult;
use schema_types::Schema;

use super::state::SchemaServiceState;

/// Minimum cosine similarity between descriptive names to consider them a semantic match.
pub(super) const DESCRIPTIVE_NAME_SIMILARITY_THRESHOLD: f32 = 0.8;

/// Minimum cosine similarity between context-enriched field names to consider them synonyms.
/// Threshold for canonicalize_fields: compares description-only embeddings against
/// the canonical field registry. Set to 0.88 based on empirical testing with
/// all-MiniLM-L6-v2: true synonyms score ≥0.88, false positives score ≤0.85.
pub(super) const FIELD_SIMILARITY_THRESHOLD: f32 = 0.88;

/// Threshold for semantic_field_rename_map: compares hybrid embeddings
/// ("the {name} of the {context}: {description}") during schema expansion.
/// Lower than FIELD_SIMILARITY_THRESHOLD because the hybrid format includes
/// field names and context that reduce absolute similarity, but the bidirectional
/// best-match check provides strong false-positive protection.
/// Empirical: start_date↔start_time=0.86, venue↔location=0.86, tags↔content=0.83 (reject).
pub(super) const SEMANTIC_RENAME_THRESHOLD: f32 = 0.84;

/// τ_purpose — minimum cosine similarity between purpose-statement embeddings
/// (formatted as `"{descriptive_name} — {purpose_statement}"`) to confirm a
/// structural canonicalization match. Phase B of the fbrain
/// `dual-signal-schema-canonicalization` design. Gated by env flag
/// `SCHEMA_DUAL_SIGNAL_CANONICALIZATION`. Tighter than the structural gate
/// because purpose statements are short and high-signal.
pub(super) const PURPOSE_SIMILARITY_THRESHOLD: f32 = 0.88;

/// Whether the dual-signal (structural + purpose) canonicalization gate is
/// active. **Default on** as of Phase E cutover — callers get dual-signal
/// unless the env var is explicitly set to `0`/`false`/`FALSE`/`no`, in
/// which case they fall back to the legacy single-signal behavior. The
/// explicit-off escape hatch lets operators pin back during deployment
/// rollout without a code change.
pub(super) fn dual_signal_canonicalization_enabled() -> bool {
    !std::env::var("SCHEMA_DUAL_SIGNAL_CANONICALIZATION")
        .is_ok_and(|v| matches!(v.as_str(), "0" | "false" | "FALSE" | "no"))
}

/// Default purpose-similarity threshold for the **reuse-before-NEW** check
/// (`find_purpose_reuse_target`). A proposal that survived every structural
/// merge seam — same descriptive_name, semantic-name, and field-overlap all
/// missed — gets one last purpose-gated look at the existing canonicals
/// before it spawns a brand-new one. On a strong same-purpose match it
/// EXPANDS into the existing canonical instead of dup-exploding.
///
/// **Calibrated empirically** against `schema_service/eval` on the cleaned
/// `corpus_generated.json` with the production all-MiniLM-L6-v2 embedder
/// (card `schema-canon-purpose-aware-matching`, 2026-06-21). Measured
/// `"{desc} — {purpose}"` cosines:
///   - "Transactions" ↔ "Financial Transactions" (same concept) = **0.745**
///   - "Events" ↔ "Meeting Schedules" (same concept) = 0.717
///   - "Events" ↔ "Meeting Notes" (CROSS concept) = 0.565
///   - note variants (Journal/Travel/Meeting) = 0.38–0.43
///
/// 0.65 admits the genuine same-concept transaction/contact/photo variants while the
/// descriptive_name floor below (not the purpose number alone) is what
/// excludes the cross-concept Events↔Meeting-Notes case. Purpose alone is
/// deliberately NOT trusted to separate concepts here — the floor is the
/// correctness guard (see `REUSE_STRUCT_FLOOR`). Lower than the whole-schema
/// dual-signal τ ([`PURPOSE_SIMILARITY_THRESHOLD`] = 0.88) because that gate
/// runs in tandem with an *exact/near* descriptive_name match, whereas this
/// last-chance path pairs a looser purpose bar with a much TIGHTER name
/// floor. Overridable via `SCHEMA_REUSE_PURPOSE_THRESHOLD`.
pub const REUSE_PURPOSE_THRESHOLD: f32 = 0.65;

/// Structural floor for the **reuse-before-NEW** check: the candidate's
/// descriptive_name cosine must clear this as a cheap PRE-FILTER before its
/// purpose score is consulted. The floor is NOT the wrong-merge guard —
/// **the purpose gate ([`REUSE_PURPOSE_THRESHOLD`]) is** (that is the whole
/// point of the dual-signal design: the purpose signal is the veto). The
/// floor's only job is to prune obviously-unrelated candidates cheaply
/// (skip the purpose embed) and to require *some* surface-name overlap so
/// reuse stays interpretable.
///
/// **Why a low floor + a strong purpose gate, not a high floor.** The
/// descriptive_name signal alone CANNOT separate same-concept from
/// cross-concept in the danger zone — it inverts. Measured (all-MiniLM-L6-v2,
/// `corpus.json`, 2026-06-23):
///   - "Contacts" ↔ "Contact Records" (SAME concept) name = **0.628**
///   - "Customers" ↔ "Customer Orders" (CROSS concept) name = **0.745**
///
/// A name floor high enough to admit the genuine "Contacts"/"Contact Records"
/// variant (0.628) would also admit the cross-concept Customers↔Orders pair
/// (0.745) → a manufactured wrong-merge. The OLD floor of 0.80 avoided that
/// only by *also* excluding the genuine variant — i.e. it bought correctness
/// by leaving the dup-explosion uncollapsed (the bug this card fixes).
///
/// The **purpose signal** separates them cleanly where the name signal can't.
/// Same measurement, `"{desc} — {purpose}"` blob cosines:
///   - "Contacts" ↔ "Contact Records" (SAME) purpose = **0.894** (≥ τ 0.65 ✓)
///   - "Customers" ↔ "Customer Orders" (CROSS) purpose = **0.538** (< τ ✗)
///   - "Contacts" ↔ "Addresses" (CROSS) purpose = **0.528** (< τ ✗)
///   - "Customer Orders" ↔ "Invoices" (CROSS) purpose = **0.581** (< τ ✗)
///
/// Every cross-concept pair sits ≤ 0.581 while the one same-concept variant is
/// 0.894 — a wide, clean margin around τ_purpose = 0.65.
///
/// So the floor drops to **0.55**: low enough that the genuine same-concept
/// name variant (0.628) clears it, while the purpose gate vetoes every
/// cross-concept pair that also clears it (Customers↔Orders, Orders↔Invoices,
/// Contacts↔Addresses all fail purpose). Pairs below 0.55 (the bulk —
/// Photos↔Recipes 0.43 etc.) are still pruned cheaply without a purpose embed.
/// Overridable via `SCHEMA_REUSE_STRUCT_FLOOR`.
pub const REUSE_STRUCT_FLOOR: f32 = 0.55;

/// Field-fidelity floor for the **reuse-before-NEW** check: the fraction of the
/// proposal's own fields that must genuinely correspond to a field in the reuse
/// candidate (by literal name OR a confident semantic rename) before the
/// purpose match is allowed to become a REUSE/EXPAND rather than a fresh NEW.
///
/// **Why this exists.** The purpose+struct gates above are computed entirely on
/// the *names and purposes* of the two schemas — they never look at the field
/// sets. Every earlier structural seam that DID look at fields (the exact-hash
/// dedup, the semantic-name merge, the Jaccard ≥ 0.6 field-overlap fallback)
/// already missed by the time this last-chance path runs, so by construction
/// this path fires on pairs whose fields do NOT strongly overlap. Letting a
/// purpose-only match expand into such a candidate is exactly the
/// over-generalization the eval's LLM-judge flags as a field-fidelity failure:
/// the proposal's distinct fields get force-merged into (or silently dropped in
/// favor of) a canonical full of fields the input never had. A REUSE/EXPAND is
/// only honest when the input genuinely *is* an instance of the candidate
/// concept — i.e. most of its fields map into the candidate's field set. When
/// they don't, the proposal is a distinct concept that merely phrases its
/// purpose similarly, and it should register its OWN canonical with exactly its
/// input fields (NEW), not inherit a foreign field shape.
///
/// 0.5 means: at least half of the proposal's fields must land in the
/// candidate (after semantic rename). That admits the genuine same-concept
/// surface-name variant (e.g. "Financial Transactions" ⊇ "Transactions", whose
/// fields are a near-superset) while rejecting a purpose-coincidental pair
/// whose field shapes diverge (e.g. a proposal carrying nested
/// `camera_details` / `dimensions` structures absent from the candidate).
/// Subset proposals (input ⊆ candidate) trivially clear it. Overridable via
/// `SCHEMA_REUSE_FIELD_FIDELITY_FLOOR`.
pub(super) const REUSE_FIELD_FIDELITY_FLOOR: f32 = 0.5;

/// Whether the reuse-before-NEW semantic-overlap check runs. **Default on**
/// (same posture as the dual-signal gate it shares thresholds with); set the
/// env var to a falsy value to fall back to the pre-change behavior where a
/// proposal that missed every structural seam always registered a fresh
/// canonical.
pub(super) fn reuse_before_new_enabled() -> bool {
    !std::env::var("SCHEMA_REUSE_BEFORE_NEW")
        .is_ok_and(|v| matches!(v.as_str(), "0" | "false" | "FALSE" | "no"))
}

/// Read an `f32` threshold override from `key`, falling back to `default`
/// when the var is unset or unparseable. Keeps the two reuse-before-NEW
/// thresholds tunable in a deployment without a recompile, mirroring the
/// env-flag escape hatches the dual-signal gate already exposes.
pub(super) fn reuse_threshold_override(key: &str, default: f32) -> f32 {
    env_flag::var_or(key, default)
}

/// Whether the Phase C shadow-mode observability for dual-signal canonicalization
/// is active. Default off. When on, every registration that hits the dual-signal
/// seams also computes the diagnostic via [`SchemaServiceState::dual_signal_diagnostic`];
/// if the dual-signal outcome would disagree with the single-signal outcome a
/// [`crate::near_miss::NearMissRecord`] is persisted. Shadow mode forces the
/// actual decision back to the single-signal algorithm even when
/// [`dual_signal_canonicalization_enabled`] is also true — disagreement data
/// is the goal, so we don't want Phase B vetoes to perturb the baseline.
pub(super) fn shadow_mode_enabled() -> bool {
    env_flag::var_truthy("SCHEMA_SHADOW_MODE")
}

/// Per-comparison snapshot produced by
/// [`SchemaServiceState::dual_signal_diagnostic`]. Field names line up with
/// [`crate::near_miss::NearMissRecord`] so the shadow-mode call sites can
/// copy them across without re-deriving anything.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DualSignalDiagnostic {
    /// Cosine similarity between the two `descriptive_name` embeddings.
    /// Short-circuits to `1.0` when the names match case-insensitively (no
    /// embedder trip needed). When one side has an empty `descriptive_name`
    /// the embedder is still consulted — `cosine_similarity` handles that
    /// fine and the resulting low score is the right shadow signal.
    pub struct_similarity: f32,
    /// Cosine similarity between the two
    /// `"{descriptive_name} — {purpose_statement}"` embeddings — the Phase B
    /// purpose signal.
    pub purpose_similarity: f32,
    /// Whether the dual-signal gate would allow this merge:
    /// `purpose_similarity >= PURPOSE_SIMILARITY_THRESHOLD`.
    pub allows_merge: bool,
}

/// Strip the cross-schema_type **de-collision suffix** from a descriptive_name
/// for the purpose of *semantic* comparison.
///
/// When a User proposal's `descriptive_name` collides with an
/// incompatible-`schema_type` starter seed, [`SchemaServiceState::
/// decollide_seed_descriptive_name`] renames it to `"<name> (<Type>)"` (or
/// `"<name> (<Type> <n>)"`) so the document ingests as its own canonical
/// instead of 409'ing — e.g. a `Hash` "Contacts" proposal lands as
/// `"Contacts (Hash)"` next to the seed's `Range` "Contacts".
///
/// That suffix is a **storage-layer disambiguation artifact, not semantic
/// content**, but it is baked into the stored `descriptive_name` and its
/// cached embedding. Left unstripped it *poisons* every later semantic match:
/// measured (all-MiniLM-L6-v2), `"Contact Records"` ↔ `"Contacts"` name
/// cosine is 0.628 and the purpose-blob 0.894 (a clear same-concept reuse),
/// but `"Contact Records"` ↔ `"Contacts (Hash)"` drops to 0.462 / 0.660 —
/// below both reuse gates — so a genuine same-concept variant dup-explodes
/// instead of collapsing. Stripping the suffix before embedding restores the
/// true concept signal so reuse-before-NEW can see through the artifact.
///
/// Only strips a trailing parenthetical whose content is a known
/// `schema_type` token (optionally followed by a numeric variant counter), so
/// a legitimate name that merely ends in parentheses (`"Notes (2024)"`,
/// `"Recipes (Vegan)"`) is left untouched.
pub(super) fn strip_decollision_suffix(desc: &str) -> &str {
    let trimmed = desc.trim_end();
    let Some(open) = trimmed.rfind(" (") else {
        return desc;
    };
    if !trimmed.ends_with(')') {
        return desc;
    }
    // Content between "… (" and the trailing ")".
    let inner = &trimmed[open + 2..trimmed.len() - 1];
    // First token must be a known schema_type; an optional second token, if
    // present, must be a positive integer variant counter.
    let mut parts = inner.split_whitespace();
    let Some(type_tok) = parts.next() else {
        return desc;
    };
    let is_type = matches!(type_tok, "Single" | "Hash" | "Range" | "HashRange");
    if !is_type {
        return desc;
    }
    match parts.next() {
        None => &trimmed[..open],
        Some(n) if n.chars().all(|c| c.is_ascii_digit()) && parts.next().is_none() => {
            &trimmed[..open]
        }
        // Trailing parenthetical isn't a pure de-collision tag — leave as-is.
        Some(_) => desc,
    }
}

/// The reuse-before-NEW candidate that cleared the purpose + descriptive_name
/// gates, carrying everything the field-fidelity guard needs after the
/// `schemas` read lock has been dropped (so the guard can embed field names
/// without re-taking it). See
/// [`SchemaServiceState::find_purpose_reuse_target`].
struct BestReuseCandidate {
    /// Identity hash (the schema's `name`) of the reuse target.
    hash: String,
    /// The candidate's `descriptive_name` — adopted by the proposal on reuse.
    desc: String,
    /// Purpose-blob cosine that won the candidate selection (for logging).
    purpose_sim: f32,
    /// The candidate's field set (cloned out from under the lock).
    fields: Vec<String>,
    /// The candidate's per-field descriptions (for semantic-rename embedding).
    field_descriptions: HashMap<String, String>,
}

/// Collect all field names from a schema (union of fields and transform_fields keys)
pub(super) fn collect_field_names(schema: &Schema) -> HashSet<String> {
    let mut names = HashSet::new();
    if let Some(ref fields) = schema.fields {
        for f in fields {
            names.insert(f.clone());
        }
    }
    if let Some(ref tf) = schema.transform_fields {
        for key in tf.keys() {
            names.insert(key.clone());
        }
    }
    names
}

/// Compute Jaccard index: |A ∩ B| / |A ∪ B|
pub fn jaccard_index(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    let intersection = a.intersection(b).count();
    let union = a.union(b).count();
    intersection as f64 / union as f64
}

impl SchemaServiceState {
    /// Find an existing descriptive_name that matches the given name within
    /// the `owner_app_id` namespace.
    ///
    /// First tries exact match (via the namespaced
    /// `descriptive_name_index`), then falls back to semantic (embedding)
    /// similarity restricted to other entries in the same namespace.
    /// Returns `(matched_descriptive_name, schema_identity_hash, is_exact_match)`.
    ///
    /// **Namespacing (app_identity v3.1, Lane B2b):** the dedup index and
    /// embedding cache are keyed by `descriptive_name_key(owner_app_id,
    /// desc)` (see [`super::state::descriptive_name_key`]). Passing
    /// `owner_app_id = Some("fbrain")` restricts the search to other
    /// `fbrain/*` schemas; `None` matches the un-owned / legacy / seed
    /// bucket. This is what stops an `fbrain/Project` registration from
    /// matching (and getting 409'd against) a same-named seed `Project`.
    pub(super) fn find_matching_descriptive_name(
        &self,
        desc_name: &str,
        owner_app_id: Option<&str>,
    ) -> FoldDbResult<(Option<String>, Option<String>, bool)> {
        // 1. Exact match (within the same namespace). The helper rejects
        //    same-key hits whose resolved schema lives in a different
        //    namespace — `descriptive_name_key` is ambiguous on
        //    `(None, "a/b")` vs `(Some("a"), "b")`, and a raw index lookup
        //    would otherwise leak across namespaces.
        if let Some(hash) = self.lookup_descriptive_name_in_namespace(owner_app_id, desc_name)? {
            return Ok((Some(desc_name.to_string()), Some(hash), true));
        }

        // 2. Semantic similarity via embeddings — restricted to the same
        //    `owner_app_id` namespace so an `fbrain/Hiking Trip Reports` can't
        //    fuzzy-match into a seed `Hiking Trip Reports`.
        let query_embedding = match self.embedder.embed_text(desc_name) {
            Ok(vec) => vec,
            Err(e) => {
                tracing::warn!(
                target: "schema_service::schema",
                        "Failed to embed descriptive_name '{}' for similarity search: {}",
                        desc_name,
                        e
                    );
                return Ok((None, None, false));
            }
        };

        // Source of truth for the namespace is the schema's own
        // `owner_app_id` — `parse_canonical_name(existing_key)` is NOT
        // safe to use here because `descriptive_name_key` is non-injective
        // on `(Option<&str>, &str)`: a legacy un-owned schema whose
        // `descriptive_name` contains `/` (e.g. "Vehicles/Sedans") encodes
        // to the same key as `(Some("Vehicles"), "Sedans")`, so a parse
        // would misclassify it as belonging to `Some("Vehicles")` and
        // silently skip it from same-namespace lookups. Iterating schemas
        // first lets us read the actual `owner_app_id`. Same shape as the
        // bug fixed in `resolve_active_schema` and
        // `descriptive_name_conflicts_cross_type` at sibling sites.
        let schemas = read_lock(&self.schemas, "schemas")?;
        let embeddings = read_lock(
            &self.descriptive_name_embeddings,
            "descriptive_name_embeddings",
        )?;

        fn normalize(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        let target_owner = normalize(owner_app_id);

        let mut best_match: Option<(String, f32)> = None;
        for schema in schemas.values() {
            // Superseded schemas are not merge targets — their
            // `descriptive_name_index` slot has already been replaced by
            // the active expansion successor (see `expand_schema`).
            if schema.superseded_by.is_some() {
                continue;
            }
            if crate::builtin_schemas::is_schema_org_leftover(schema) {
                continue;
            }
            if normalize(schema.owner_app_id.as_deref()) != target_owner {
                continue;
            }
            let Some(existing_desc) = schema.descriptive_name.as_deref() else {
                continue;
            };
            let key =
                crate::state::descriptive_name_key(schema.owner_app_id.as_deref(), existing_desc);
            let Some(existing_vec) = embeddings.get(&key) else {
                continue;
            };
            let sim = cosine_similarity(&query_embedding, existing_vec);
            if sim >= DESCRIPTIVE_NAME_SIMILARITY_THRESHOLD
                && best_match
                    .as_ref()
                    .is_none_or(|(_, best_sim)| sim > *best_sim)
            {
                best_match = Some((existing_desc.to_string(), sim));
            }
        }
        drop(schemas);

        if let Some((matched_desc, similarity)) = best_match {
            tracing::info!(
            target: "schema_service::schema",
                "Semantic descriptive_name match: '{}' ≈ '{}' (similarity: {:.3})",
                desc_name,
                matched_desc,
                similarity
            );
            drop(embeddings);
            // Re-resolve through the namespace-aware helper so a
            // matched_desc whose `descriptive_name_key` happens to alias
            // another namespace's entry doesn't return that entry's hash.
            let hash = self.lookup_descriptive_name_in_namespace(owner_app_id, &matched_desc)?;
            return Ok((Some(matched_desc), hash, false));
        }

        Ok((None, None, false))
    }

    /// Check whether two schema names are semantically similar enough to be
    /// considered the same collection. Uses embedding similarity on the
    /// human-readable form of the names (underscores → spaces).
    ///
    /// This acts as a second gate for descriptive_name matching: even if
    /// "Holiday Illustration" ≈ "Famous Paintings" in embedding space, the
    /// schema names `artwork_collection` vs `famous_paintings` should NOT merge.
    pub(super) fn schema_names_are_similar(&self, incoming: &str, existing: &str) -> bool {
        // Exact match (case-insensitive)
        if incoming.eq_ignore_ascii_case(existing) {
            return true;
        }

        // Convert snake_case to readable form for embedding comparison
        let readable_incoming = incoming.replace('_', " ");
        let readable_existing = existing.replace('_', " ");

        let Ok(incoming_emb) = self.embedder.embed_text(&readable_incoming) else {
            return false;
        };
        let Ok(existing_emb) = self.embedder.embed_text(&readable_existing) else {
            return false;
        };

        let sim = cosine_similarity(&incoming_emb, &existing_emb);
        tracing::info!(
            target: "schema_service::schema",
            "Schema name similarity: '{}' vs '{}' = {:.3}",
            incoming,
            existing,
            sim
        );
        // Use a high threshold — schema names are short and precise, so only
        // near-synonyms should match (e.g., "blog_posts" ≈ "blog_articles").
        sim >= 0.85
    }

    /// Compute the Phase B/C diagnostic for one (incoming, existing) pair:
    /// both the structural descriptive_name similarity and the dual-signal
    /// purpose similarity, plus the gate result. Returns `None` when any
    /// required embedding fails — callers treat that as a veto (Phase B
    /// hot path) or as "no near-miss data" (Phase C shadow). The
    /// "embedder failure = veto" rule matches the original
    /// `purpose_signal_passes`.
    pub(crate) fn dual_signal_diagnostic(
        &self,
        incoming: &Schema,
        existing: &Schema,
    ) -> Option<DualSignalDiagnostic> {
        let inc_desc = incoming.descriptive_name.as_deref().unwrap_or("");
        let ex_desc = existing.descriptive_name.as_deref().unwrap_or("");
        let inc_purpose = incoming.purpose_statement.as_deref().unwrap_or("");
        let ex_purpose = existing.purpose_statement.as_deref().unwrap_or("");

        // Structural signal — descriptive_name cosine. Exact-match
        // short-circuit avoids the embedder trip and matches the
        // exact-match arm in `find_matching_descriptive_name`.
        let struct_similarity = if inc_desc.eq_ignore_ascii_case(ex_desc) {
            1.0
        } else {
            let inc_vec = self.embedder.embed_text(inc_desc).ok()?;
            let ex_vec = self.embedder.embed_text(ex_desc).ok()?;
            cosine_similarity(&inc_vec, &ex_vec)
        };

        // Purpose signal — identical formula to `purpose_signal_passes`.
        // Identical blobs short-circuit to 1.0 without an embedder trip
        // (cosine of a vector with itself), mirroring the struct-signal
        // exact-match short-circuit above. This keeps the identity-hash
        // dedup seam free of embedding calls on the idempotent re-publish
        // hot path (same name, same fields, same purpose).
        let inc_blob = format!("{inc_desc} — {inc_purpose}");
        let ex_blob = format!("{ex_desc} — {ex_purpose}");
        let purpose_similarity = if inc_blob == ex_blob {
            1.0
        } else {
            let inc_vec = self.embedder.embed_text(&inc_blob).ok()?;
            let ex_vec = self.embedder.embed_text(&ex_blob).ok()?;
            cosine_similarity(&inc_vec, &ex_vec)
        };

        Some(DualSignalDiagnostic {
            struct_similarity,
            purpose_similarity,
            allows_merge: purpose_similarity >= PURPOSE_SIMILARITY_THRESHOLD,
        })
    }
}

mod coverage;
mod reuse;
mod tokens;
use tokens::*;
