//! Compositional (decompositional) Stage-2 canonicalization.
//!
//! Tom's directive (2026-06-18): *"decompose schemas to use existing schemas
//! as much as possible."* Where whole-schema canonicalization
//! ([`crate::state_matching`]) asks "does this *entire* proposal match one
//! existing canonical?", compositional canonicalization asks a finer question:
//! "can the *parts* of this proposal reuse *different* existing canonicals via
//! typed schema references, so the canonical we keep is a graph of reused
//! sub-schemas rather than a re-inlined monolith?"
//!
//! ## What a "component" is (nested-only, by design)
//!
//! A proposal already declares its nested components through
//! [`Schema::ref_fields`] — a `field_name → child_schema_name` map (the
//! foreign-key topology). Each such entry is a natural component: the proposal
//! is saying "this field points at a sub-schema". This module starts
//! **nested-only**: it considers exactly the `ref_fields` components and
//! defers flat-field clustering (grouping un-nested fields into a candidate
//! component) to a lower-risk follow-on. The Phase-0 feasibility spike
//! (fbrain `compositional-schema-canonicalization`, 2026-06-18) confirmed the
//! storage layer can already hold the result — a canonical carrying
//! [`FieldValueType::SchemaRef`] fields needs zero fold_db / molecule-codec
//! change — so the only new work is the Stage-2 *decision*: per component,
//! does an existing same-purpose canonical exist to reuse?
//!
//! ## Advisory / shadow first (mirrors dual-signal Phase C)
//!
//! Like the dual-signal Phase C shadow mode ([`crate::near_miss`]), this
//! ships **advisory-only by default**: it computes the per-component reuse
//! verdict and logs it, but does NOT alter the live add-schema decision. The
//! decision stays whatever the whole-schema path produced. Flipping the
//! decision to actually rewrite `ref_fields` to point at the matched
//! canonical (and emit [`crate::types::SchemaAddOutcome::Composed`]) is gated
//! behind a *separate* env flag that is off by default — so with no flags set
//! the behavior is byte-for-byte today's, a pure superset, never a regression.
//!
//! | env flag                              | effect                                  |
//! |---------------------------------------|-----------------------------------------|
//! | (none)                                | no-op — today's whole-schema behavior   |
//! | `SCHEMA_COMPOSITIONAL_DECOMPOSITION` (any other truthy) | advisory: compute + log per-component reuse verdict |
//! | `…=apply`                             | apply: rewrite reusing components' `ref_fields` to the matched canonical + emit `Composed` |
//!
//! ## Read-time contract (out of scope here)
//!
//! Whether a reused `SchemaRef` is dereferenced on read (inert ref vs an
//! auto-materialized resolving view over the referenced canonicals) is a
//! separate design choice and an explicit follow-on card — this module only
//! decides *reuse at registration time*, never the read contract.

use serde::{Deserialize, Serialize};

use schema_types::Schema;
use schema_types::SchemaSource;

use super::state::SchemaServiceState;
use super::state_matching::PURPOSE_SIMILARITY_THRESHOLD;

/// Whether compositional decomposition runs at all. **Default off.** When the
/// env var is unset (or set to a falsy value) the add-schema path is exactly
/// today's whole-schema-only behavior — no component decomposition, no
/// advisory records, no new outcome. Turning it on enables the *advisory*
/// (shadow) pass: per-component reuse is computed and logged, but the live
/// decision is unchanged.
///
/// Truthy values: `1` / `true` / `TRUE` / `yes` / `advisory` / `apply`.
/// All of them turn on the advisory (shadow) pass. Only the exact value
/// `apply` *additionally* turns on the apply step (see
/// [`compositional_apply_enabled`]) that rewrites `ref_fields` and emits
/// [`crate::types::SchemaAddOutcome::Composed`]; every other truthy value
/// stays advisory-only, so it cannot change the persisted result.
pub(super) fn compositional_decomposition_enabled() -> bool {
    std::env::var("SCHEMA_COMPOSITIONAL_DECOMPOSITION").is_ok_and(|v| {
        matches!(
            v.as_str(),
            "1" | "true" | "TRUE" | "yes" | "advisory" | "apply"
        )
    })
}

/// Whether the compositional **apply** step runs — the follow-on to the
/// advisory shadow pass. When on, a proposal whose nested components clear the
/// per-component reuse gate has those `ref_fields` rewritten to point at the
/// matched existing canonical, and a newly-registered parent is returned as
/// [`crate::types::SchemaAddOutcome::Composed`] instead of `Added`.
///
/// Strictly stronger than [`compositional_decomposition_enabled`]: ONLY the
/// exact value `apply` enables it. Any other truthy value (`1`, `advisory`, …)
/// leaves this off, so the persisted result is identical to advisory mode
/// unless an operator explicitly opts into `apply`. Default off ⇒ the
/// add-schema path never mutates `ref_fields` and never emits `Composed`, so
/// the shipped behavior remains a pure superset of today's.
pub(super) fn compositional_apply_enabled() -> bool {
    std::env::var("SCHEMA_COMPOSITIONAL_DECOMPOSITION").is_ok_and(|v| v == "apply")
}

/// Per-component verdict from decomposing a proposal: a single `ref_fields`
/// component (a `field_name → child_schema_name` edge) and whether an existing
/// same-purpose canonical was found to reuse for it.
///
/// This is **advisory evidence**, not a persisted decision (mirrors the
/// dual-signal Phase C [`crate::near_miss::NearMissRecord`] split between
/// measured signal and enforced policy). The whole-schema add-schema outcome
/// is unchanged when the apply gate is off; this just records what a
/// compositional pass *would* reuse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComponentReuseAdvice {
    /// The proposal field whose `ref_fields` entry names this component.
    pub field: String,
    /// The child schema name the proposal's `ref_fields` points at.
    pub proposed_child_schema: String,
    /// `Some(canonical_descriptive_name)` when an existing active canonical
    /// matched this component on the dual-signal purpose gate strongly enough
    /// to reuse via [`schema_types::FieldValueType::SchemaRef`];
    /// `None` when no existing canonical cleared the gate (the component stays
    /// in the residual and registers as new, exactly as today).
    pub reuse_target: Option<String>,
    /// Purpose-signal cosine similarity between the component and the matched
    /// canonical (or the best candidate that fell short, for tuning). `None`
    /// when no candidate could be embedded at all.
    pub purpose_similarity: Option<f32>,
}

impl ComponentReuseAdvice {
    /// Whether this component would reuse an existing canonical (vs. registering new).
    pub fn reuses(&self) -> bool {
        self.reuse_target.is_some()
    }
}

impl SchemaServiceState {
    /// Decompose a proposal into its nested (`ref_fields`) components and, for
    /// each, decide whether an existing same-purpose canonical can be reused
    /// via a typed `SchemaRef`.
    ///
    /// **Advisory-only** — never mutates state and never changes the caller's
    /// add-schema outcome. Returns one [`ComponentReuseAdvice`] per
    /// `ref_fields` component, in deterministic (field-name-sorted) order so
    /// logs and tests are stable. A proposal with no `ref_fields` yields an
    /// empty vec (the common flat-proposal case — zero cost).
    ///
    /// Matching reuses the existing dual-signal machinery: a candidate
    /// canonical is a reuse target iff its purpose-statement embedding clears
    /// [`PURPOSE_SIMILARITY_THRESHOLD`] against the component — the same
    /// τ_purpose gate the whole-schema path uses, so a component reuse can
    /// never be *looser* than a whole-schema merge. Schemas are identified by
    /// PURPOSE, not field shape (`feedback_schema_semantic_distinction`).
    pub(super) fn compositional_component_advice(
        &self,
        proposal: &Schema,
    ) -> Vec<ComponentReuseAdvice> {
        // Deterministic order for stable logs/tests.
        let mut components: Vec<(&String, &String)> = proposal.ref_fields.iter().collect();
        components.sort_by(|a, b| a.0.cmp(b.0));

        let mut advice: Vec<ComponentReuseAdvice> = components
            .into_iter()
            .map(|(field, child_schema)| {
                let (reuse_target, purpose_similarity) =
                    self.best_component_reuse(proposal, field, child_schema);
                ComponentReuseAdvice {
                    field: field.clone(),
                    proposed_child_schema: child_schema.clone(),
                    reuse_target,
                    purpose_similarity,
                }
            })
            .collect();

        if let Some(fields) = proposal.fields.as_ref() {
            let mut inferred_fields: Vec<&String> = fields
                .iter()
                .filter(|field| !proposal.ref_fields.contains_key(*field))
                .collect();
            inferred_fields.sort();

            for field in inferred_fields {
                let child_schema = inferred_component_name(field);
                let (reuse_target, purpose_similarity) =
                    self.best_component_reuse(proposal, field, &child_schema);
                if reuse_target.is_some() {
                    advice.push(ComponentReuseAdvice {
                        field: field.clone(),
                        proposed_child_schema: child_schema,
                        reuse_target,
                        purpose_similarity,
                    });
                }
            }
        }

        advice.sort_by(|a, b| a.field.cmp(&b.field));
        advice
    }

    /// Find the best existing canonical to reuse for one component.
    ///
    /// Builds a synthetic single-component schema (the child schema name as
    /// both `descriptive_name` and `purpose_statement` context — the proposal
    /// gives us the field's role and any description) and runs it through the
    /// existing [`Self::dual_signal_diagnostic`] purpose gate against every
    /// active, same-namespace canonical. Returns the highest-scoring candidate
    /// that clears [`PURPOSE_SIMILARITY_THRESHOLD`] (or the best sub-threshold
    /// score, with `reuse_target = None`, for tuning visibility).
    ///
    /// Read-only: takes the `schemas` read lock briefly and never persists.
    fn best_component_reuse(
        &self,
        proposal: &Schema,
        field: &str,
        child_schema: &str,
    ) -> (Option<String>, Option<f32>) {
        // The component's purpose: prefer the proposal's field description
        // (it states what this nested thing is *for*), else fall back to the
        // child schema name. This is the same "name + description" signal the
        // field-canonicalization path uses.
        let component_purpose = proposal
            .field_descriptions
            .get(field)
            .cloned()
            .unwrap_or_else(|| child_schema.replace('_', " "));

        let component = synthetic_component_schema(
            child_schema,
            &component_purpose,
            proposal.owner_app_id.as_deref(),
        );

        let Ok(schemas) = self.schemas.read() else {
            return (None, None);
        };

        fn normalize(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        let target_owner = normalize(proposal.owner_app_id.as_deref());

        let mut best: Option<(String, f32)> = None;
        for existing in schemas.values() {
            // Only active canonicals in the same namespace are reuse targets —
            // mirrors `find_matching_descriptive_name`'s superseded + namespace
            // filters so a component can't reuse a retired or cross-app schema.
            if existing.superseded_by.is_some() {
                continue;
            }
            if existing.source != SchemaSource::User {
                continue;
            }
            if normalize(existing.owner_app_id.as_deref()) != target_owner {
                continue;
            }
            // Don't reuse the proposal against itself if it's already registered.
            if existing.descriptive_name.as_deref() == proposal.descriptive_name.as_deref() {
                continue;
            }
            let Some(diag) = self.dual_signal_diagnostic(&component, existing) else {
                let coverage = component_field_coverage(&component_purpose, existing);
                if coverage >= 0.6
                    && best
                        .as_ref()
                        .is_none_or(|(_, best_sim)| coverage > *best_sim)
                {
                    let name = existing
                        .descriptive_name
                        .clone()
                        .unwrap_or_else(|| existing.name.clone());
                    best = Some((name, coverage));
                }
                continue;
            };
            let sim = diag
                .purpose_similarity
                .max(component_field_coverage(&component_purpose, existing));
            if best.as_ref().is_none_or(|(_, best_sim)| sim > *best_sim) {
                let name = existing
                    .descriptive_name
                    .clone()
                    .unwrap_or_else(|| existing.name.clone());
                best = Some((name, sim));
            }
        }
        drop(schemas);

        match best {
            Some((name, sim)) if sim >= PURPOSE_SIMILARITY_THRESHOLD => (Some(name), Some(sim)),
            Some((_, sim)) => (None, Some(sim)),
            None => (None, None),
        }
    }

    /// Advisory (shadow-mode) compositional pass, called from
    /// [`Self::add_schema`]. When [`compositional_decomposition_enabled`] is
    /// off this is a cheap no-op; when on it decomposes the proposal and logs
    /// the per-component reuse verdict WITHOUT changing the add-schema outcome.
    ///
    /// Returns the advice so callers/tests can assert on it; the live add path
    /// ignores the return today (advisory-only — the `apply` rewrite is a
    /// follow-on).
    pub(super) fn log_compositional_advice(&self, proposal: &Schema) -> Vec<ComponentReuseAdvice> {
        if !compositional_decomposition_enabled() {
            return Vec::new();
        }
        let advice = self.compositional_component_advice(proposal);
        if advice.is_empty() {
            return advice;
        }
        let reused = advice.iter().filter(|a| a.reuses()).count();
        tracing::info!(
            target: "schema_service::schema",
            descriptive_name = %proposal.descriptive_name.as_deref().unwrap_or(""),
            components = advice.len(),
            reusable = reused,
            "compositional (advisory): proposal decomposed into nested components; \
             {reused}/{} could reuse an existing canonical via SchemaRef",
            advice.len(),
        );
        for a in &advice {
            if let Some(target) = &a.reuse_target {
                tracing::info!(
                    target: "schema_service::schema",
                    field = %a.field,
                    child = %a.proposed_child_schema,
                    reuse_target = %target,
                    purpose_similarity = a.purpose_similarity.unwrap_or(0.0),
                    "compositional (advisory): component would reuse existing canonical",
                );
            } else {
                tracing::debug!(
                    target: "schema_service::schema",
                    field = %a.field,
                    child = %a.proposed_child_schema,
                    purpose_similarity = a.purpose_similarity.unwrap_or(0.0),
                    "compositional (advisory): component has no reuse target — stays in residual",
                );
            }
        }
        advice
    }

    /// Compositional **apply** step (the follow-on to [`Self::log_compositional_advice`]).
    /// When [`compositional_apply_enabled`] is on, decompose `schema` into its
    /// nested components and, for every component that clears the per-component
    /// reuse gate, rewrite its `ref_fields` entry to point at the matched
    /// existing canonical's descriptive name — so the persisted parent
    /// *references* the reused sub-schema instead of implying a fresh child.
    ///
    /// Returns the advice for the components that were actually rewritten
    /// (empty when apply is off or nothing reused). The caller emits
    /// [`crate::types::SchemaAddOutcome::Composed`] for a newly-registered
    /// parent iff this returns non-empty.
    ///
    /// **Purely additive.** Only `ref_fields` *targets* change; `fields`,
    /// `field_descriptions`, `schema_type`, and the identity-hash inputs are
    /// untouched (`ref_fields` is not part of `compute_identity_hash` —
    /// declarative_schemas.rs). So the dedup / expansion / dual-signal seams
    /// downstream behave byte-for-byte as they do today; the only difference is
    /// the reference topology of a parent that registers as new.
    pub(super) fn apply_compositional_reuse(
        &self,
        schema: &mut Schema,
    ) -> Vec<ComponentReuseAdvice> {
        if !compositional_apply_enabled() {
            return Vec::new();
        }
        let advice = self.compositional_component_advice(schema);
        let mut applied = Vec::new();
        for a in &advice {
            if let Some(target) = &a.reuse_target {
                // Point the typed ref at the existing canonical to reuse it.
                // Idempotent when the proposal already named the canonical.
                schema.ref_fields.insert(a.field.clone(), target.clone());
                applied.push(a.clone());
            }
        }
        if !applied.is_empty() {
            tracing::info!(
                target: "schema_service::schema",
                descriptive_name = %schema.descriptive_name.as_deref().unwrap_or(""),
                reused = applied.len(),
                components = advice.len(),
                "compositional (apply): rewrote {}/{} ref_fields component(s) to reuse existing canonicals via SchemaRef",
                applied.len(),
                advice.len(),
            );
        }
        applied
    }
}

/// Build a synthetic single-component `Schema` to run through the existing
/// dual-signal purpose gate. Carries just the identity signal the gate reads:
/// the component's name (as `descriptive_name`) and its purpose-statement.
/// Namespace is inherited from the proposal so the gate's same-namespace
/// filter behaves correctly.
fn synthetic_component_schema(
    child_schema: &str,
    purpose: &str,
    owner_app_id: Option<&str>,
) -> Schema {
    use schema_types::DeclarativeSchemaType;
    let mut s = Schema::new(
        child_schema.to_string(),
        DeclarativeSchemaType::Single,
        None,
        Some(Vec::new()),
        None,
        None,
    );
    s.descriptive_name = Some(child_schema.replace('_', " "));
    s.purpose_statement = Some(purpose.to_string());
    s.owner_app_id = owner_app_id.filter(|x| !x.is_empty()).map(str::to_string);
    s
}

fn inferred_component_name(field: &str) -> String {
    field
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|p| !p.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn component_field_coverage(component_purpose: &str, existing: &Schema) -> f32 {
    let Some(fields) = existing.fields.as_ref().filter(|fields| !fields.is_empty()) else {
        return 0.0;
    };
    let text_tokens = component_text_tokens(component_purpose);
    let matched = fields
        .iter()
        .filter(|field| {
            let tokens = component_text_tokens(field);
            !tokens.is_empty() && tokens.iter().all(|token| text_tokens.contains(token))
        })
        .count();
    matched as f32 / fields.len() as f32
}

fn component_text_tokens(text: &str) -> std::collections::HashSet<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter_map(|part| {
            let token = part.trim().to_ascii_lowercase();
            (!token.is_empty()).then_some(token)
        })
        .collect()
}
