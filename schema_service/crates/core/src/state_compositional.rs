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
//! [`FieldValueType::SchemaRef`] fields needs zero fold_db / field-key-codec
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

mod advice;
