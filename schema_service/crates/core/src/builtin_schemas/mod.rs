//! Boot seed for built-in schemas.
//!
//! Cold start does not install starter templates. Canonical field seed is
//! separate and still runs.
//!
//! `seed(&state)` still walks `all_phase_1_schemas()`. That list is empty,
//! so boot adds nothing. An identity that is already stored stays stored.

mod seed;

pub use seed::seed;

use schema_types::Schema;
use schema_types::SchemaSource;

pub const TEMPLATE_OWNER_APP_ID: &str = "templates";

pub fn is_app_starter_template(schema: &Schema) -> bool {
    schema.source == SchemaSource::StarterSeed
        && schema.owner_app_id.as_deref() == Some(TEMPLATE_OWNER_APP_ID)
}

/// Schema.org type dump leftover: `starter_seed` that is not an app template.
/// These must not appear in default available / resolve (Tom 2026-08-17).
pub fn is_schema_org_leftover(schema: &Schema) -> bool {
    schema.source == SchemaSource::StarterSeed && !is_app_starter_template(schema)
}

// ────────────────────────────────────────────────────────────────────
//  Registration order
// ────────────────────────────────────────────────────────────────────

/// Schemas installed at boot. Empty: cold start adds no schema.
pub fn all_phase_1_schemas() -> Vec<Schema> {
    Vec::new()
}

/// Descriptive names of schemas installed at boot. Empty.
pub const PHASE_1_DESCRIPTIVE_NAMES: &[&str] = &[];
