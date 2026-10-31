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

/// The `source` query on `GET /v1/schemas/available`.
///
/// Both routers use this so the three names and the default filter stay one rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvailableSource {
    /// No `source` parameter. Hide Schema.org leftovers. Keep templates.
    Default,
    /// Keep schemas of this source only.
    Exact(SchemaSource),
    /// `source` is present and is not a known name. The list is empty.
    Unknown,
}

impl AvailableSource {
    /// `None` is the default list. A known name is an exact source.
    /// Any other text is [`Self::Unknown`].
    #[must_use]
    pub fn parse(raw: Option<&str>) -> Self {
        match raw {
            None => Self::Default,
            Some("system_seed") => Self::Exact(SchemaSource::SystemSeed),
            Some("starter_seed") => Self::Exact(SchemaSource::StarterSeed),
            Some("user") => Self::Exact(SchemaSource::User),
            Some(_) => Self::Unknown,
        }
    }

    #[must_use]
    pub fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }

    /// Whether this schema stays in the available list for `self`.
    #[must_use]
    pub fn keeps(self, schema: &Schema) -> bool {
        match self {
            Self::Unknown => false,
            Self::Exact(wanted) => schema.source == wanted,
            Self::Default => !is_schema_org_leftover(schema),
        }
    }
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
