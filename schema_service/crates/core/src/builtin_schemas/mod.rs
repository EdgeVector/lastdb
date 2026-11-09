//! Built-in schema classification.
//!
//! Cold start does not install schemas. Canonical field seed is separate
//! and still runs. Starter-template identity stays so Schema.org leftovers
//! stay out of the default available list.

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
