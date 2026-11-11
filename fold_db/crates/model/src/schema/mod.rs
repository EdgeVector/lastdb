//! Schema model types and the pure logical layout rules.

pub mod logical_rules;
pub mod types;

pub use logical_rules::{require_logical_filter, require_logical_layout};
pub use types::{Schema, SchemaError};
