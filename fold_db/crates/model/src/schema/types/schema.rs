//! Schema type aliases. `DeclarativeSchemaType` is owned by schema_types;
//! `Schema` remains the fold_db declarative definition (with runtime_fields).

pub use crate::schema::types::declarative_schemas::DeclarativeSchemaDefinition as Schema;
pub use schema_types::DeclarativeSchemaType;
