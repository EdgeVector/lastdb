//! Stateless schema interpretation: `DeclarativeSchemaDefinition` → runtime `Schema`.
//!
//! This is a pure ETL operation with no runtime state dependencies. It can be
//! tested in isolation from the `SchemaCore` cache manager. Extracting this from
//! `SchemaCore` makes it clear that interpretation is a pure transformation and
//! not entangled with persistent cache state or in-memory lookup maps.

use crate::schema::types::{DeclarativeSchemaDefinition, Schema, SchemaError};

/// Stateless interpreter that turns a parsed declarative schema definition
/// into a runtime [`Schema`] with `runtime_fields` populated.
pub struct SchemaInterpreter;

impl SchemaInterpreter {
    /// Convert a declarative schema definition into a runtime [`Schema`].
    ///
    /// This is a pure function: it does not touch the database, the schema
    /// cache, or any other shared state. It only materialises the
    /// `runtime_fields` map on the definition so that downstream query and
    /// mutation code can look fields up by name.
    pub fn interpret(
        mut declarative_schema: DeclarativeSchemaDefinition,
    ) -> Result<Schema, SchemaError> {
        declarative_schema.populate_runtime_fields()?;
        Ok(declarative_schema)
    }
}
