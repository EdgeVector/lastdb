//! The edges from a schema field or an alternate key to its molecule. The
//! attribution ledger roots each molecule through them.

use serde::{Deserialize, Serialize};

/// One active source that binds a molecule.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MoleculeRefEdge {
    pub molecule_uuid: String,
    pub source: String,
    pub edge_type: String,
}

impl MoleculeRefEdge {
    /// A schema field source. The JSON tuple preserves both names exactly,
    /// including separator text inside a schema or field name.
    #[must_use]
    pub fn schema_field(schema: &str, field: &str, molecule_uuid: &str) -> Self {
        Self {
            molecule_uuid: molecule_uuid.to_string(),
            source: format!(
                "schema:{}",
                serde_json::to_string(&(schema, field))
                    .expect("schema field identity is serializable")
            ),
            edge_type: "schema-field".to_string(),
        }
    }
}
