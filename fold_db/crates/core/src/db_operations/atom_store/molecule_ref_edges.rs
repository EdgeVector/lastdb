use super::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use lastdb_model::hex::hex_lower;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(super) const MOLECULE_REF_PREFIX: &str = "mref:v1:e:";
const MOLECULE_BINDINGS_PREFIX: &str = "mref:v1:n:";
const MOLECULE_REF_DOMAIN: &[u8] = b"lastdb:molecule-ref-edge-source:v1\0";

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

    /// One named database catalog's field path to an existing molecule. The
    /// JSON tuple preserves the database, schema, and field boundaries.
    #[must_use]
    pub fn database_catalog_field(
        db_locator: &str,
        schema: &str,
        field: &str,
        molecule_uuid: &str,
    ) -> Self {
        Self {
            molecule_uuid: molecule_uuid.to_string(),
            source: format!(
                "database:{}",
                serde_json::to_string(&(db_locator, schema, field))
                    .expect("database catalog field identity is serializable")
            ),
            edge_type: "database-catalog-field".to_string(),
        }
    }

    pub(crate) fn storage_key(&self, storage_prefix: Option<&str>) -> String {
        let mut hasher = Sha256::new();
        hasher.update(MOLECULE_REF_DOMAIN);
        hasher.update(self.edge_type.as_bytes());
        hasher.update([0]);
        hasher.update(self.source.as_bytes());
        let source = hex_lower(hasher.finalize());
        build_storage_key(
            storage_prefix,
            &format!(
                "{MOLECULE_REF_PREFIX}{}\0{source}",
                molecule_target(&self.molecule_uuid)
            ),
        )
    }
}

/// The hex token of a molecule in the keys of its binding records.
pub(super) fn molecule_target(molecule_uuid: &str) -> String {
    hex_lower(Sha256::digest(molecule_uuid.as_bytes()))
}

/// The key of the `bindings` counter of one molecule in one scope.
pub(super) fn molecule_bindings_key(molecule_uuid: &str, storage_prefix: Option<&str>) -> String {
    build_storage_key(
        storage_prefix,
        &format!(
            "{MOLECULE_BINDINGS_PREFIX}{}",
            molecule_target(molecule_uuid)
        ),
    )
}

impl AtomStore {
    /// Whether the molecule has at least one binding record in the scope: a range
    /// read of at most one row under one hash.
    pub(super) async fn binding_record_present(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let prefix = build_storage_key(
            storage_prefix,
            &format!("{MOLECULE_REF_PREFIX}{}\0", molecule_target(molecule_uuid)),
        );
        let (start, end) = lastdb_model::kind_partition::exact_prefix_bounds(&prefix);
        let found = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), 1)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read molecule binding records: {error}"))
            })?;
        Ok(!found.is_empty())
    }
}
