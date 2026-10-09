use std::path::Path;

use crate::schema::SchemaError;

use super::FoldDB;

impl FoldDB {
    /// Load schema from JSON string (creates Available schema)
    pub async fn load_schema_from_json(&self, json_str: &str) -> Result<(), SchemaError> {
        self.schema_manager.load_schema_from_json(json_str).await
    }

    /// Load schema from file (creates Available schema)
    pub async fn load_schema_from_file<P: AsRef<Path>>(&self, path: P) -> Result<(), SchemaError> {
        self.schema_manager.load_schema_from_file(path).await
    }

    /// Compact live keys under one hash onto this schema's record molecule.
    ///
    /// Key discovery is a prefix list of the hash-field molecule. Tests pass
    /// one known hash so the walk stays O(log M) under that partition.
    pub async fn compact_record_molecule(
        &self,
        schema_name: &str,
        hash: &str,
    ) -> Result<crate::record_molecule::CompactReport, SchemaError> {
        crate::record_molecule::compact_record_molecule(self, schema_name, hash).await
    }

    /// Compact one `(hash, range)` onto R. Host-lane proofs use this so a
    /// wide BoardCards hash is not rewritten in one pass.
    pub async fn compact_record_molecule_key(
        &self,
        schema_name: &str,
        hash: &str,
        range: &str,
    ) -> Result<crate::record_molecule::CompactReport, SchemaError> {
        crate::record_molecule::compact_record_molecule_key(self, schema_name, hash, Some(range))
            .await
    }
}
