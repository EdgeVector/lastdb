//! The atom store answers the schema key preload of the disk pack.

use super::DbOperations;
use crate::atom::MoleculeKeyCodec;
use crate::records::molecule_record::{MoleculeGenerationPointer, PerKeyRecord};
use crate::schema::SchemaError;
use crate::storage::disk_pack::schema_keys::SchemaKeySource;

impl SchemaKeySource for DbOperations {
    async fn load_molecule_key_bundle(&self, molecule: &str) -> Result<(), SchemaError> {
        self.atoms().load_molecule_key_bundle(molecule).await
    }

    fn key_codec_for_molecule(&self, molecule: &str) -> Result<MoleculeKeyCodec, SchemaError> {
        self.atoms().key_codec_for_molecule(molecule)
    }

    async fn active_molecule_generation(
        &self,
        molecule: &str,
        scope: Option<&str>,
    ) -> Result<Option<MoleculeGenerationPointer>, SchemaError> {
        self.atoms()
            .active_molecule_generation(molecule, scope)
            .await
    }

    async fn scan_logical_tip_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        scope: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        self.atoms()
            .scan_logical_tip_page(prefix, after, scope, limit)
            .await
    }

    async fn load_exact_per_key_records(
        &self,
        scope: Option<&str>,
        keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        self.atoms().load_exact_per_key_records(scope, keys).await
    }
}
