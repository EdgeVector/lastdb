//! Ready core membership selects keys; unknown current tips fetch in batches.

use super::types::PerKeyRecord;
use super::AtomStore;
use crate::atom::molecule_key_codec;
use crate::resident::core_keys::{CoreKeyLease, CoreStoredKey};
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use std::sync::Arc;

mod captured;
mod hash_window;
mod missing;
mod page;
mod prefix;
mod query;
mod rows;

type TipRow = (String, PerKeyRecord);

struct ReadyTipKey {
    molecule: String,
    base: String,
    api_key: Option<KeyValue>,
}

impl ReadyTipKey {
    fn new(
        molecule: &str,
        key: CoreStoredKey,
        api: Option<(&str, &crate::atom::MoleculeKeyCodec)>,
    ) -> Result<Self, SchemaError> {
        let base = molecule_key_codec::hash_range_record_key(molecule, &key.hash, &key.range);
        let api_key = api
            .map(|(hash, codec)| -> Result<KeyValue, SchemaError> {
                let range = codec.api_range(molecule, &key.range).map_err(key_error)?;
                Ok(KeyValue::new(Some(hash.to_string()), Some(range)))
            })
            .transpose()?;
        Ok(Self {
            molecule: molecule.to_string(),
            base,
            api_key,
        })
    }
}

impl AtomStore {
    fn schema_keys_runtime(&self) -> Option<Arc<PackRuntime>> {
        self.namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
    }

    fn ready_key_lease(
        &self,
        runtime: &Arc<PackRuntime>,
        scope: Option<&str>,
        molecule: &str,
    ) -> Option<CoreKeyLease> {
        runtime.schema_key_lease(scope, molecule)
    }
}

fn key_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("complete molecule keys: {error}"))
}
