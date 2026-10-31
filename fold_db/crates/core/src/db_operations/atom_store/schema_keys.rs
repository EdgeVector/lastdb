//! Ready core membership selects keys; unknown current tips fetch in batches.

use super::ports::{KeyCodecs, NativeRuntime, PerKeyReads};
use super::types::PerKeyRecord;
use crate::atom::molecule_key_codec;
use crate::resident::core_keys::{CoreKeyLease, CoreStoredKey};
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use std::sync::Arc;

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
        key: &CoreStoredKey,
        api: Option<(&str, &crate::atom::MoleculeKeyCodec)>,
    ) -> Result<Self, SchemaError> {
        let base = molecule_key_codec::hash_range_record_key(molecule, &key.hash, &key.range);
        let api_key = api
            .map(|(hash, codec)| -> Result<KeyValue, SchemaError> {
                let range = codec.api_range(&key.range).map_err(key_error)?;
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

impl<S: SchemaKeyPorts> SchemaKeys<'_, S> {
    fn schema_keys_runtime(&self) -> Option<Arc<PackRuntime>> {
        self.store.native_runtime()
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

/// The ports the schema keys concept reads through.
pub(crate) trait SchemaKeyPorts: KeyCodecs + NativeRuntime + PerKeyReads {}

impl<T: KeyCodecs + NativeRuntime + PerKeyReads> SchemaKeyPorts for T {}

/// Ready key membership and exact query tips over the ports of one store.
pub(crate) struct SchemaKeys<'s, S> {
    store: &'s S,
}

impl<'s, S: SchemaKeyPorts> SchemaKeys<'s, S> {
    pub(crate) fn new(store: &'s S) -> Self {
        Self { store }
    }
}

fn key_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("complete molecule keys: {error}"))
}
