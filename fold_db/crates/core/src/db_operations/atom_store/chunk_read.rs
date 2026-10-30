//! Chunked typed reads by key.
//!
//! The native get-many call accepts at most [`VALUE_READ_CHUNK`] keys.

use super::AtomStore;
use crate::schema::SchemaError;
use serde::de::DeserializeOwned;

/// Keys one get-many call accepts, at most.
const VALUE_READ_CHUNK: usize = 10_000;

impl AtomStore {
    /// The stored value of each key, in the order given.
    ///
    /// `failure` is the text before the store error.
    pub(super) async fn read_stored_values<T>(
        &self,
        keys: &[String],
        failure: &'static str,
    ) -> Result<Vec<Option<T>>, SchemaError>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(VALUE_READ_CHUNK) {
            out.extend(
                self.main_store
                    .get_items::<T>(chunk)
                    .await
                    .map_err(|error| SchemaError::InvalidData(format!("{failure}: {error}")))?,
            );
        }
        Ok(out)
    }
}
