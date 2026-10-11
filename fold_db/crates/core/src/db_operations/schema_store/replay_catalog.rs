//! Strict schema authority reads for a dependent replay mutation.

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::{Schema, SchemaError, SchemaNameClaim, SchemaState};
use crate::storage::traits::KvStore;
use crate::storage::TypedKvStore;
use std::collections::{HashMap, HashSet};

pub(crate) struct ReplaySchemaCatalog {
    pub(crate) schemas: HashMap<String, Schema>,
    pub(crate) states: HashMap<String, SchemaState>,
    pub(crate) superseded_by: HashMap<String, String>,
    pub(crate) retired_name_claims: HashSet<String>,
}

impl SchemaStore {
    /// Only schema cache authority needs replayable bodies. Retention and
    /// janitor receipts keep their existing physical capture policy.
    pub(crate) fn is_replay_authority_key(namespace: &str, key: &[u8]) -> bool {
        match namespace {
            "schemas" | "schema_superseded_by" => true,
            "schema_states" => !other_schema_metadata_prefixes()
                .iter()
                .any(|prefix| key.starts_with(prefix.as_bytes())),
            _ => false,
        }
    }

    /// Boot can skip unreadable metadata; a required replay dependency cannot.
    /// Complete every read before the caller replaces any cached authority.
    pub(crate) async fn load_replay_catalog(&self) -> Result<ReplaySchemaCatalog, SchemaError> {
        let (schemas, state_rows, supersession_rows) = tokio::try_join!(
            self.get_all_schemas_strict(),
            self.replay_catalog_rows(&self.schema_states_store),
            self.replay_catalog_rows(&self.superseded_by_store),
        )?;
        let mut states = HashMap::new();
        let mut retired_name_claims = HashSet::new();
        for (key, bytes) in state_rows {
            if let Some(name) = key.strip_prefix(NAME_CLAIM_KEY_PREFIX) {
                if name.is_empty() {
                    return Err(SchemaError::InvalidData(
                        "empty replay schema name claim".into(),
                    ));
                }
                let claim: SchemaNameClaim = self.decode_replay_catalog(&bytes)?;
                if claim.retired {
                    retired_name_claims.insert(name.to_string());
                }
            } else if !is_other_schema_metadata(&key) {
                states.insert(key, self.decode_replay_catalog::<SchemaState>(&bytes)?);
            }
        }
        let mut superseded_by = HashMap::new();
        for (key, bytes) in supersession_rows {
            superseded_by.insert(key, self.decode_replay_catalog::<String>(&bytes)?);
        }
        Ok(ReplaySchemaCatalog {
            schemas,
            states,
            superseded_by,
            retired_name_claims,
        })
    }

    async fn replay_catalog_rows(
        &self,
        store: &TypedKvStore<dyn KvStore>,
    ) -> Result<Vec<(String, Vec<u8>)>, SchemaError> {
        let rows = store
            .inner()
            .scan_prefix_partition_undecryptable(b"")
            .await?;
        if !rows.undecryptable.is_empty() {
            return Err(SchemaError::InvalidData(
                "unreadable replay schema authority".into(),
            ));
        }
        rows.rows
            .into_iter()
            .map(|(key, bytes)| {
                let key = String::from_utf8(key).map_err(|_| {
                    SchemaError::InvalidData("non-UTF-8 replay schema authority key".into())
                })?;
                Ok((key, bytes))
            })
            .collect()
    }

    fn decode_replay_catalog<T: serde::de::DeserializeOwned>(
        &self,
        bytes: &[u8],
    ) -> Result<T, SchemaError> {
        decode_catalog_json(bytes, self.catalog_unwrap_key.as_ref()).map_err(|_| {
            SchemaError::InvalidData("unreadable replay schema authority value".into())
        })
    }
}

fn is_other_schema_metadata(key: &str) -> bool {
    other_schema_metadata_prefixes()
        .iter()
        .any(|prefix| key.starts_with(prefix))
}

fn other_schema_metadata_prefixes() -> [&'static str; 5] {
    [
        RETENTION_POLICY_KEY_PREFIX,
        RETENTION_HASH_PARTITION_KEY_PREFIX,
        RETENTION_AGE_KEY_PREFIX,
        RETENTION_AGE_LATEST_PREFIX,
        SCHEMA_DROP_RECEIPT_KEY_PREFIX,
    ]
}
