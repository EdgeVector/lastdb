//! Empty retained-source roots use exact account authority before any purge hold.

use super::super::AuthClient;
use crate::sync::cloud_purge::inventory::bounded_json;
use crate::sync::cloud_purge::protocol::{CloudAccount, PreparedCloudSource};
use crate::sync::{SyncError, SyncResult};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeSet;

#[derive(Deserialize)]
struct RegisteredSourceRoots {
    account: CloudAccount,
    scope_version: u64,
    registered_roots: Vec<String>,
}

impl AuthClient {
    pub async fn cloud_source_root_register(
        &self,
        account: &CloudAccount,
        operation_id: &str,
        generation: u64,
        source: &PreparedCloudSource,
    ) -> SyncResult<u64> {
        let roots: Vec<_> = source
            .roots
            .iter()
            .map(|root| {
                json!({"logical_root":root.logical_root,
                "format":root.format,"store_uuid":root.store_uuid})
            })
            .collect();
        bounded_json(&roots, 160_000)?;
        let expected: BTreeSet<_> = source
            .roots
            .iter()
            .map(|root| root.logical_root.as_str())
            .collect();
        if roots.is_empty() || expected.len() != roots.len() {
            return Err(SyncError::Storage("invalid retained source roots".into()));
        }
        let response: RegisteredSourceRoots = self
            .purge_call(
                json!({"account":account,"cloud_operation_id":operation_id,
                    "source_id":source.source_id,"cloud_generation":generation,"roots":roots}),
                "cloud_source_root_register",
            )
            .await?;
        let actual: BTreeSet<_> = response
            .registered_roots
            .iter()
            .map(String::as_str)
            .collect();
        if response.account != *account
            || actual != expected
            || actual.len() != response.registered_roots.len()
        {
            return Err(SyncError::Storage(
                "registered cloud source roots differ from the retained source".into(),
            ));
        }
        Ok(response.scope_version)
    }
}
