//! Bounded cleanup of markers that cannot hide an immutable generation slot.

use super::counter::TipChangeSet;
use super::helpers::validate_scope;
use super::ports::{
    CommitTips, GenerationLocks, MainRows, MoleculeReads, NativeRuntime, WriteScopes,
};
use crate::atom::molecule_key_codec;
use crate::schema::{types::field::build_storage_key, SchemaError};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use std::sync::Arc;

/// The ports the marker cleanup concept uses.
pub(crate) trait MarkerCleanupPorts:
    CommitTips + GenerationLocks + MainRows + MoleculeReads + NativeRuntime + WriteScopes
{
}

impl<T> MarkerCleanupPorts for T where
    T: CommitTips + GenerationLocks + MainRows + MoleculeReads + NativeRuntime + WriteScopes
{
}

/// Delete-marker cleanup over the ports of one store.
pub(crate) struct MarkerCleanup<'s, S> {
    store: &'s S,
}

impl<'s, S: MarkerCleanupPorts> MarkerCleanup<'s, S> {
    pub(crate) fn new(store: &'s S) -> Self {
        Self { store }
    }

    /// One personal or scoped molecule, at most 256 marker keys. Preview is
    /// read-only. Active generations and in-flight generation cuts are retained.
    /// A deleted cursor remains valid: continuation uses its exclusive bound.
    pub async fn cleanup_delete_markers_page(
        &self,
        molecule: &str,
        scope: Option<&str>,
        after_key: Option<&str>,
        limit: usize,
        execute: bool,
    ) -> Result<Value, SchemaError> {
        validate_scope(scope)?;
        if molecule.is_empty()
            || !molecule
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return Err(SchemaError::InvalidData("invalid molecule identity".into()));
        }
        let runtime = self.store.native_runtime().ok_or_else(|| {
            SchemaError::InvalidData("delete-marker cleanup requires native storage".into())
        })?;
        if crate::storage::disk_pack::transaction::publication::active(Some(Arc::clone(&runtime))) {
            return Err(SchemaError::InvalidData(
                "delete-marker cleanup requires its own native request scope".into(),
            ));
        }
        let prefix = build_storage_key(
            scope,
            &molecule_key_codec::molecule_generation_delete_prefix(molecule),
        );
        let after = after_key
            .map(|token| URL_SAFE_NO_PAD.decode(token))
            .transpose()
            .map_err(|_| SchemaError::InvalidData("invalid marker cursor".into()))?;
        if after
            .as_ref()
            .is_some_and(|key| !key.starts_with(prefix.as_bytes()))
        {
            return Err(SchemaError::InvalidData(
                "cursor belongs to another molecule or scope".into(),
            ));
        }
        self.store
            .in_background_scope(async {
                let lock = self.store.generation_cleanup_lock(molecule, scope);
                let Ok(guard) = lock.try_write_owned() else {
                    return Ok(
                        json!({"blocked": "generation_preparation", "complete": false,
                    "examined": 0, "eligible": 0, "deleted": 0, "next_cursor": after_key}),
                    );
                };
                if self
                    .store
                    .active_molecule_generation(molecule, scope)
                    .await?
                    .is_some()
                {
                    return Ok(json!({"blocked": "active_generation", "complete": false,
                    "examined": 0, "eligible": 0, "deleted": 0, "next_cursor": after_key}));
                }
                // Private preparation ends before publication. Keep the exclusion
                // until this standalone native batch publishes.
                crate::storage::disk_pack::transaction::retain(&runtime, guard)?;
                self.cleanup_unselected_markers(
                    &prefix,
                    scope,
                    after.as_deref(),
                    limit.clamp(1, 256),
                    execute,
                )
                .await
            })
            .await
    }

    async fn cleanup_unselected_markers(
        &self,
        prefix: &str,
        scope: Option<&str>,
        after_key: Option<&[u8]>,
        limit: usize,
        execute: bool,
    ) -> Result<Value, SchemaError> {
        let (start, end) = crate::kind_partition::exact_prefix_bounds(prefix);
        let mut start = start.into_bytes();
        if let Some(after) = after_key {
            start = after.to_vec();
            start.push(0);
        }
        let rows = self
            .store
            .rows()
            .inner()
            .scan_range_paged(&start, end.as_bytes(), limit)
            .await?;
        if rows.len() > limit
            || rows.windows(2).any(|p| p[0].0 >= p[1].0)
            || rows.iter().any(|(key, _)| {
                key.as_slice() < start.as_slice() || key.as_slice() >= end.as_bytes()
            })
        {
            return Err(SchemaError::InvalidData(
                "invalid delete-marker page".into(),
            ));
        }
        let cursor = rows.last().map(|(key, _)| URL_SAFE_NO_PAD.encode(key));
        let count = rows.len();
        if execute && count != 0 {
            self.store
                .commit_tip_changes(TipChangeSet {
                    scope,
                    deletes: rows.into_iter().map(|(key, _)| key).collect(),
                    ..TipChangeSet::default()
                })
                .await?;
        }
        Ok(
            json!({"blocked": null, "complete": count == 0, "examined": count,
            "eligible": count, "deleted": if execute {count} else {0},
            "next_cursor": cursor, "execute": execute}),
        )
    }
}
