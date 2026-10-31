//! Bounded cleanup of markers that cannot hide an immutable generation slot.

use super::{counter::TipChangeSet, AtomStore};
use crate::atom::molecule_key_codec;
use crate::schema::{types::field::build_storage_key, SchemaError};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;

impl AtomStore {
    pub(crate) fn retain_generation_cleanup_guard(
        &self,
        guard: Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
    ) -> Result<(), SchemaError> {
        if let Some(runtime) = self
            .namespaced_store
            .as_ref()
            .and_then(|s| s.disk_pack_runtime())
        {
            if crate::storage::disk_pack::transaction::publication::active(Some(Arc::clone(
                &runtime,
            ))) {
                crate::storage::disk_pack::transaction::retain(&runtime, guard)?;
            }
        }
        Ok(())
    }

    /// Normal tips never take this lock. A prepared generation keeps a shared
    /// guard until activation and reclamation end. Cleanup only tries an
    /// exclusive guard after native admission; it never waits for a preparation
    /// that might itself need native admission.
    pub(crate) fn generation_cleanup_lock(
        &self,
        molecule: &str,
        scope: Option<&str>,
    ) -> Arc<RwLock<()>> {
        let key = format!("{}\u{1f}{molecule}", scope.unwrap_or_default());
        let mut locks = self
            .generation_cleanup_locks
            .lock()
            .expect("generation cleanup locks");
        if locks.len() >= 4096 {
            locks.retain(|_, lock| lock.strong_count() != 0);
        }
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(RwLock::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        lock
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
        Self::validate_scope(scope)?;
        if molecule.is_empty()
            || !molecule
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return Err(SchemaError::InvalidData("invalid molecule identity".into()));
        }
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|s| s.disk_pack_runtime())
            .ok_or_else(|| {
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
        self.in_background_scope(async {
            let lock = self.generation_cleanup_lock(molecule, scope);
            let Ok(guard) = lock.try_write_owned() else {
                return Ok(
                    json!({"blocked": "generation_preparation", "complete": false,
                    "examined": 0, "eligible": 0, "deleted": 0, "next_cursor": after_key}),
                );
            };
            if self
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
        let (start, end) = lastdb_model::kind_partition::exact_prefix_bounds(prefix);
        let mut start = start.into_bytes();
        if let Some(after) = after_key {
            start = after.to_vec();
            start.push(0);
        }
        let rows = self
            .main_store
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
            self.commit_tip_changes(TipChangeSet {
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
