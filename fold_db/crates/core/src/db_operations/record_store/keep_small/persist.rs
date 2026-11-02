use super::*;

impl RecordStore {
    /// The shutdown flush: stamps the snapshot `clean_stop = true`, so the
    /// next boot hydrates it as exact without a bootstrap. Call it after every
    /// writer has stopped. Always writes: the stamp is the verdict.
    pub async fn flush_keep_small_for_clean_stop(&self) -> Result<(), crate::schema::SchemaError> {
        let Some(store) = self.keep_small_persist.as_ref() else {
            return Ok(());
        };
        let _persist_guard = self.keep_small_persist_lock.lock().await;
        self.write_keep_small_with_clean_stop_locked(true).await?;
        store.inner().flush().await.map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("flush keep-small: {e}"))
        })?;
        Ok(())
    }

    /// Unconditional durable write of the projection with an explicit
    /// clean-stop stamp. Only the shutdown flush passes `true`.
    pub(super) async fn write_keep_small_with_clean_stop(
        &self,
        clean_stop: bool,
    ) -> Result<(), crate::schema::SchemaError> {
        if self.keep_small_persist.is_none() {
            return Ok(());
        }
        let _persist_guard = self.keep_small_persist_lock.lock().await;
        self.write_keep_small_with_clean_stop_locked(clean_stop)
            .await
    }

    pub(super) async fn write_keep_small_with_clean_stop_locked(
        &self,
        clean_stop: bool,
    ) -> Result<(), crate::schema::SchemaError> {
        let mut snapshot = self.keep_small.export();
        snapshot.clean_stop = Some(clean_stop);
        if !clean_stop && snapshot.trust.global.state.is_complete() {
            snapshot.trust.global = MeterDomainTrust::incomplete("dirty_snapshot");
            for domain in snapshot.trust.schemas.values_mut() {
                *domain = MeterDomainTrust::incomplete("dirty_snapshot");
            }
        }
        let dirty = self.keep_small.take_dirty_schemas();
        self.write_keep_small_header_and_shards(&snapshot, Some(&dirty))
            .await?;
        // The caller flushes.
        Ok(())
    }

    pub(super) async fn merge_keep_small_shards(
        &self,
        store: &TypedKvStore<dyn KvStore>,
        snapshot: &mut crate::db_operations::KeepSmallSnapshot,
    ) -> Result<(), crate::schema::SchemaError> {
        let shards = store
            .scan_items_with_prefix::<KeepSmallSchemaShard>(
                crate::db_operations::KEEP_SMALL_SCHEMA_SHARD_PREFIX,
            )
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("hydrate keep-small shards: {e}"))
            })?;
        for (_key, shard) in shards {
            snapshot.merge_shard(shard);
        }
        Ok(())
    }

    pub(in crate::db_operations::record_store) async fn write_keep_small_header_and_shards(
        &self,
        snapshot: &crate::db_operations::KeepSmallSnapshot,
        only_schemas: Option<&HashSet<String>>,
    ) -> Result<(), crate::schema::SchemaError> {
        let Some(store) = self.keep_small_persist.as_ref() else {
            return Ok(());
        };
        let (header, shards) = snapshot.clone().into_header_and_shards(only_schemas);
        let mut items = Vec::with_capacity(shards.len() + 1);
        items.push((
            crate::db_operations::KEEP_SMALL_SNAPSHOT_KEY
                .as_bytes()
                .to_vec(),
            serde_json::to_vec(&header).map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("serialize keep-small header: {e}"))
            })?,
        ));
        for (key, shard) in shards {
            items.push((
                key.into_bytes(),
                serde_json::to_vec(&shard).map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "serialize keep-small shard: {e}"
                    ))
                })?,
            ));
        }
        store.inner().batch_put(items).await.map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("persist keep-small: {e}"))
        })?;
        if only_schemas.is_none() {
            self.keep_small.clear_dirty_schemas();
        }
        Ok(())
    }

    pub(crate) async fn write_keep_small_snapshot(
        &self,
        snapshot: &crate::db_operations::KeepSmallSnapshot,
    ) -> Result<(), crate::schema::SchemaError> {
        self.write_keep_small_header_and_shards(snapshot, None)
            .await
    }
}
