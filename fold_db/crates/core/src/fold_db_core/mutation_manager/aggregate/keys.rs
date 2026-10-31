//! Grant keys, fingerprints, named locks, and target key resolution for aggregate writes.

use super::*;

impl MutationManager {
    /// Drop the repair grant, then the finalized grant, for one target.
    pub(super) async fn delete_aggregate_target_grants(
        &self,
        storage_prefix: Option<&str>,
        target_schema_name: &str,
        target_key: &str,
    ) -> Result<(), SchemaError> {
        let grant_key =
            aggregate_plan::repair_grant_key(storage_prefix, target_schema_name, target_key);
        let finalized_key =
            aggregate_plan::finalized_grant_key(storage_prefix, target_schema_name, target_key);
        self.delete_durable_copies([grant_key.as_str(), finalized_key.as_str()])
            .await
    }

    /// Delete metadata keys in order. A failed key stops the rest.
    pub(super) async fn delete_durable_copies(&self, keys: [&str; 2]) -> Result<(), SchemaError> {
        for key in keys {
            self.db_ops.metadata().delete_typed_durable(key).await?;
        }
        Ok(())
    }

    /// Prepare author clocks, write the mutations, and require a durable receipt.
    /// `stamp_revision` records the first mutation counter on that receipt.
    pub(super) async fn commit_clocked_mutations(
        &self,
        mut mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
        label: &str,
        stamp_revision: bool,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let clock = self.prepare_mutation_author_clocks(&mut mutations)?;
        let revision = if stamp_revision {
            mutations.first().map(|mutation| mutation.logical_counter)
        } else {
            None
        };
        if let Some((reservation, state)) = clock {
            reservation.submit_and_wait(state).await?;
        }
        let result = self
            .write_mutations_batch_inner(mutations, storage_prefix, origin)
            .await;
        let mut receipt = aggregate_plan::require_durable_receipt(result?, label)?;
        if let Some(revision) = revision {
            receipt.revision = Some(revision);
        }
        Ok(receipt)
    }

    pub(super) fn aggregate_named_lock(&self, key: String) -> Arc<tokio::sync::RwLock<()>> {
        let mut locks = self
            .aggregate_locks
            .lock()
            .expect("aggregate locks poisoned");
        // A waiter owns its Arc before the map lock releases. Only a lock
        // without holders or waiters can disappear; the next call drops idle
        // keys rather than retaining one for every aggregate ever addressed.
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        Arc::clone(locks.entry(key).or_default())
    }

    pub(super) async fn acquire_aggregate_named_locks(
        &self,
        mut keys: Vec<String>,
    ) -> Vec<tokio::sync::OwnedRwLockWriteGuard<()>> {
        keys.sort_unstable();
        keys.dedup();
        let mut guards = Vec::with_capacity(keys.len());
        for key in keys {
            guards.push(self.aggregate_named_lock(key).write_owned().await);
        }
        guards
    }

    pub(super) fn aggregate_target_lock_key(
        storage_prefix: Option<&str>,
        target_schema_name: &str,
        target_key: &KeyValue,
    ) -> String {
        let base = format!(
            "aggregate_target\u{1f}{target_schema_name}\u{1f}{}",
            target_key.to_storage_key()
        );
        crate::schema::types::field::build_storage_key(storage_prefix, &base)
    }

    pub(super) fn aggregate_probe(schema_name: String, key_value: KeyValue) -> Mutation {
        Mutation::new(
            schema_name,
            HashMap::new(),
            key_value,
            String::new(),
            MutationType::Update,
        )
    }

    /// Load one named aggregate schema. The missing-name error keeps `kind`.
    pub(super) fn require_aggregate_schema(
        &self,
        kind: &str,
        name: &str,
    ) -> Result<crate::schema::Schema, SchemaError> {
        self.schema_manager
            .get_schema_metadata(name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!("aggregate {kind} schema '{name}' not found"))
            })
    }

    pub(super) fn resolve_aggregate_target_key(
        &self,
        schema_name: &str,
        key_value: &KeyValue,
    ) -> Result<KeyValue, SchemaError> {
        let schema = self.require_aggregate_schema("target", schema_name)?;
        if schema.schema_type != DeclarativeSchemaType::Hash {
            return Err(SchemaError::InvalidData(
                "aggregate target schema must be Hash".into(),
            ));
        }
        let probe = Self::aggregate_probe(schema_name.to_string(), key_value.clone());
        self.resolve_mutation_key_value(schema_name, &schema, &probe)
    }
}
