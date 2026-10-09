//! Grant keys, fingerprints, named locks, and target key resolution for aggregate writes.

use super::*;

impl MutationManager {
    pub(super) fn aggregate_repair_grant_key(
        storage_prefix: Option<&str>,
        target_schema_name: &str,
        target_key: &str,
    ) -> String {
        format!(
            "{}:repair_grant",
            aggregate_target_guard_key(storage_prefix, target_schema_name, target_key)
        )
    }

    pub(super) fn aggregate_finalized_grant_key(
        storage_prefix: Option<&str>,
        target_schema_name: &str,
        target_key: &str,
    ) -> String {
        format!(
            "{}:finalized",
            Self::aggregate_repair_grant_key(storage_prefix, target_schema_name, target_key)
        )
    }

    pub(super) fn aggregate_totals_fingerprint(totals: &BTreeMap<String, i64>) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"lastdb-aggregate-totals-v1\0");
        for (field, total) in totals {
            hasher.update(field.as_bytes());
            hasher.update([0]);
            hasher.update(total.to_be_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    pub(super) fn require_durable_receipt(
        receipt: ResidentCommitReceipt,
        phase: &str,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if receipt.durability != ResidentDurability::Durable {
            return Err(SchemaError::InvalidData(format!(
                "aggregate {phase} returned a queued receipt"
            )));
        }
        Ok(receipt)
    }

    pub(super) fn aggregate_named_lock(&self, key: String) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.cas_locks.lock().expect("aggregate locks poisoned");
        Arc::clone(locks.entry(key).or_default())
    }

    pub(super) async fn acquire_aggregate_named_locks(
        &self,
        mut keys: Vec<String>,
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        keys.sort_unstable();
        keys.dedup();
        let mut guards = Vec::with_capacity(keys.len());
        for key in keys {
            guards.push(self.aggregate_named_lock(key).lock_owned().await);
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

    pub(super) fn resolve_aggregate_target_key(
        &self,
        schema_name: &str,
        key_value: &KeyValue,
    ) -> Result<KeyValue, SchemaError> {
        let schema = self
            .schema_manager
            .get_schema_metadata(schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate target schema '{schema_name}' not found"
                ))
            })?;
        if schema.schema_type != DeclarativeSchemaType::Hash {
            return Err(SchemaError::InvalidData(
                "aggregate target schema must be Hash".into(),
            ));
        }
        let probe = Self::aggregate_probe(schema_name.to_string(), key_value.clone());
        Self::resolve_mutation_key_value(schema_name, &schema, &probe)
    }
}
