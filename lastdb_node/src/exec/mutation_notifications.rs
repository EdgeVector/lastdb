//! Pure receipt and notification rules. Callers own clocks, queues, and writes.

use fold_db::db_operations::ChangeFeedEvent;
use fold_db::schema::types::MutationType;
use lastdb_host::handlers::MutationComponents;
use lastdb_host::HostError;
use serde_json::Value;

/// Input identity retained while the mutation components move into the write.
pub(super) struct MutationWatchHint {
    pub schema: String,
    pub operation: String,
    pub hash: Option<String>,
    pub range: Option<String>,
}

impl From<&MutationComponents> for MutationWatchHint {
    fn from(components: &MutationComponents) -> Self {
        let operation = match components.mutation_type {
            MutationType::Create => "create",
            MutationType::Update => "update",
            MutationType::Delete => "delete",
            MutationType::Purge => "purge",
        };
        Self {
            schema: components.schema.clone(),
            operation: operation.to_string(),
            hash: components.key_value.hash.clone(),
            range: components.key_value.range.clone(),
        }
    }
}

impl MutationWatchHint {
    /// Keep the caller's schema spelling and the receipt's convergence flags.
    /// The route supplies the timestamp at the existing notification boundary.
    pub(super) fn event(
        &self,
        mutation_id: String,
        result: &Value,
        committed_at_ms: u64,
    ) -> ChangeFeedEvent {
        let background_tasks_drained = result
            .get("background_tasks_drained")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let convergence_pending = result
            .get("convergence_pending")
            .and_then(Value::as_bool)
            .unwrap_or(!background_tasks_drained);
        ChangeFeedEvent {
            seq: 0,
            mutation_id,
            schema: self.schema.clone(),
            operation: self.operation.clone(),
            hash: self.hash.clone(),
            range: self.range.clone(),
            committed_at_ms,
            background_tasks_drained,
            convergence_pending,
        }
    }
}

pub(super) fn single_mutation_id(result: &Value) -> Result<&str, HostError> {
    result
        .get("mutation_id")
        .and_then(Value::as_str)
        .ok_or_else(|| HostError::new(500, "Single mutation receipt has no input identity"))
}

/// Validate the whole receipt before the route emits any local-watch hint.
pub(super) fn batch_mutation_ids(result: &Value, count: usize) -> Result<Vec<&str>, HostError> {
    result
        .get("mutation_ids")
        .and_then(Value::as_array)
        .filter(|ids| ids.len() == count)
        .and_then(|ids| ids.iter().map(Value::as_str).collect::<Option<Vec<_>>>())
        .ok_or_else(|| HostError::new(500, "Batch receipt does not match its input identities"))
}
