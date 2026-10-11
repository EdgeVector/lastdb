//! DynamoDB-backed schema mutation gate quota store.

use super::*;

const QUOTA_TTL_GRACE_SECS: u64 = 60;

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuotaItemKey {
    bucket_key: String,
    window_start: u64,
    expires_at: u64,
}

fn quota_item_key(key: &str, window: Duration, now: u64) -> QuotaItemKey {
    let window_secs = window.as_secs().max(1);
    let window_start = (now / window_secs) * window_secs;
    QuotaItemKey {
        bucket_key: format!("{window_secs}:{key}"),
        window_start,
        expires_at: window_start + window_secs + QUOTA_TTL_GRACE_SECS,
    }
}

/// DynamoDB-backed schema mutation gate quota store.
///
/// Table contract: partition key `bucket_key` (S), sort key `window_start` (N),
/// numeric `count`, numeric TTL attribute `expires_at`.
#[derive(Clone)]
pub struct DynamoDbSchemaMutationGateStore {
    client: DdbClient,
    table: String,
}

impl DynamoDbSchemaMutationGateStore {
    pub fn new(client: DdbClient, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    pub fn into_schema_mutation_gate_store(self) -> SchemaMutationGateStore {
        SchemaMutationGateStore::new(Arc::new(self))
    }

    async fn get_count(
        client: DdbClient,
        table: String,
        item_key: QuotaItemKey,
    ) -> Result<usize, SchemaMutationGateError> {
        let resp = client
            .get_item()
            .table_name(table)
            .key("bucket_key", AttributeValue::S(item_key.bucket_key))
            .key(
                "window_start",
                AttributeValue::N(item_key.window_start.to_string()),
            )
            .projection_expression("#count")
            .expression_attribute_names("#count", "count")
            .send()
            .await
            .map_err(|e| {
                SchemaMutationGateError::Internal(format!(
                    "schema mutation gate quota get failed: {e}"
                ))
            })?;
        let count = resp
            .item
            .as_ref()
            .and_then(|item| item.get("count"))
            .and_then(|value| match value {
                AttributeValue::N(n) => n.parse::<usize>().ok(),
                _ => None,
            })
            .unwrap_or(0);
        Ok(count)
    }

    async fn increment_under_limit(
        client: DdbClient,
        table: String,
        item_key: QuotaItemKey,
        bucket_label: &'static str,
        window_label: &'static str,
        limit: usize,
        now: u64,
    ) -> Result<(), SchemaMutationGateError> {
        let retry_after_secs = item_key
            .window_start
            .saturating_add(item_key.expires_at.saturating_sub(item_key.window_start))
            .saturating_sub(QUOTA_TTL_GRACE_SECS)
            .saturating_sub(now)
            .max(1);
        let result = client
            .update_item()
            .table_name(table)
            .key("bucket_key", AttributeValue::S(item_key.bucket_key))
            .key(
                "window_start",
                AttributeValue::N(item_key.window_start.to_string()),
            )
            .update_expression(
                "SET #count = if_not_exists(#count, :zero) + :one, \
                 #expires_at = :expires_at, #updated_at = :now",
            )
            .condition_expression("attribute_not_exists(#count) OR #count < :limit")
            .expression_attribute_names("#count", "count")
            .expression_attribute_names("#expires_at", "expires_at")
            .expression_attribute_names("#updated_at", "updated_at")
            .expression_attribute_values(":zero", AttributeValue::N("0".to_string()))
            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
            .expression_attribute_values(":limit", AttributeValue::N(limit.to_string()))
            .expression_attribute_values(
                ":expires_at",
                AttributeValue::N(item_key.expires_at.to_string()),
            )
            .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(DdbSdkError::ServiceError(e))
                if matches!(e.err(), UpdateItemError::ConditionalCheckFailedException(_)) =>
            {
                Err(SchemaMutationGateError::QuotaExceeded {
                    bucket: bucket_label,
                    window: window_label,
                    limit,
                    retry_after_secs,
                })
            }
            Err(e) => Err(SchemaMutationGateError::Internal(format!(
                "schema mutation gate quota update failed: {e}"
            ))),
        }
    }
}

impl SchemaMutationGateQuotaStore for DynamoDbSchemaMutationGateStore {
    fn backend_label(&self) -> &'static str {
        "dynamodb"
    }

    fn bucket_len(
        &self,
        key: &str,
        window: Duration,
        now: u64,
    ) -> Result<usize, SchemaMutationGateError> {
        let item_key = quota_item_key(key, window, now);
        block_on_quota(Self::get_count(
            self.client.clone(),
            self.table.clone(),
            item_key,
        ))
    }

    fn check_quota_bucket(
        &self,
        bucket_label: &'static str,
        window_label: &'static str,
        key: &str,
        window: Duration,
        limit: usize,
        now: u64,
    ) -> Result<(), SchemaMutationGateError> {
        if limit == 0 {
            return Ok(());
        }
        let item_key = quota_item_key(key, window, now);
        block_on_quota(Self::increment_under_limit(
            self.client.clone(),
            self.table.clone(),
            item_key,
            bucket_label,
            window_label,
            limit,
            now,
        ))
    }
}

fn block_on_quota<T>(
    future: impl std::future::Future<Output = Result<T, SchemaMutationGateError>>,
) -> Result<T, SchemaMutationGateError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| SchemaMutationGateError::Internal(e.to_string()))?;
        runtime.block_on(future)
    }
}
