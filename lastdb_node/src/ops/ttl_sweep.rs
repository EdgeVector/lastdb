//! Node-local TTL sweep for retained Single, Hash, Range, and HashRange series.
//!
//! Consumes [`fold_db::schema::SchemaRetentionPolicy`] records and hard-deletes
//! expired keys through the audited `MutationType::Purge` batch path. Range
//! and HashRange layouts select by their time-sortable key. HashRange writes
//! register their observed partitions in node-local retention state, so a
//! sweep never needs a cross-partition product read. Hash and Single layouts
//! select from the explicit node-local written-at index maintained by writes.
//!
//! Safety properties borrowed from `drain_orphaned_telemetry`:
//! - never create an absent schema
//! - skip while a backup cut is held
//! - bounded batches
//! - report a completed no-op, but recheck on the next sampler tick because
//!   time and local policy can change without a daemon restart
//! - give up after `MAX_DRAIN_ATTEMPTS` failures
//!
//! Kill switch: `LASTDB_TTL_SWEEP=0`. Default **on**.

use fold_db::clock::unix_secs;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use fold_db::access::{AccessContext, CallerTransport};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::{Mutation, MutationType, Query};
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::{DeclarativeSchemaDefinition, KeyValue};
use fold_db::schema::SchemaRetentionPolicy;

use crate::host::Host;

/// Default-on kill switch. Off only for the explicit falsey values.
pub const TTL_SWEEP_ENV: &str = "LASTDB_TTL_SWEEP";

/// Upper bound on keys purged in one mutation batch.
const TTL_SWEEP_BATCH_SIZE: usize = 64;

static TEST_BACKUP_CUT_HELD: AtomicBool = AtomicBool::new(false);

pub fn ttl_sweep_enabled() -> bool {
    !matches!(
        std::env::var(TTL_SWEEP_ENV).ok().as_deref().map(str::trim),
        Some("0" | "false" | "no" | "off")
    )
}

fn test_backup_cut_held() -> bool {
    TEST_BACKUP_CUT_HELD.load(Ordering::SeqCst)
}

/// Spawn the TTL sweep off the sampler tick, at most one at a time.
pub fn maybe_spawn_ttl_sweep(host: &std::sync::Arc<Host>) {
    if !ttl_sweep_enabled() {
        return;
    }
    if !host.self_metrics.begin_ttl_sweep() {
        return;
    }
    let host = std::sync::Arc::clone(host);
    tokio::spawn(async move {
        sweep_ttl(&host).await;
        host.self_metrics.end_ttl_sweep();
    });
}

/// Run one TTL sweep pass. Tests call this directly so they do not race the
/// sampler task.
pub async fn sweep_ttl(host: &Host) {
    if !ttl_sweep_enabled() {
        return;
    }
    if backup_cut_is_held(host).await {
        tracing::info!(
            target: "lastdbd::ttl_sweep",
            "ttl sweep skipped: backup cut is held"
        );
        return;
    }

    let started = std::time::Instant::now();
    match sweep_ttl_inner(host).await {
        Ok(removed) => {
            host.self_metrics.mark_ttl_sweep_pass(removed, unix_secs());
            if removed > 0 {
                tracing::info!(
                    target: "lastdbd::ttl_sweep",
                    removed,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "ttl sweep purged expired retained rows"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::ttl_sweep",
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "ttl sweep failed"
            );
            host.self_metrics.record_ttl_sweep_failure();
        }
    }
}

async fn backup_cut_is_held(host: &Host) -> bool {
    if test_backup_cut_held() {
        return true;
    }
    host.db.backup_publish_target_is_held().await
}

async fn sweep_ttl_inner(host: &Host) -> Result<usize, String> {
    let policies = match host.db.db_ops().list_schema_retention_policies().await {
        Ok(policies) => {
            host.self_metrics
                .cache_retention_policies(policies.iter().cloned().collect());
            policies
        }
        Err(error) => {
            let error = error.to_string();
            host.self_metrics
                .cache_retention_policy_error(error.clone());
            return Err(error);
        }
    };
    if policies.is_empty() {
        host.self_metrics.settle_ttl_sweep();
        return Ok(0);
    }

    let now = unix_secs();
    let mut removed = 0usize;
    for (schema_name, policy) in policies {
        removed += sweep_one_schema(host, &schema_name, policy, now).await?;
    }
    Ok(removed)
}

async fn sweep_one_schema(
    host: &Host,
    schema_name: &str,
    policy: SchemaRetentionPolicy,
    now: u64,
) -> Result<usize, String> {
    let Some(schema) = host
        .db
        .schema_manager()
        .get_schema_metadata(schema_name)
        .map_err(|e| e.to_string())?
    else {
        return Ok(0);
    };
    let cutoff = now.saturating_sub(policy.ttl_seconds);
    let mut expired = match &schema.schema_type {
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => host
            .db
            .db_ops()
            .schemas()
            .expired_schema_retention_keys(schema_name, cutoff)
            .await
            .map_err(|e| e.to_string())?,
        DeclarativeSchemaType::Range | DeclarativeSchemaType::HashRange => {
            let Some(range_field) = schema
                .key
                .as_ref()
                .and_then(|key| key.range_field.clone())
                .filter(|field| !field.is_empty())
            else {
                return Ok(0);
            };
            let mut hash_partitions = policy.hash_partitions.clone();
            if matches!(schema.schema_type, DeclarativeSchemaType::HashRange) {
                hash_partitions.extend(
                    host.db
                        .db_ops()
                        .schemas()
                        .schema_retention_hash_partitions(schema_name)
                        .await
                        .map_err(|e| e.to_string())?,
                );
                hash_partitions.sort();
                hash_partitions.dedup();
            }
            query_expired_keys(
                host,
                schema_name,
                &schema,
                &range_field,
                &hash_partitions,
                &format!("{cutoff:020}"),
            )
            .await?
        }
    };
    expired.sort_by(|a, b| a.range.cmp(&b.range).then(a.hash.cmp(&b.hash)));
    if expired.is_empty() {
        return Ok(0);
    }

    let mut removed = 0usize;
    for chunk in expired.chunks(TTL_SWEEP_BATCH_SIZE) {
        purge_keys(host, schema_name, chunk).await?;
        removed += chunk.len();
    }
    Ok(removed)
}

async fn query_expired_keys(
    host: &Host,
    schema_name: &str,
    schema: &DeclarativeSchemaDefinition,
    range_field: &str,
    hash_partitions: &[String],
    cutoff_key: &str,
) -> Result<Vec<KeyValue>, String> {
    let filters: Vec<HashRangeFilter> = match &schema.schema_type {
        DeclarativeSchemaType::Range => vec![HashRangeFilter::RangeRange {
            start: String::new(),
            end: cutoff_key.to_string(),
        }],
        DeclarativeSchemaType::HashRange => hash_partitions
            .iter()
            .map(|hash| HashRangeFilter::HashRangeRange {
                hash: hash.clone(),
                start: String::new(),
                end: cutoff_key.to_string(),
            })
            .collect(),
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => Vec::new(),
    };
    let mut keys = Vec::new();
    for filter in filters {
        let query = Query::new_with_filter(
            schema_name.to_string(),
            vec![range_field.to_string()],
            Some(filter),
        );
        let rows = host
            .db
            .query_executor()
            .query_with_access(query, &owner_context(host))
            .await
            .map_err(|e| e.to_string())?;
        keys.extend(
            rows.get(range_field)
                .into_iter()
                .flat_map(|field| field.keys().cloned())
                .filter(|key| key_is_expired(key, &schema.schema_type, cutoff_key)),
        );
    }
    keys.sort_by(|a, b| a.range.cmp(&b.range).then(a.hash.cmp(&b.hash)));
    keys.dedup();
    Ok(keys)
}

fn key_is_expired(key: &KeyValue, schema_type: &DeclarativeSchemaType, cutoff_key: &str) -> bool {
    let Some(range) = key.range.as_deref() else {
        return false;
    };
    if range >= cutoff_key {
        return false;
    }
    match schema_type {
        DeclarativeSchemaType::Range => true,
        DeclarativeSchemaType::HashRange => key.hash.is_some(),
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => false,
    }
}

async fn purge_keys(host: &Host, schema_name: &str, keys: &[KeyValue]) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let mutations: Vec<Mutation> = keys
        .iter()
        .map(|key| {
            Mutation::new(
                schema_name.to_string(),
                HashMap::new(),
                key.clone(),
                host.public_key(),
                MutationType::Purge,
            )
        })
        .collect();
    host.db
        .mutation_manager()
        .write_mutations_with_access(mutations, &owner_context(host))
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn owner_context(host: &Host) -> AccessContext {
    AccessContext::owner(host.user_hash.clone()).with_transport(CallerTransport::InProcess)
}
