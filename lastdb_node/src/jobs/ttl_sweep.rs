//! Node-local TTL sweep for retained Single, Hash, Range, and HashRange series.
//!
//! Consumes [`fold_db::schema::SchemaRetentionPolicy`] records and erases
//! expired keys through the audited mutation batch path: the hard-erasure
//! `MutationType::Purge` by default, the ordinary `MutationType::Delete` when
//! `LASTDB_TTL_SWEEP_VERB=delete` (see [`verb`]). Range
//! and HashRange layouts select by their time-sortable key: the entry time at
//! the front of the range key, written as unix seconds zero-padded to 20
//! digits. A range key that does not start with 20 ASCII digits has no entry
//! time, so the sweep never expires it. HashRange writes register their
//! observed partitions in node-local retention state, so a sweep never needs a
//! cross-partition product read. Hash and Single layouts select from the
//! explicit node-local written-at index maintained by writes.
//!
//! Policies are listed by anchor name. An app schema has two installed names
//! that share records, and an app manifest `retention` makes its catalog schema
//! (named by the identity hash) the anchor of both. The sweep reads, queries and
//! deletes through the anchor, so a write through either name is covered. See
//! `docs/app-manifest-retention.md`.
//!
//! Safety properties borrowed from `drain_orphaned_telemetry`:
//! - never create an absent schema
//! - bounded batches
//! - report a completed no-op, but recheck on the next sampler tick because
//!   time and local policy can change without a daemon restart
//! - after `MAX_DRAIN_ATTEMPTS` failed passes in a row, wait longer before each
//!   next pass (up to an hour), and never stop for the life of the process
//!
//! Kill switch: `LASTDB_TTL_SWEEP=0`. Default **on**.
//!
//! Pacing, verb and the knobs that tune them: `docs/lastdb-ttl-sweep.md`.

use fold_db::clock::unix_secs;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::Instrument;

use fold_db::access::{AccessContext, CallerTransport};
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;
use fold_db::schema::SchemaRetentionPolicy;

use crate::host::Host;

mod backlog;
mod discovery;
mod erase;
mod pace;
mod send;
mod verb;
use backlog::Backlog;
pub use pace::{TTL_SWEEP_BATCH_ENV, TTL_SWEEP_DUTY_PCT_ENV, TTL_SWEEP_MIN_PAUSE_MS_ENV};
pub use verb::TTL_SWEEP_VERB_ENV;

/// Default-on kill switch. Off only for the explicit falsey values.
pub const TTL_SWEEP_ENV: &str = "LASTDB_TTL_SWEEP";

/// Width of the entry time at the front of a Range or HashRange key: unix
/// seconds, zero-padded to 20 digits (the width of `u64::MAX`).
const AGE_KEY_DIGITS: usize = 20;

pub fn ttl_sweep_enabled() -> bool {
    !matches!(
        std::env::var(TTL_SWEEP_ENV).ok().as_deref().map(str::trim),
        Some("0" | "false" | "no" | "off")
    )
}

/// Spawn the TTL sweep off the sampler tick, at most one at a time. The pass
/// stops between batches once `shutdown` is set, so a paced pass never holds up
/// a graceful stop.
pub fn maybe_spawn_ttl_sweep(host: &std::sync::Arc<Host>, shutdown: &std::sync::Arc<AtomicBool>) {
    if !ttl_sweep_enabled() {
        return;
    }
    verb::warn_on_bad_verb_once();
    if !host.self_metrics.begin_ttl_sweep() {
        return;
    }
    let host = std::sync::Arc::clone(host);
    let shutdown = std::sync::Arc::clone(shutdown);
    tokio::spawn(
        async move {
            // The admission is taken per batch inside the pass, not for the whole
            // pass. A snapshot cut waits for every held admission, and a waiting
            // cut blocks every later writer.
            sweep_ttl(&host, &shutdown).await;
            host.self_metrics.end_ttl_sweep();
        }
        .in_current_span(),
    );
}

/// Run one bounded TTL sweep pass.
pub async fn sweep_ttl(host: &Host, shutdown: &AtomicBool) {
    if !ttl_sweep_enabled() {
        return;
    }
    let started = std::time::Instant::now();
    match sweep_ttl_inner(host, shutdown).await {
        Ok(totals) => finish_pass(host, &totals, started),
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

/// Count a pass that listed its schemas. Only a pass where no schema succeeded
/// counts toward the wait rule, because one broken schema must not slow the
/// others.
fn finish_pass(host: &Host, totals: &erase::PassTotals, started: std::time::Instant) {
    let elapsed_ms = started.elapsed().as_millis() as u64;
    if totals.whole_pass_failed() {
        tracing::warn!(
            target: "lastdbd::ttl_sweep",
            schemas = totals.schemas,
            elapsed_ms,
            "ttl sweep failed: no schema succeeded"
        );
        host.self_metrics.record_ttl_sweep_failure();
    } else {
        host.self_metrics
            .mark_ttl_sweep_pass(totals.erased, unix_secs());
        if totals.erased > 0 || totals.dropped > 0 {
            tracing::info!(
                target: "lastdbd::ttl_sweep",
                removed = totals.erased,
                dropped_after_recheck = totals.dropped,
                elapsed_ms,
                "ttl sweep purged expired retained rows"
            );
        }
    }
    host.self_metrics.record_ttl_pass(totals.report());
    if totals.keys_left > 0 || totals.cut {
        tracing::info!(
            target: "lastdbd::ttl_sweep",
            keys_left = totals.keys_left,
            cut = totals.cut,
            schemas_failed = totals.schemas_failed,
            "ttl sweep pass ended with listed keys left over; the next pass lists them again"
        );
    }
}

async fn sweep_ttl_inner(host: &Host, shutdown: &AtomicBool) -> Result<erase::PassTotals, String> {
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
        return Ok(erase::PassTotals::default());
    }

    // Keep schema metadata and cursors here. Discovery and erasure serve
    // one bounded page per schema in turn, with only one page buffered globally.
    // A large backlog in one schema cannot starve the others for a paced pass,
    // and a schema that fails to list is skipped, not fatal to the pass.
    let now = unix_secs();
    let mut totals = erase::PassTotals {
        schemas: policies.len(),
        ..erase::PassTotals::default()
    };
    let mut backlogs = Vec::new();
    for (schema_name, policy) in policies {
        if shutdown.load(Ordering::Acquire) {
            totals.cut = true;
            return Ok(totals);
        }
        match prepare_backlog(host, &schema_name, policy, now).await {
            Ok(listed) => backlogs.extend(listed),
            Err(error) => {
                tracing::warn!(
                    target: "lastdbd::ttl_sweep",
                    schema = %schema_name,
                    error = %error,
                    "ttl sweep could not list the expired keys of this schema; it is skipped for this pass"
                );
                totals.schemas_failed += 1;
            }
        }
    }
    Ok(erase::purge_round_robin(host, backlogs, shutdown, totals).await)
}

/// Prepare one schema cursor without collecting expired keys. `None` means
/// the schema is absent or has no usable range field.
async fn prepare_backlog(
    host: &Host,
    schema_name: &str,
    policy: SchemaRetentionPolicy,
    now: u64,
) -> Result<Option<Backlog>, String> {
    let Some(schema) = host
        .db
        .schema_manager()
        .get_schema_metadata(schema_name)
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let cutoff = now.saturating_sub(policy.ttl_seconds);
    let source = match &schema.schema_type {
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => {
            discovery::Source::Ages { after: None }
        }
        DeclarativeSchemaType::Range | DeclarativeSchemaType::HashRange => {
            let Some(range_field) = schema
                .key
                .as_ref()
                .and_then(|key| key.range_field.clone())
                .filter(|field| !field.is_empty())
            else {
                return Ok(None);
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
            if matches!(schema.schema_type, DeclarativeSchemaType::Range) {
                hash_partitions = vec![String::new()];
            }
            discovery::Source::Range {
                field: range_field,
                hashes: hash_partitions,
                partition: 0,
                after: None,
                leading: true,
            }
        }
    };
    Ok(Some(Backlog::new(
        schema_name.to_string(),
        schema.schema_type.clone(),
        fold_db::fold_db_core::purge::budget::EraseShape::of(&schema),
        cutoff,
        source,
    )))
}

/// Order keys by hash, then by range (oldest first within one hash).
///
/// The store keeps all rows of one hash together in one group. A batch that
/// holds keys of one hash touches a few groups. The same batch in range order
/// holds keys of many hashes, and touches many groups, and under a small warm
/// set each group is a cold load.
fn partition_then_age(a: &KeyValue, b: &KeyValue) -> std::cmp::Ordering {
    a.hash.cmp(&b.hash).then_with(|| a.range.cmp(&b.range))
}

/// The entry time at the front of a range key, or `None` when the key does not
/// start with [`AGE_KEY_DIGITS`] ASCII digits. An undated key is never expired.
fn range_key_entry_time(range: &str) -> Option<u64> {
    let digits = range.get(..AGE_KEY_DIGITS)?;
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn key_is_expired(key: &KeyValue, schema_type: &DeclarativeSchemaType, cutoff: u64) -> bool {
    let Some(entry_time) = key.range.as_deref().and_then(range_key_entry_time) else {
        return false;
    };
    if entry_time >= cutoff {
        return false;
    }
    match schema_type {
        DeclarativeSchemaType::Range => true,
        DeclarativeSchemaType::HashRange => key.hash.is_some(),
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => false,
    }
}

fn owner_context(host: &Host) -> AccessContext {
    AccessContext::owner(host.user_hash.clone()).with_transport(CallerTransport::InProcess)
}
