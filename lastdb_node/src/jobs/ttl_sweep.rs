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
//! deletes through the anchor, so a write through either name is covered.
//!
//! Safety properties of the sweep:
//! - never create an absent schema
//! - bounded batches
//! - report a completed no-op, but recheck on the next sampler tick because
//!   time and local policy can change without a daemon restart
//! - after `MAX_DRAIN_ATTEMPTS` failed passes in a row, wait longer before each
//!   next pass (up to an hour), and never stop for the life of the process
//!
//! Kill switch: `LASTDB_TTL_SWEEP=0`. Default **on**.
//!
//! Pacing, verb and the knobs that tune them: `ttl_sweep/pace.rs` and `ttl_sweep/verb.rs`.

use fold_db::clock::unix_secs;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::Instrument;

use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;
use fold_db::schema::SchemaRetentionPolicy;

use crate::host::Host;

mod discovery;
mod erase;
mod send;
use discovery::Backlog;

/// Default-on kill switch. Off only for the explicit falsey values.
pub const TTL_SWEEP_ENV: &str = "LASTDB_TTL_SWEEP";
/// Interval between periodic TTL sweep passes in seconds (default: 60).
pub const TTL_SWEEP_INTERVAL_SECS_ENV: &str = "LASTDB_TTL_SWEEP_INTERVAL_SECS";

/// Width of the entry time at the front of a Range or HashRange key: unix
/// seconds, zero-padded to 20 digits (the width of `u64::MAX`).
const AGE_KEY_DIGITS: usize = 20;

pub fn ttl_sweep_enabled() -> bool {
    !matches!(
        std::env::var(TTL_SWEEP_ENV).ok().as_deref().map(str::trim),
        Some("0" | "false" | "no" | "off")
    )
}

fn sweep_interval_from_env() -> std::time::Duration {
    let secs = std::env::var(TTL_SWEEP_INTERVAL_SECS_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(60)
        .max(1);
    std::time::Duration::from_secs(secs)
}

/// Spawn the periodic TTL sweep task. Stops cleanly once `shutdown` is set.
pub fn spawn_periodic(
    host: std::sync::Arc<Host>,
    shutdown: std::sync::Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let interval = sweep_interval_from_env();
    tokio::spawn(
        async move {
            if ttl_sweep_enabled() {
                send::warn_on_bad_verb_once();
            }
            while !shutdown.load(Ordering::Acquire) {
                if ttl_sweep_enabled() {
                    sweep_ttl(&host, &shutdown).await;
                }
                let until = tokio::time::Instant::now() + interval;
                while !shutdown.load(Ordering::Acquire) && tokio::time::Instant::now() < until {
                    tokio::time::sleep(
                        std::time::Duration::from_millis(250)
                            .min(until.saturating_duration_since(tokio::time::Instant::now())),
                    )
                    .await;
                }
            }
        }
        .in_current_span(),
    )
}

/// Run one bounded TTL sweep pass.
pub async fn sweep_ttl(host: &Host, shutdown: &AtomicBool) {
    if !ttl_sweep_enabled() {
        return;
    }
    let started = std::time::Instant::now();
    match sweep_ttl_inner(host, shutdown).await {
        Ok(totals) => finish_pass(&totals, started),
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::ttl_sweep",
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "ttl sweep failed"
            );
        }
    }
}

fn finish_pass(totals: &erase::PassTotals, started: std::time::Instant) {
    let elapsed_ms = started.elapsed().as_millis() as u64;
    if totals.whole_pass_failed() {
        tracing::warn!(
            target: "lastdbd::ttl_sweep",
            schemas = totals.schemas,
            elapsed_ms,
            "ttl sweep failed: no schema succeeded"
        );
    } else if totals.erased > 0 || totals.dropped > 0 {
        tracing::info!(
            target: "lastdbd::ttl_sweep",
            removed = totals.erased,
            dropped_after_recheck = totals.dropped,
            elapsed_ms,
            "ttl sweep purged expired retained rows"
        );
    }
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
    let policies = host
        .db
        .db_ops()
        .list_schema_retention_policies()
        .await
        .map_err(|error| error.to_string())?;
    if policies.is_empty() {
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
/// the schema is absent. Range retention uses the declared external key.
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
