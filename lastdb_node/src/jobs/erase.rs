//! TTL sweep batch pacing, verb selection, expiry re-check, and erasure.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once};
use std::time::{Duration, Instant};

use fold_db::alternate_keys::MAX_ALTERNATE_ENTRIES;
use fold_db::clock::unix_secs;
use fold_db::db_operations::WriterWatch;
use fold_db::record_replacement::{RecordExpectation, RecordTarget};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::{Mutation, MutationType};
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::{KeyValue, Query};

use super::ttl_sweep::{fill_page, Backlog, Source, AGE_KEY_DIGITS};
use crate::host::Host;

pub(super) fn key_is_expired(
    key: &KeyValue,
    schema_type: &DeclarativeSchemaType,
    cutoff: u64,
) -> bool {
    key.range
        .as_deref()
        .and_then(|r| r.get(..AGE_KEY_DIGITS))
        .is_some_and(|d| {
            d.bytes().all(|b| b.is_ascii_digit())
                && d.parse::<u64>().is_ok_and(|t| t < cutoff)
                && (*schema_type == DeclarativeSchemaType::Range
                    || (*schema_type == DeclarativeSchemaType::HashRange && key.hash.is_some()))
        })
}

static WATCH: Mutex<Option<WriterWatch>> = Mutex::new(None);
const REST_SLICE: Duration = Duration::from_millis(250);
const MAX_PAUSE: Duration = Duration::from_secs(600);
const TTL_SWEEP_VERB_ENV: &str = "LASTDB_TTL_SWEEP_VERB";

#[derive(Default, Clone, Copy)]
pub(super) struct PassTotals {
    pub(super) erased: usize,
    pub(super) dropped: usize,
    pub(super) keys_left: usize,
    pub(super) cut: bool,
    pub(super) schemas: usize,
    pub(super) schemas_failed: usize,
}

type PaceCfg = (bool, usize, u64, Duration);

fn batch_size(host: &Host, b: &Backlog, pace_batch: usize, is_delete: bool) -> usize {
    let limit = host.db.db_ops().batch_operation_limit();
    let (op_f, rd_f) = if is_delete { (5, 13) } else { (4, 12) };
    (1..=pace_batch.min(b.batch_cap))
        .rev()
        .find(|&k| {
            let chunk_ops = k.div_ceil(64).saturating_mul(b.fields + 8);
            let ops = k * b.fields * op_f + chunk_ops + k * (b.alternates + 2);
            let reads = k * b.fields * rd_f + chunk_ops + k * (b.alternates * 4 + 2);
            ops <= limit && reads <= 40_000 && k * b.alternates <= MAX_ALTERNATE_ENTRIES
        })
        .unwrap_or(1)
}

pub(super) fn warn_on_bad_verb_once() {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        if let Ok(raw) = std::env::var(TTL_SWEEP_VERB_ENV) {
            let v = raw.trim();
            if !v.is_empty()
                && !v.eq_ignore_ascii_case("delete")
                && !v.eq_ignore_ascii_case("purge")
            {
                tracing::warn!(target: "lastdbd::ttl_sweep", value = v, "unknown {TTL_SWEEP_VERB_ENV} value; defaulting to `purge`");
            }
        }
    });
}

pub(super) async fn purge_round_robin(
    host: &Host,
    mut backlogs: Vec<Backlog>,
    shutdown: &AtomicBool,
    mut totals: PassTotals,
) -> PassTotals {
    let is_delete =
        std::env::var(TTL_SWEEP_VERB_ENV).is_ok_and(|v| v.trim().eq_ignore_ascii_case("delete"));
    let cfg: PaceCfg = (
        is_delete,
        fold_db::env_flag::var_or("LASTDB_TTL_SWEEP_BATCH", 16usize).clamp(1, 64),
        fold_db::env_flag::var_or("LASTDB_TTL_SWEEP_DUTY_PCT", 25u64).clamp(1, 100),
        Duration::from_millis(fold_db::env_flag::var_or(
            "LASTDB_TTL_SWEEP_MIN_PAUSE_MS",
            25u64,
        ))
        .min(MAX_PAUSE),
    );
    let mut watch = WATCH
        .lock()
        .expect("ttl writer watch")
        .take()
        .unwrap_or_default();
    'rounds: while backlogs.iter().any(|b| !b.is_done()) {
        for b in &mut backlogs {
            if b.is_done() {
                continue;
            }
            if shutdown.load(Ordering::Acquire) {
                totals.cut = true;
                break 'rounds;
            }
            if let Err(error) = fill_page(host, b, batch_size(host, b, cfg.1, cfg.0)).await {
                tracing::warn!(target: "lastdbd::ttl_sweep", schema = %b.schema_name, error = %error, "ttl discovery failed; skip this schema for this pass");
                b.abandon();
                totals.schemas_failed += 1;
                continue;
            }
            if erase_page(host, b, shutdown, cfg, &mut totals, &mut watch).await {
                totals.cut = true;
                break 'rounds;
            }
        }
    }
    *WATCH.lock().expect("ttl writer watch") = Some(watch);
    totals.keys_left = backlogs.iter().map(|b| b.abandoned + b.keys.len()).sum();
    totals
}

async fn erase_page(
    host: &Host,
    b: &mut Backlog,
    shutdown: &AtomicBool,
    (is_delete, pace_batch, duty_pct, min_pause): PaceCfg,
    totals: &mut PassTotals,
    watch: &mut WriterWatch,
) -> bool {
    while !b.keys.is_empty() {
        if shutdown.load(Ordering::Acquire) {
            return true;
        }
        let n = b.keys.len().min(batch_size(host, b, pace_batch, is_delete));
        let chunk = b.keys[..n].to_vec();
        let started = Instant::now();
        let (outcome, blocked, scopes) =
            Box::pin(WriterWatch::measure(erase_batch(host, b, chunk, is_delete))).await;
        let pause = if watch.writer_present(scopes) {
            let work_ms = u64::try_from(blocked.as_millis()).unwrap_or(u64::MAX);
            Duration::from_millis(work_ms.saturating_mul(100 - duty_pct) / duty_pct)
                .clamp(min_pause, MAX_PAUSE)
        } else {
            min_pause
        };
        match outcome {
            Ok((erased, dropped, missing)) => {
                b.keys.drain(..n.min(b.keys.len()));
                totals.erased += erased;
                totals.dropped += dropped;
                if dropped > 0 {
                    tracing::info!(target: "lastdbd::ttl_sweep", schema = %b.schema_name, dropped, erased, "ttl sweep re-check dropped keys that are no longer expired");
                }
                tracing::debug!(target: "lastdbd::ttl_sweep", schema = %b.schema_name, keys = erased, dropped, missing, took_ms = started.elapsed().as_millis() as u64, "ttl sweep batch erased");
            }
            Err(error)
                if n > 1
                    && [
                        "read resource limit exceeded",
                        "complete mutation limit exceeded",
                        "write resource limit",
                    ]
                    .iter()
                    .any(|s| error.contains(s)) =>
            {
                b.batch_cap = (n / 2).max(1);
                tracing::warn!(target: "lastdbd::ttl_sweep", schema = %b.schema_name, keys = n, error = %error, "ttl size refusal; retry a smaller batch");
            }
            Err(error) => {
                b.batch_cap = (n / 2).max(1);
                b.abandon();
                totals.schemas_failed += 1;
                tracing::warn!(target: "lastdbd::ttl_sweep", schema = %b.schema_name, error = %error, "ttl erase failed; skip this schema for this pass");
                return false;
            }
        }
        if !b.is_done() {
            let until = Instant::now() + pause;
            while !shutdown.load(Ordering::Acquire) {
                let left = until.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    break;
                }
                tokio::time::sleep(left.min(REST_SLICE)).await;
                if left > REST_SLICE && watch.rest_may_end() {
                    break;
                }
            }
        }
    }
    false
}

async fn erase_batch(
    host: &Host,
    b: &mut Backlog,
    chunk: Vec<KeyValue>,
    is_delete: bool,
) -> Result<(usize, usize, usize), String> {
    let listed = chunk.len();
    let Some(policy) = host
        .db
        .db_ops()
        .schemas()
        .get_schema_retention_policy(&b.schema_name)
        .await
        .map_err(|e| e.to_string())?
    else {
        b.keys.clear();
        b.source = Source::Done;
        tracing::info!(target: "lastdbd::ttl_sweep", schema = %b.schema_name, "retention policy is gone; dropping rest of schema");
        return Ok((0, listed, 0));
    };
    let cutoff = b.cutoff.min(unix_secs().saturating_sub(policy.ttl_seconds));
    let hash_or_single = matches!(
        b.schema_type,
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single
    );
    let mut keys = if hash_or_single {
        host.db
            .db_ops()
            .schemas()
            .retention_keys_still_expired(&b.schema_name, &chunk, cutoff)
            .await
            .map_err(|e| e.to_string())?
    } else {
        chunk
            .into_iter()
            .filter(|k| key_is_expired(k, &b.schema_type, cutoff))
            .collect()
    };
    let (dropped, cutoff_nanos, mut missing) = (
        listed - keys.len(),
        hash_or_single.then(|| cutoff.saturating_mul(1_000_000_000)),
        0,
    );
    loop {
        let Err(error) = purge_keys(host, &b.schema_name, &keys, is_delete, cutoff_nanos).await
        else {
            return Ok((keys.len(), dropped, missing));
        };
        if is_delete {
            return Err(error);
        }
        let absent = host
            .db
            .absent_erase_targets(&b.schema_name, &keys)
            .await
            .map_err(|c| format!("{error}; the absent-key check failed: {c}"))?;
        if absent.is_empty() {
            return Err(error);
        }
        missing += absent.len();
        keys.retain(|k| !absent.contains(k));
        tracing::info!(target: "lastdbd::ttl_sweep", schema = %b.schema_name, absent = absent.len(), kept = keys.len(), "ttl sweep dropped already-absent keys and retries remaining batch");
        if keys.is_empty() {
            return Ok((0, dropped, missing));
        }
    }
}

async fn purge_keys(
    host: &Host,
    schema_name: &str,
    keys: &[KeyValue],
    is_delete: bool,
    write_cutoff_nanos: Option<u64>,
) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let pairs = keys
        .iter()
        .map(|k| {
            (
                k.hash.clone().unwrap_or_default(),
                k.range.clone().unwrap_or_default(),
            )
        })
        .collect();
    let mut rows = host
        .db
        .query_executor()
        .query(Query::new_with_filter(
            schema_name.into(),
            Some(HashRangeFilter::HashRangeKeys(pairs)),
        ))
        .await
        .map_err(|e| e.to_string())?;
    if rows.len() != keys.len() {
        return Err("erasure target disappeared before preparation".into());
    }
    let (mtype, caller, sync) = if is_delete {
        (MutationType::Delete, "ttl-sweep-delete", Some(true))
    } else {
        (MutationType::Purge, "ttl-sweep-purge", None)
    };
    let pubkey = host.public_key();
    let mutations = keys
        .iter()
        .map(|key| {
            let r = rows.remove(key).expect("every erasure key was read");
            let mut m = Mutation::with_uuid(
                uuid::Uuid::new_v4().to_string(),
                RecordTarget {
                    schema: schema_name.into(),
                    key: key.clone(),
                    alternates: r.alternates,
                },
                HashMap::new(),
                pubkey.clone(),
                mtype,
                RecordExpectation::Revision {
                    revision: r.revision,
                },
            );
            (m.erase_caller, m.synchronous, m.erase_written_before_nanos) =
                (Some(caller), sync, write_cutoff_nanos);
            m
        })
        .collect();
    host.db
        .mutation_manager()
        .write_mutations(mutations)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}
