//! The erase step of the TTL sweep: which verb, a re-check of each key before
//! its batch, one admission per batch, one batch per schema in turn, and the
//! rest between batches.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fold_db::clock::unix_secs;
use fold_db::db_operations::WriterWatch;

static WATCH: std::sync::Mutex<Option<WriterWatch>> = std::sync::Mutex::new(None);
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;

use super::backlog::Backlog;
use super::key_is_expired;

mod rounds;
use super::pace::Pace;
use super::send::send_with_missing_retry;
use super::verb::SweepVerb;
use crate::host::Host;
use crate::self_metrics::TtlPassReport;

/// What one pass did.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct PassTotals {
    /// Keys that a batch sent to the erase. A key that the write-time guard
    /// spared counts here; the delete ledger counts only the records it erased.
    pub(super) erased: usize,
    /// Keys that the re-check dropped because they were no longer expired.
    pub(super) dropped: usize,
    /// Keys that a purge batch named and that were already gone. The sweep
    /// dropped them and sent the rest of the batch.
    pub(super) missing: usize,
    /// Keys that the pass listed and had not reached when it ended.
    pub(super) keys_left: usize,
    /// The pass ended early: the node was stopping or local writes were paused.
    pub(super) cut: bool,
    /// Schemas with a policy that the pass tried.
    pub(super) schemas: usize,
    /// Schemas that the pass skipped because their listing or a batch failed.
    pub(super) schemas_failed: usize,
    /// Largest expired-key page held by the worker, across all schemas.
    pub(super) peak_buffered_keys: usize,
}

impl PassTotals {
    /// Every schema failed, so the pass did no useful work.
    pub(super) fn whole_pass_failed(&self) -> bool {
        self.schemas > 0 && self.schemas_failed >= self.schemas
    }

    pub(super) fn report(&self) -> TtlPassReport {
        TtlPassReport {
            finished_backlog: !self.cut && self.keys_left == 0 && self.schemas_failed == 0,
            keys_left: u64::try_from(self.keys_left).unwrap_or(u64::MAX),
            cut: self.cut,
            schemas_failed: u32::try_from(self.schemas_failed).unwrap_or(u32::MAX),
            missing_dropped: u64::try_from(self.missing).unwrap_or(u64::MAX),
            peak_buffered_keys: u64::try_from(self.peak_buffered_keys).unwrap_or(u64::MAX),
        }
    }
}

/// Erase the backlogs in rounds: one batch from each schema that still has
/// keys, then the rest, then the next round. A schema whose batch fails is
/// skipped for the rest of the pass, logged once, and the others go on. The
/// pass ends early, without an error, when the node stops or pauses local
/// writes. Snapshot cuts retain the normal mutation admission for each batch.
/// The next sampler tick lists the rest again.
pub(super) async fn purge_round_robin(
    host: &Host,
    mut backlogs: Vec<Backlog>,
    shutdown: &AtomicBool,
    mut totals: PassTotals,
) -> PassTotals {
    let verb = SweepVerb::from_env();
    let pace = Pace::from_env();
    let mut watch = WATCH
        .lock()
        .expect("ttl writer watch")
        .take()
        .unwrap_or_default();
    totals.cut = rounds::erase_rounds(
        host,
        &mut backlogs,
        shutdown,
        (verb, pace),
        &mut totals,
        &mut watch,
    )
    .await;
    *WATCH.lock().expect("ttl writer watch") = Some(watch);
    totals.keys_left = backlogs.iter().map(Backlog::left).sum();
    totals
}

/// Keys in the next batch of a schema. The pace sets the size. A schema with
/// many fields or alternate keys gets fewer keys, so the batch always fits in
/// one native batch (16,384 operations). One key at least.
fn batch_size(host: &Host, backlog: &Backlog, pace_batch: usize, verb: SweepVerb) -> usize {
    use fold_db::fold_db_core::purge::budget::max_erase_keys;
    let fits = host
        .db
        .db_ops()
        .batch_operation_limit()
        .map_or(usize::MAX, |limit| {
            max_erase_keys(verb == SweepVerb::Delete, backlog.shape, limit)
        });
    pace_batch.min(fits).min(backlog.batch_cap()).max(1)
}

/// True when the error says that the request was over a size limit (the
/// operation limit of a batch or the read budget of a request).
fn is_size_limit_error(error: &str) -> bool {
    error.contains("read resource limit exceeded")
        || error.contains("complete mutation limit exceeded")
        || error.contains("write resource limit")
}

/// What one batch did.
struct Batch {
    /// Keys that the batch sent to the erase.
    erased: usize,
    /// Keys that the re-check dropped because they were no longer expired.
    dropped: usize,
    /// Keys of a purge that were already gone and that the sweep dropped.
    missing: usize,
}

/// Re-check `chunk`, then erase the keys that are still expired.
async fn erase_batch(
    host: &Host,
    backlog: &mut Backlog,
    chunk: Vec<KeyValue>,
    verb: SweepVerb,
) -> Result<Batch, String> {
    let listed = chunk.len();
    let Some((keys, cutoff)) = still_expired(host, backlog, chunk).await? else {
        backlog.clear();
        tracing::info!(
            target: "lastdbd::ttl_sweep",
            schema = %backlog.schema_name,
            "retention policy is gone; the ttl sweep drops the rest of this schema"
        );
        return Ok(Batch {
            erased: 0,
            dropped: listed,
            missing: 0,
        });
    };
    let dropped = listed - keys.len();
    // A Hash or Single key ages by its record write time, the newest field
    // stamp, so the erase spares a record written at or after the cutoff and
    // takes its delete floor at the cutoff. A rewrite that is acked but not yet
    // in the age rows is spared this way. Range and HashRange keys age by the
    // key, so a rewrite does not change their age.
    let write_cutoff_nanos = matches!(
        backlog.schema_type,
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single
    )
    .then(|| cutoff.saturating_mul(1_000_000_000));
    let (erased, missing) =
        send_with_missing_retry(host, &backlog.schema_name, keys, (verb, write_cutoff_nanos))
            .await?;
    Ok(Batch {
        erased,
        dropped,
        missing,
    })
}

/// The keys of `chunk` that are expired now, under the policy now, and the
/// cutoff that decided it in unix seconds. `None` means the policy is gone.
///
/// The pass lists its keys once. A paced pass reaches a key long after the
/// listing, and a client can rewrite the key in that time. Range and HashRange
/// keys carry their age in the key, so a rewrite does not change it. Hash and
/// Single keys carry it in the write-time index, which the re-check reads in
/// one batch. A policy that now has a longer TTL protects keys that it
/// protected not when the pass listed them.
async fn still_expired(
    host: &Host,
    backlog: &Backlog,
    chunk: Vec<KeyValue>,
) -> Result<Option<(Vec<KeyValue>, u64)>, String> {
    let policy = host
        .db
        .db_ops()
        .get_schema_retention_policy(&backlog.schema_name)
        .await
        .map_err(|e| e.to_string())?;
    let Some(policy) = policy else {
        return Ok(None);
    };
    let cutoff = backlog
        .cutoff
        .min(unix_secs().saturating_sub(policy.ttl_seconds));
    match backlog.schema_type {
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => host
            .db
            .db_ops()
            .schemas()
            .retention_keys_still_expired(&backlog.schema_name, &chunk, cutoff)
            .await
            .map(|keys| Some((keys, cutoff)))
            .map_err(|e| e.to_string()),
        DeclarativeSchemaType::Range | DeclarativeSchemaType::HashRange => Ok(Some((
            chunk
                .into_iter()
                .filter(|key| key_is_expired(key, &backlog.schema_type, cutoff))
                .collect(),
            cutoff,
        ))),
    }
}

/// One line per batch, for the operator who asks where a batch spent its time.
/// A batch that dropped keys at the re-check is logged at info.
fn log_batch(schema_name: &str, batch: &Batch, took: Duration) {
    let Batch {
        erased,
        dropped,
        missing,
    } = *batch;
    if dropped > 0 {
        tracing::info!(
            target: "lastdbd::ttl_sweep",
            schema = schema_name,
            dropped,
            erased,
            "ttl sweep re-check dropped keys that are no longer expired"
        );
    }
    tracing::debug!(
        target: "lastdbd::ttl_sweep",
        schema = schema_name,
        keys = erased,
        dropped,
        missing,
        took_ms = took.as_millis() as u64,
        "ttl sweep batch erased"
    );
}
