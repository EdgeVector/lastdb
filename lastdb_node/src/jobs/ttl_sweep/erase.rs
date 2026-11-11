//! The erase step of the TTL sweep: which verb, a re-check of each key before
//! its batch, one admission per batch, one batch per schema in turn, and the
//! rest between batches.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fold_db::clock::unix_secs;
use fold_db::db_operations::WriterWatch;
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;

use super::discovery::{self, Backlog};
use super::key_is_expired;
use super::send::{send_with_missing_retry, Pace, SweepVerb};
use crate::host::Host;

static WATCH: std::sync::Mutex<Option<WriterWatch>> = std::sync::Mutex::new(None);

const REST_SLICE: Duration = Duration::from_millis(250);

/// What one pass did.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct PassTotals {
    /// Keys that a batch sent to the erase. A key that the write-time guard
    /// spared counts here; the delete ledger counts only the records it erased.
    pub(super) erased: usize,
    /// Keys that the re-check dropped because they were no longer expired.
    pub(super) dropped: usize,
    /// Keys that the pass listed and had not reached when it ended.
    pub(super) keys_left: usize,
    /// The pass ended early: the node was stopping or local writes were paused.
    pub(super) cut: bool,
    /// Schemas with a policy that the pass tried.
    pub(super) schemas: usize,
    /// Schemas that the pass skipped because their listing or a batch failed.
    pub(super) schemas_failed: usize,
}

impl PassTotals {
    /// Every schema failed, so the pass did no useful work.
    pub(super) fn whole_pass_failed(&self) -> bool {
        self.schemas > 0 && self.schemas_failed >= self.schemas
    }
}

/// Erase the backlogs in rounds: one batch from each schema that still has
/// keys, then the rest, then the next round.
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
    totals.cut = erase_rounds(
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

async fn erase_rounds(
    host: &Host,
    backlogs: &mut [Backlog],
    shutdown: &AtomicBool,
    options: (SweepVerb, Pace),
    totals: &mut PassTotals,
    watch: &mut WriterWatch,
) -> bool {
    while backlogs.iter().any(|backlog| !backlog.is_done()) {
        for backlog in &mut *backlogs {
            if backlog.is_done() {
                continue;
            }
            if shutdown.load(Ordering::Acquire) {
                return true;
            }
            let size = batch_size(host, backlog, options.1.batch, options.0);
            if let Err(error) = discovery::fill_page(host, backlog, size).await {
                tracing::warn!(
                    target: "lastdbd::ttl_sweep",
                    schema = %backlog.schema_name,
                    error = %error,
                    "ttl discovery failed; skip this schema for this pass"
                );
                backlog.abandon();
                totals.schemas_failed += 1;
                continue;
            }
            if erase_page(host, backlog, shutdown, options, totals, watch).await {
                return true;
            }
            // No second schema can hold a page while this one still owns it.
            // A failed page keeps only its outstanding count, never its keys.
            backlog.release_page();
        }
    }
    false
}

/// Drain this one page, including smaller retries after size refusals. Each
/// batch observes the existing duty pause.
async fn erase_page(
    host: &Host,
    backlog: &mut Backlog,
    shutdown: &AtomicBool,
    (verb, pace): (SweepVerb, Pace),
    totals: &mut PassTotals,
    watch: &mut WriterWatch,
) -> bool {
    while backlog.page_left() > 0 {
        if shutdown.load(Ordering::Acquire) {
            return true;
        }
        let size = batch_size(host, backlog, pace.batch, verb);
        let chunk = backlog.take(size);
        let taken = chunk.len();
        let started = Instant::now();
        let (outcome, blocked, scopes) = Box::pin(WriterWatch::measure(erase_batch(
            host, backlog, chunk, verb,
        )))
        .await;
        let pause = pace.pause_after(blocked, watch.writer_present(scopes));
        match outcome {
            Ok(done) => {
                totals.erased += done.erased;
                totals.dropped += done.dropped;
                log_batch(&backlog.schema_name, &done, started.elapsed());
            }
            Err(error) if taken > 1 && is_size_limit_error(&error) => {
                backlog.shrink_after_limit(taken);
                tracing::warn!(
                    target: "lastdbd::ttl_sweep",
                    schema = %backlog.schema_name,
                    keys = taken,
                    error = %error,
                    "ttl size refusal; retry a smaller batch"
                );
            }
            Err(error) => {
                backlog.shrink_after_limit(taken);
                backlog.abandon();
                totals.schemas_failed += 1;
                tracing::warn!(
                    target: "lastdbd::ttl_sweep",
                    schema = %backlog.schema_name,
                    error = %error,
                    "ttl erase failed; skip this schema for this pass"
                );
                return false;
            }
        }
        if !backlog.is_done() {
            rest(pause, shutdown, watch).await;
        }
    }
    false
}

/// Rest for `pause`. The slice is 250 ms; ends early on shutdown or when no
/// other task has opened a write scope within the quiet period.
async fn rest(pause: Duration, shutdown: &AtomicBool, watch: &mut WriterWatch) {
    let end = Instant::now() + pause;
    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return;
        }
        tokio::time::sleep(left.min(REST_SLICE)).await;
        if left > REST_SLICE && watch.rest_may_end() {
            return;
        }
    }
}

/// Keys in the next batch of a schema.
fn batch_size(host: &Host, backlog: &Backlog, pace_batch: usize, verb: SweepVerb) -> usize {
    use fold_db::fold_db_core::purge::budget::max_erase_keys;
    let fits = max_erase_keys(
        verb == SweepVerb::Delete,
        backlog.shape,
        host.db.db_ops().batch_operation_limit(),
    );
    pace_batch.min(fits).min(backlog.batch_cap()).max(1)
}

fn is_size_limit_error(error: &str) -> bool {
    error.contains("read resource limit exceeded")
        || error.contains("complete mutation limit exceeded")
        || error.contains("write resource limit")
}

/// What one batch did.
struct Batch {
    erased: usize,
    dropped: usize,
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
