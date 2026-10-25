//! Round-robin discovery and erase, with only one key page held globally.

use super::*;

pub(super) async fn erase_rounds(
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
            if shutdown.load(Ordering::Acquire) || backup_cut_is_held(host).await {
                return true;
            }
            let size = batch_size(host, backlog, options.1.batch, options.0);
            if let Err(error) = super::super::discovery::fill_page(host, backlog, size).await {
                tracing::warn!(target: "lastdbd::ttl_sweep", schema = %backlog.schema_name,
                    error = %error, "ttl discovery failed; skip this schema for this pass");
                backlog.abandon();
                totals.schemas_failed += 1;
                continue;
            }
            totals.peak_buffered_keys = totals.peak_buffered_keys.max(backlog.page_left());
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
/// batch still releases admission and observes the existing duty pause.
async fn erase_page(
    host: &Host,
    backlog: &mut Backlog,
    shutdown: &AtomicBool,
    (verb, pace): (SweepVerb, Pace),
    totals: &mut PassTotals,
    watch: &mut WriterWatch,
) -> bool {
    while backlog.page_left() > 0 {
        if shutdown.load(Ordering::Acquire) || backup_cut_is_held(host).await {
            return true;
        }
        let size = batch_size(host, backlog, pace.batch, verb);
        let chunk = backlog.take(size);
        let taken = chunk.len();
        let started = Instant::now();
        let admitted = crate::maintenance_write_gate::run(
            host,
            WriterWatch::measure(run_with_phases(erase_batch(host, backlog, chunk, verb))),
        )
        .await;
        let Some(((outcome, phases, _counts), blocked, scopes)) = admitted else {
            // Count this unaccepted batch as outstanding before the pass ends.
            backlog.shrink_after_limit(taken);
            return true;
        };
        let pause = pace.pause_after(blocked, watch.writer_present(scopes));
        match outcome {
            Ok(done) => {
                totals.erased += done.erased;
                totals.dropped += done.dropped;
                totals.missing += done.missing;
                log_batch(&backlog.schema_name, &done, started.elapsed(), &phases);
            }
            Err(error) if taken > 1 && is_size_limit_error(&error) => {
                backlog.shrink_after_limit(taken);
                tracing::warn!(target: "lastdbd::ttl_sweep", schema = %backlog.schema_name,
                    keys = taken, error = %error, "ttl size refusal; retry a smaller batch");
            }
            Err(error) => {
                backlog.shrink_after_limit(taken);
                backlog.abandon();
                totals.schemas_failed += 1;
                tracing::warn!(target: "lastdbd::ttl_sweep", schema = %backlog.schema_name,
                    error = %error, "ttl erase failed; skip this schema for this pass");
                return false;
            }
        }
        if !backlog.is_done() {
            rest(pause, shutdown, watch).await;
        }
    }
    false
}
