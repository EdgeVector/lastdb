use super::*;

/// Per-call op cap for `--until-complete` when the caller named none.
///
/// The walk is `O(tips walked)`, so a larger cap no longer costs a re-scan of the
/// store — the cap now exists only to keep one call inside the admin handler
/// deadline ([`lastdb_node::exec::DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS`], 600s) and
/// to give the operator a progress line at a human interval. Each op can cost an
/// existence check, a body read, a two-key batch write and a verify read, so this
/// is deliberately well under the tombstone walk's 50,000 read-mostly keys.
pub(crate) const REKEY_UNTIL_COMPLETE_OPS: usize = 10_000;

/// Backoff bounds for `--until-complete`. A supervised restart takes a couple of
/// minutes; the ceiling is low enough that the migration resumes promptly after
/// one and high enough not to spin on the socket while it is legitimately gone.
pub(crate) const REKEY_RETRY_BACKOFF_START: Duration = Duration::from_secs(2);
pub(crate) const REKEY_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Give up only after the node has been unreachable for this long in a row.
pub(crate) const REKEY_RETRY_GIVE_UP_AFTER: Duration = Duration::from_secs(30 * 60);

/// One invocation's worth of `db rekey-atom-partition-prefix` flags.
#[derive(Clone, Copy)]
pub(crate) struct RekeyCliOpts {
    pub(crate) execute: bool,
    pub(crate) remove_flat: bool,
    pub(crate) max_ops: Option<usize>,
    pub(crate) tip_page: Option<usize>,
    pub(crate) audit_unresolved: Option<usize>,
    pub(crate) progress: bool,
    pub(crate) until_complete: bool,
    pub(crate) compact_after: bool,
    pub(crate) json_only: bool,
}

pub(crate) fn db_rekey_post(
    socket: &Path,
    body: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let value = db_post_json(socket, "/api/db/rekey-atom-partition-prefix", body)?;
    value
        .get("rekey_atom_partition_prefix")
        .cloned()
        .ok_or_else(|| "response missing rekey_atom_partition_prefix".to_string())
}

pub(crate) fn rekey_u64(report: &serde_json::Value, key: &str) -> u64 {
    report
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

pub(crate) fn rekey_flat_remaining(checkpoint: &serde_json::Value) -> u64 {
    rekey_u64(checkpoint, "dual_written").saturating_sub(rekey_u64(checkpoint, "flat_removed"))
}

pub(crate) fn add_rekey_progress_derived_fields(report: &mut serde_json::Value) {
    let remaining = report.get("checkpoint").map_or(0, rekey_flat_remaining);
    if let Some(checkpoint) = report
        .get_mut("checkpoint")
        .and_then(serde_json::Value::as_object_mut)
    {
        checkpoint.insert("flat_remaining".to_string(), serde_json::json!(remaining));
    }
}

/// Print the durable checkpoint — what the migration has actually done.
pub(crate) fn print_rekey_progress(report: &serde_json::Value) {
    let checkpoint = report.get("checkpoint").cloned().unwrap_or_default();
    println!("Atom partition-prefix rekey — DURABLE CHECKPOINT (read-only)");
    println!(
        "  completed:         {}",
        rekey_bool(&checkpoint, "completed")
    );
    println!(
        "  tips_walked_total: {}",
        rekey_u64(&checkpoint, "tips_walked_total")
    );
    for key in [
        "dual_written",
        "already_prefixed",
        "missing_body",
        "flat_removed",
    ] {
        println!("  {key}: {}", rekey_u64(&checkpoint, key));
    }
    println!(
        "  flat_remaining:    {} (dual_written - flat_removed)",
        rekey_flat_remaining(&checkpoint)
    );
    println!(
        "  cursor:            {}",
        checkpoint
            .get("after_tip_key")
            .and_then(serde_json::Value::as_str)
            .map_or_else(
                || "<start of keyspace>".to_string(),
                |k| k.escape_default().to_string(),
            )
    );
    if !rekey_bool(&checkpoint, "completed") {
        println!();
        println!("NOT COMPLETE. Resume with: --execute --until-complete");
    }
}

pub(crate) fn rekey_bool(value: &serde_json::Value, key: &str) -> bool {
    value
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// Drive `--execute` passes until the checkpoint says the migration is complete.
///
/// Transport failures are retryable by design: the node going away mid-call is
/// what an authorized `lastdb-safe-upgrade` or `launchctl kickstart` looks like
/// from here, and the migration's progress lives in the node's durable
/// checkpoint, not in this loop's counters. So the loop reconnects with backoff
/// and lets the node resume from its own cursor. Only an error the node itself
/// returned — a real refusal, not a lost socket — aborts.
pub(crate) fn db_rekey_until_complete(
    socket: &Path,
    remove_flat: bool,
    max_ops: usize,
    tip_page: Option<usize>,
    json_only: bool,
) -> Result<(), String> {
    let mut body = serde_json::json!({
        "dry_run": false,
        "remove_flat": remove_flat,
        "max_ops": max_ops,
    });
    if let Some(n) = tip_page {
        body["tip_page"] = serde_json::json!(n);
    }
    let mut backoff = REKEY_RETRY_BACKOFF_START;
    let mut unreachable_since: Option<Instant> = None;
    let mut passes = 0u64;
    loop {
        match db_rekey_post(socket, &body) {
            Ok(report) => {
                backoff = REKEY_RETRY_BACKOFF_START;
                unreachable_since = None;
                passes += 1;
                let checkpoint = report.get("checkpoint").cloned().unwrap_or_default();
                if !json_only {
                    println!(
                        "pass={passes} page={} walked={} dual_written={} already_prefixed={} \
                         missing_body={} total_walked={} completed={}",
                        rekey_u64(&report, "tip_page"),
                        rekey_u64(&report, "tips_scanned"),
                        rekey_u64(&report, "dual_written"),
                        rekey_u64(&report, "already_prefixed"),
                        rekey_u64(&report, "missing_body"),
                        rekey_u64(&checkpoint, "tips_walked_total"),
                        rekey_bool(&report, "completed"),
                    );
                }
                if rekey_bool(&report, "completed") {
                    if json_only {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&report)
                                .map_err(|e| format!("serialize: {e}"))?
                        );
                    } else {
                        println!();
                        print_rekey_progress(&report);
                    }
                    return Ok(());
                }
                // A pass that walked nothing and did not complete would spin.
                if rekey_u64(&report, "tips_scanned") == 0 {
                    return Err(
                        "rekey pass walked 0 tips without completing — cursor is not advancing"
                            .to_string(),
                    );
                }
            }
            Err(e) if is_retryable_transport_error(&e) => {
                let since = *unreachable_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= REKEY_RETRY_GIVE_UP_AFTER {
                    return Err(format!(
                        "node unreachable for {}s while resuming rekey — last error: {e}",
                        since.elapsed().as_secs()
                    ));
                }
                if !json_only {
                    println!(
                        "node unreachable ({e}) — retrying in {}s (restarts are expected)",
                        backoff.as_secs()
                    );
                }
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(REKEY_RETRY_BACKOFF_MAX);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Does this error mean "the node was not there", rather than "the node said no"?
///
/// `no_http_body` is the in-flight request when the daemon begins shutting down;
/// the file-not-found family is the unix socket being absent between a stop and
/// the next start. Both were treated as fatal by the shell loop that parked this
/// migration.
pub(crate) fn is_retryable_transport_error(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    e.contains("no_http_body")
        || e.contains("no such file or directory")
        || e.contains("connection refused")
        || e.contains("connection reset")
        || e.contains("broken pipe")
        || e.contains("not reachable")
        || e.contains("timed out")
        || e.contains("os error 2")
        || e.contains("os error 61")
}
