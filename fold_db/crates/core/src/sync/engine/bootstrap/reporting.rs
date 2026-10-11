//! Pure helpers for bootstrap log selection and logging.

use super::parse_mutation_log_object_key;

/// Sequence numbers of legacy flat log objects newer than the snapshot.
pub(super) fn legacy_log_seqs_after(
    log_objects: &[crate::sync::auth::S3ObjectInfo],
    snapshot_last_seq: u64,
    has_photograph: bool,
) -> Vec<u64> {
    let mut seqs: Vec<u64> = log_objects
        .iter()
        .filter_map(|obj| {
            let (writer, seq) = parse_mutation_log_object_key(&obj.key)?;
            // A named photograph uses the writer-scoped mutation-log
            // reader. Keep this loop for legacy flat objects only.
            if has_photograph && writer.is_some() {
                None
            } else {
                Some(seq)
            }
        })
        .filter(|seq| *seq > snapshot_last_seq)
        .collect();
    seqs.sort();
    seqs
}

pub(super) fn log_fresh_bootstrap(label: &str, snapshot_present: bool, log_len: usize) {
    if snapshot_present {
        return;
    }
    if log_len == 0 {
        tracing::info!(
            "no snapshot and no log history for '{label}' — bootstrapping a fresh empty store"
        );
    } else {
        // Reachable only with accept_fresh set (the guard errors
        // otherwise): the operator explicitly chose a fresh start.
        tracing::warn!(
            "no snapshot found for '{label}' but accept_fresh is set — starting fresh despite {log_len} existing log entr{} in the cloud prefix",
            if log_len == 1 { "y" } else { "ies" }
        );
    }
}

pub(super) fn log_bootstrap_complete(
    label: &str,
    last_seq: u64,
    entries_replayed: usize,
    org_skips: u64,
) {
    if org_skips > 0 {
        tracing::warn!(
            target: "fold_db::sync::bootstrap",
            target = %label,
            org_scoped_keys_skipped = org_skips,
            "bootstrap of '{label}' complete at seq {last_seq} ({entries_replayed} entries replayed); \
             SKIPPED {org_skips} org-scoped storage key(s) (consented drop after org-crypto strip)"
        );
    } else {
        tracing::info!(
            "bootstrap of '{label}' complete at seq {last_seq} ({entries_replayed} entries replayed)"
        );
    }
}
