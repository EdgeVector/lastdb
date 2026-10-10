use super::status::SyncReplayBlocker;
use crate::sync::error::{redact_sync_error_text, SyncError};

/// Which kind of replay operation a key rewrite is being computed for.
///
/// The org-schema prefix-strip in [`SyncEngine::rewrite_key_if_needed`](crate::sync::engine::SyncEngine::rewrite_key_if_needed)
/// applies to writes only: a delete must never strip an org-routing companion
/// key down to the bare schema name, or it would clobber the receiver's
/// canonical schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyRewriteOp {
    /// `Put` / `BatchPut` — store the org schema under its bare name.
    Write,
    /// `Delete` / `BatchDelete` — leave org-prefixed schema keys intact so only
    /// the routing companion is removed, never the canonical schema.
    Delete,
}

pub(crate) fn replay_blocker_from_error(err: &SyncError) -> Option<SyncReplayBlocker> {
    match err {
        SyncError::CorruptEntry {
            target,
            seq,
            reason,
        } => Some(SyncReplayBlocker {
            code: "cloud_replay_corrupt_entry".to_string(),
            target: target.clone(),
            seq: *seq,
            reason: redact_sync_error_text(reason),
            action: "Cloud replay stopped before this sequence. Do not upload more entries for this target; repair or quarantine the cloud log prefix, then retry sync.".to_string(),
        }),
        // Any apply failure that pinned the cursor, whatever its cause. The two
        // arms above name a *diagnosis* (the cloud object is corrupt; this
        // device's key is wrong); this one names only the *fact* — replay
        // stopped, here. That is deliberately weaker and deliberately
        // unconditional: the four-hour outage this arm exists for was a
        // deterministic local policy refusal, a cause no allowlist of error
        // variants had anticipated, and the cost of not naming it was that the
        // one field designed to name it read `null` for the whole outage.
        //
        // Arming on a transient apply failure too is acceptable and honest: at
        // that instant the cursor genuinely has not advanced past this seq. The
        // blocker is sticky but `record_sync_success` clears it, so one good
        // cycle retires it. What it buys meanwhile is the forced-sync rate limit
        // in `SyncCoordinator::force_sync_inner`, which stops a UI "sync now"
        // from hot-retrying the same doomed seq.
        SyncError::ReplayApplyFailed {
            target,
            seq,
            reason,
            ..
        } => Some(SyncReplayBlocker {
            code: "cloud_replay_apply_failed".to_string(),
            target: target.clone(),
            seq: *seq,
            reason: redact_sync_error_text(reason),
            action: "Cloud replay stopped applying at this sequence, so the download cursor is pinned and this target's uploads are blocked behind it — backups stop until it clears. If the reason looks transient (IO, disk pressure), the next cycle recovers on its own. If it repeats on the same seq across cycles on this build, clear it without destroying peer data: `lastdb cloud quarantine-replay --target <label> --seq <n>` (skip-local tombstone; does not delete the cloud object). Capture the target/seq for a product fix on the replay path.".to_string(),
        }),
        SyncError::KeyProofFailed { target, reason } => Some(SyncReplayBlocker {
            code: "cloud_sync_key_mismatch".to_string(),
            target: target.clone(),
            // Not tied to one bad seq — the whole prefix failed the pre-upload
            // decrypt proof.
            seq: 0,
            reason: redact_sync_error_text(reason),
            action: "This device's sync key cannot decrypt the existing cloud data, so uploads are blocked to avoid poisoning the shared log/snapshot. Restore the correct account key or mnemonic, then retry sync.".to_string(),
        }),
        _ => None,
    }
}

/// Rank two sync-cycle transfer errors and keep the one an operator most needs
/// to see. A poison signal (corrupt cloud object, or a failed pre-upload
/// decrypt proof) must not be masked behind a transient network error raised by
/// a *different* target in the same cycle: the network error clears on the next
/// cycle, but the poison error is what pins the cursor and arms the replay
/// blocker. On a tie, the earlier-seen error wins (stable).
pub(crate) fn is_poison_transfer_error(err: &SyncError) -> bool {
    matches!(
        err,
        SyncError::CorruptEntry { .. }
            | SyncError::KeyProofFailed { .. }
            // A pinned apply is the same shape of problem for this ranking: it
            // is the error that stopped the cursor, so a network error raised
            // by a *different* target in the same cycle must not be the one the
            // operator is shown.
            | SyncError::ReplayApplyFailed { .. }
    )
}

pub(crate) fn select_more_severe_transfer_error(
    existing: Option<SyncError>,
    candidate: SyncError,
) -> SyncError {
    match existing {
        Some(existing) => {
            if is_poison_transfer_error(&candidate) && !is_poison_transfer_error(&existing) {
                candidate
            } else {
                existing
            }
        }
        None => candidate,
    }
}
