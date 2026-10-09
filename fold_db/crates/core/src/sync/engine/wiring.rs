//! SyncEngine construction, wiring, status, and capacity helpers.

use super::super::auth::{AuthClient, AuthRefreshCallback};
use super::super::org_sync::SyncTarget;
use super::super::s3::S3Client;
use crate::crypto::{open_at_rest, seal_at_rest, CryptoProvider};
use crate::security::Ed25519KeyPair;
use crate::storage::traits::NamespacedStore;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::*;

mod cloud_toggle;
mod construct;
mod setters;
mod status;

/// Decide `MutationLogPlaneStatus::lag_degraded` from the **seconds**-scale
/// recovery point age and a **seconds**-scale threshold.
///
/// Both operands are seconds. This is the whole point of the function: the
/// previous form compared `log_lag` — a difference of two nanosecond frontier
/// watermarks — against a seconds-scale threshold of `32`, so it read `true`
/// from a few seconds of lag through several minutes of lag and `false` only
/// at the exact instant lag was `0`. A real transport stall was then
/// indistinguishable from a healthy node mid-publish-cycle.
///
/// `recovery_point_age_secs` is `None` until a cloud upload has been confirmed
/// in this process. That is the ordinary state of a fresh node, not evidence
/// of divergence, so it reads healthy — the same posture
/// `MutationLogPlaneStatus::capture_registered` documents for a node that has
/// not yet registered a capture runtime. "Recording but never confirmed" is a
/// separate signal and is not this flag's job.
///
/// `threshold_secs == 0` disables the trigger; status still reports lag and F.
///
/// **This trigger reads publish liveness on purpose, not the crash loss
/// window.** `recovery_point_age_secs` is the age of the last publish EVENT;
/// the data at the recovery point is older still, by however far F lagged when
/// that publish ran (measured at 3x on the live primary 2026-09-06). The status
/// text now prints the larger, data-side number as the RPO — see
/// `rpo_secs_from_frontier` in `lastdb_node::ops::self_metrics`.
///
/// The two fail independently and this threshold was chosen against the
/// liveness meaning: 32 seconds is a statement about how long an uploader may
/// go quiet, and the same 32 seconds against data staleness would read degraded
/// on a healthy node whose publish cycle is minutes wide. Pointing this trigger
/// at the other clock is therefore a separate, deliberate alerting change with
/// its own threshold, not a follow-on edit to a rendering fix.
///
/// **A quiet uploader with nothing to publish is idle, not stalled.** The
/// publish event age only resets when a publish runs, and a publish only runs
/// when something sealed is still unpublished (`upload_backlog > 0`). On a
/// caught-up node the age therefore grows at one second per second by
/// construction — every recorded false positive since the seconds fix
/// (2026-09-05 through 2026-09-13, primary builds 0.23.3-1784 to -1908) read
/// `log_lag=0 rpo_secs=0 publish_age_secs=119..867 degraded=true`. That is
/// the same short-circuit `rpo_secs_from_frontier` already applies: zero lag
/// means no unprotected data, so there is no divergence for the age to
/// measure. The trigger needs both operands — a backlog that exists AND a
/// publisher that has not moved it for `threshold_secs` — before it reads
/// degraded. A dead uploader still trips the moment the next local write
/// seals, because the backlog is then nonzero and the age keeps growing.
fn mutation_log_lag_degraded(
    recovery_point_age_secs: Option<u64>,
    upload_backlog: u64,
    threshold_secs: u64,
) -> bool {
    if upload_backlog == 0 {
        return false;
    }
    match recovery_point_age_secs {
        Some(age_secs) => threshold_secs > 0 && age_secs >= threshold_secs,
        None => false,
    }
}
