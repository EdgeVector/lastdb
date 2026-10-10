//! LastStore sealed-chunk cloud backup uploader + continuous snapshot+log publish.
//!
//! Continuous v1 path (design-lastdb-cloud-sync-snapshot-log):
//! 1. Upload sealed local files (chunks) as-is
//! 2. Periodically cut snapshot S with frontier F (= cut_csn)
//! 3. CAS `latest` → (S, F, counter) — only atomic step
//! 4. Mark mutation-log segments fully ≤ F as GC-eligible only after CAS
//!
//! Never blocks local Mini R/W (runs on a dedicated throttled thread).

use super::gc_jobs::GcObjectOutcome;
use super::*;
use crate::storage::laststore::{
    apply_cas_proven_unbackable_retirement, apply_named_hole_exclusions,
    cas_proven_named_hole_shas, cas_proven_unbackable_atom_shas, cloud_db_hash_for_store_uuid,
    compute_backup_storage_footprint, manifest_referenced_chunk_shas, manifest_sha256_hex,
    select_orphan_backup_chunk_shas, unbackable_manifest_chunk_count, validate_manifest_chain,
    validate_manifest_chain_before_packing, BackupChunkUploadCandidate, BackupManifest,
    BackupManifestRole, BackupPackLocation, BackupStorageFootprint, CloudChunkPresence,
    MANIFEST_VERSION, PACKED_MANIFEST_VERSION,
};
use crate::sync::auth::ops::BackupLatestPointer;
use crate::sync::snapshot_log::{
    gc_eligibility_for_log, Frontier, GcEligibility, LatestCasPayload, MutationLogSegmentId,
    PublishPhase, SnapshotRecord,
};
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

mod primary_resume_cut;
mod primary_resume_rescue;
use primary_resume_cut::check_damage_mode;
use primary_resume_rescue::FreshCloudProof;

/// Live `laststore-backup-uploader` std threads in this process.
///
/// Boot-error tests assert this is zero when factory open fails after
/// migrations. Incremented after a successful spawn; decremented when the
/// thread function returns.
static LIVE_BACKUP_UPLOADER_THREADS: AtomicUsize = AtomicUsize::new(0);

const DEFAULT_BACKUP_UPLOADER_INTERVAL_SECS: u64 = 30;
/// How often the continuous publisher cuts S + CAS latest after chunk drain.
/// This 120s path must never await photograph-aligned compact-if-dirty (D3).
const DEFAULT_SNAPSHOT_LOG_PUBLISH_INTERVAL_SECS: u64 = 120;
/// A restore base older than this re-enters backup catch-up mode.
///
/// Six hours is a few multiples of the measured healthy full-cut duration,
/// while still leaving a fresh committed snapshot on the foreground-friendly
/// steady-state budget. Operators may override it with
/// `LASTDB_BACKUP_CATCHUP_STALENESS_SECS`; the value is bounded to 5 minutes
/// through 7 days so a typo cannot permanently force either regime.
const DEFAULT_BACKUP_CATCHUP_STALENESS_SECS: u64 = 6 * 60 * 60;
const MIN_BACKUP_CATCHUP_STALENESS_SECS: u64 = 5 * 60;
const MAX_BACKUP_CATCHUP_STALENESS_SECS: u64 = 7 * 24 * 60 * 60;
/// Durable sidecar: known-present backup chunk digests survive restarts so a
/// post-reboot drain does not re-probe O(chunks)×RTT.
const BACKUP_PRESENCE_CACHE_FILE: &str = "laststore_backup_known_present.json";
/// Parallel orphan DELETE batch. Presign stays outside the publication turn;
/// the turn is held only across the batch's HTTP DELETEs (~one RTT).
const BACKUP_GC_DELETE_BATCH: usize = 16;

mod candidates;
mod cas_inner;
mod chunk_upload;
mod cloud_snapshot;
mod cut_policy;
mod file_packs;
mod gc_execute;
mod gc_keep_set;
mod gc_run;
mod gc_test_hooks;
mod gc_types;
mod manifest_presence;
mod policy_helpers;
mod progress_recovery;
mod publish_cycle;
mod publish_target;
mod retirement;
mod stats_types;
mod uploader_loop;

use candidates::*;
pub use cut_policy::*;
pub use gc_types::*;
use policy_helpers::*;
use retirement::*;
pub use stats_types::*;

#[cfg(target_os = "macos")]
fn throttle_this_thread_io() {
    // Best-effort. macOS applies this policy to the calling OS thread, which is
    // why the uploader uses a dedicated thread instead of a Tokio task.
    unsafe extern "C" {
        fn setiopolicy_np(
            iotype: libc::c_int,
            scope: libc::c_int,
            policy: libc::c_int,
        ) -> libc::c_int;
    }
    const IOPOL_TYPE_DISK: libc::c_int = 0;
    const IOPOL_SCOPE_THREAD: libc::c_int = 1;
    const IOPOL_THROTTLE: libc::c_int = 3;

    unsafe {
        let rc = setiopolicy_np(IOPOL_TYPE_DISK, IOPOL_SCOPE_THREAD, IOPOL_THROTTLE);
        if rc != 0 {
            tracing::warn!(
                target: "fold_db::sync::backup",
                os_error = %std::io::Error::last_os_error(),
                "failed to set backup uploader IO throttle"
            );
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn throttle_this_thread_io() {}
