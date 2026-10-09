//! Age-based durability health: "how long has it been since our data reached
//! the cloud?", answered without a sync engine.
//!
//! # Why this is not part of [`crate::backup_progress`]
//!
//! Backup progress is engine state. It exists only while the uploader is
//! constructed, so every field an alarm keys on — failure streaks, cycle
//! progress, last-success — is absent exactly when backup is switched off.
//! Measured on the primary on 2026-07-29: sync had been disabled for two days
//! and off-machine backup stale for nine, and `GET /api/status` reported
//! `sync_degraded=false`, `consecutive_sync_failures=null`, with the whole
//! `Backup:` line suppressed. Nothing was failing, because nothing was running.
//!
//! A disabled backup and a failing backup are the same outcome for the data, so
//! the honest signal is the **age of the last committed manifest**, read from
//! the durable on-disk marker and evaluated whether or not the engine exists.
//!
//! # Fail-loud policy
//!
//! Unknown is reported as degraded, never as healthy. A marker that predates
//! commit-time stamping, an unreadable marker, and a home that has never backed
//! up are all states where we cannot show the data is covered — and a
//! durability alarm that stays quiet when it cannot tell is the defect this
//! module exists to remove. Each unknown self-heals at the next committed
//! manifest.

use crate::storage::laststore::{read_backup_durability, BackupDurability};
use std::path::Path;

/// Default age past which a store is reported as degraded: one day.
pub const DEFAULT_MAX_BACKUP_AGE_SECS: u64 = 86_400;

/// Env var overriding [`DEFAULT_MAX_BACKUP_AGE_SECS`]. `0` disables the age
/// check (the never/unknown checks still apply — those are not thresholds).
pub const MAX_BACKUP_AGE_ENV: &str = "LASTDB_BACKUP_MAX_AGE_SECS";

/// Machine-readable reasons a store's durability is degraded.
pub mod reason {
    /// A backup committed, but longer ago than the configured threshold.
    pub const STALE: &str = "backup_age_over_threshold";
    /// No backup manifest has ever committed on this home.
    pub const NEVER: &str = "backup_never_completed";
    /// Backups happened, but this home cannot say when (pre-stamp marker).
    pub const UNKNOWN_AGE: &str = "backup_age_unknown";
    /// The durable marker is missing or unreadable.
    pub const NO_MARKER: &str = "backup_marker_unreadable";
    /// The cut currently held names chunks it has no local source for, so it
    /// can never CAS. Known at cut time, unlike [`STALE`], which can only fire
    /// once the age threshold elapses.
    pub const CUT_UNBACKABLE: &str = "backup_cut_unbackable";
}

/// Durability verdict for one store, independent of sync engine state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackupDurabilityHealth {
    /// Highest committed manifest counter, when the marker could be read.
    pub backup_manifest_counter: Option<u64>,
    /// Unix seconds of the last committed manifest, when known.
    pub last_backup_commit_ts: Option<u64>,
    /// Age of the last committed manifest in seconds, when known.
    pub age_secs: Option<u64>,
    /// Threshold in force for this evaluation. `0` means the age check is off.
    pub max_age_secs: u64,
    /// Whether durability needs attention.
    pub degraded: bool,
    /// Machine-readable [`reason`] codes, empty when healthy.
    pub reasons: Vec<String>,
}

/// Resolve the age threshold from the environment.
pub fn max_backup_age_secs_from_env() -> u64 {
    env_flag::var_or(MAX_BACKUP_AGE_ENV, DEFAULT_MAX_BACKUP_AGE_SECS)
}

/// Evaluate durability from already-read marker facts.
///
/// Split from the disk read so the policy is unit-testable without a store, and
/// so a caller that already holds the marker does not re-read it.
pub fn evaluate(
    durability: Option<&BackupDurability>,
    now_unix_secs: u64,
    max_age_secs: u64,
) -> BackupDurabilityHealth {
    let Some(d) = durability else {
        return BackupDurabilityHealth {
            max_age_secs,
            degraded: true,
            reasons: vec![reason::NO_MARKER.to_string()],
            ..Default::default()
        };
    };

    let mut health = BackupDurabilityHealth {
        backup_manifest_counter: Some(d.backup_manifest_counter),
        last_backup_commit_ts: d.last_backup_commit_unix_secs,
        max_age_secs,
        ..Default::default()
    };

    match d.last_backup_commit_unix_secs {
        Some(ts) => {
            // A stamp in the future is clock skew, not freshness; saturating to
            // zero reports "just backed up" rather than a nonsense age.
            let age = now_unix_secs.saturating_sub(ts);
            health.age_secs = Some(age);
            if max_age_secs > 0 && age > max_age_secs {
                health.degraded = true;
                health.reasons.push(reason::STALE.to_string());
            }
        }
        None if d.backup_manifest_counter == 0 => {
            health.degraded = true;
            health.reasons.push(reason::NEVER.to_string());
        }
        None => {
            health.degraded = true;
            health.reasons.push(reason::UNKNOWN_AGE.to_string());
        }
    }

    health
}

/// Fold the engine's terminal-cut verdict into an age-derived health.
///
/// Kept separate from [`evaluate`] because that function must answer with no
/// engine at all — the disabled-backup case it exists for. This is the opposite
/// half: a fact only a running uploader knows, which the age check cannot reach.
///
/// Why it is worth a second signal. The age threshold is a 24-hour fallback for
/// a condition the node proves the moment it cuts: `cut_backup_manifest` walks
/// the store UNION the previous manifest's atom chunks, so a carried-forward ref
/// that reseal removed from disk is named by the cut and has no candidate to
/// upload it from. The count is computed and WARNed right there. Without this,
/// a home provably unable to publish reads `degraded: false` for 23h59m.
///
/// Additive only: it can raise `degraded`, never lower it, and never removes a
/// reason another check added.
pub fn apply_cut_unbackable(health: &mut BackupDurabilityHealth, unbackable_manifest_chunks: u64) {
    if unbackable_manifest_chunks == 0 {
        return;
    }
    health.degraded = true;
    let reason = reason::CUT_UNBACKABLE.to_string();
    if !health.reasons.contains(&reason) {
        health.reasons.push(reason);
    }
}

/// Read the durable marker for `store_root` and evaluate it.
pub fn evaluate_store(
    store_root: &Path,
    now_unix_secs: u64,
    max_age_secs: u64,
) -> BackupDurabilityHealth {
    let durability = read_backup_durability(store_root);
    evaluate(durability.as_ref(), now_unix_secs, max_age_secs)
}

/// One always-present operator line for `lastdb status` when Cloud Sync is
/// **off** (or the mutation-log plane is not recording).
///
/// Unlike the progress line, this never returns `None`: the state worth
/// alarming on is precisely the one with no progress to report. Photo age
/// **is** RPO in this mode — see [`snapshot_line`] for the recording case.
pub fn status_line(health: &BackupDurabilityHealth, sync_enabled: bool) -> String {
    let sync_note = if sync_enabled {
        String::new()
    } else {
        " · sync disabled".to_string()
    };
    let counter = health
        .backup_manifest_counter
        .map_or_else(String::new, |c| format!(" · manifest #{c}"));

    if !health.degraded {
        let age = health
            .age_secs
            .map_or_else(|| "unknown".to_string(), format_age_secs);
        return format!("Backup durability: last commit {age} ago{counter}{sync_note}");
    }

    let detail = if health.reasons.iter().any(|r| r == reason::NO_MARKER) {
        "durable marker unreadable — cannot prove any data reached the cloud".to_string()
    } else if health.reasons.iter().any(|r| r == reason::NEVER) {
        "no backup has ever completed — nothing of this home is in the cloud".to_string()
    } else if health.reasons.iter().any(|r| r == reason::UNKNOWN_AGE) {
        "last commit time unknown (marker predates commit stamping)".to_string()
    } else {
        format!(
            "last commit {} ago, over the {} threshold",
            health
                .age_secs
                .map_or_else(|| "unknown".to_string(), format_age_secs),
            format_age_secs(health.max_age_secs)
        )
    };

    format!("Backup durability: DEGRADED — {detail}{counter}{sync_note}")
}

/// Operator line for a committed sealed snapshot while the mutation-log plane
/// is recording. Photograph age is **not** crash RPO in this mode.
///
/// Still DEGRADED when there is no usable S0 (never / unknown / unreadable).
/// A merely *stale* photograph is labeled as a photograph, not as uncovered data.
pub fn snapshot_line(health: &BackupDurabilityHealth) -> String {
    let counter = health
        .backup_manifest_counter
        .map_or_else(|| "none".to_string(), |c| format!("#{c}"));
    let age = health
        .age_secs
        .map_or_else(|| "unknown".to_string(), format_age_secs);

    let missing_s0 = health
        .reasons
        .iter()
        .any(|r| r == reason::NO_MARKER || r == reason::NEVER || r == reason::UNKNOWN_AGE);
    if missing_s0 {
        return status_line(health, true);
    }

    format!("Snapshot: last committed {counter}, {age} ago (photograph; not crash RPO)")
}

fn format_age_secs(secs: u64) -> String {
    if secs < 60 {
        return format!("{secs}s");
    }
    let mins = secs / 60;
    if mins < 60 {
        return format!("{mins}m");
    }
    let hours = mins / 60;
    if hours < 24 {
        return format!("{hours}h{:02}m", mins % 60);
    }
    format!("{}d{:02}h", hours / 24, hours % 24)
}
