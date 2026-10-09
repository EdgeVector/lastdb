//! `keycheck.enc` — a tiny, always-present decrypt-proof object.
//!
//! Genesis into a prefix that has never held any cloud data has no proof
//! object to open: no `latest.enc` snapshot exists yet, and the log has no
//! head. Today that "genuinely empty" state is treated as safe (there is
//! nothing to poison with a first write), but the very first upload that
//! follows can itself become the *only* candidate proof object on every
//! later cycle — and if that first personal mutation-log segment happens to
//! exceed `SyncConfig::max_download_entry_bytes` (a real occurrence on a
//! large local backlog: fold card
//! `lastdb-cloud-genesis-oversized-first-log-head-blocks-bootstrap`), the
//! prefix is left permanently `Indeterminate`: too big to open as a proof,
//! too small (in the sense of "not `latest.enc`") to fall back to a
//! snapshot. `prove_prefix_decryptable_from_objects` then latches
//! `SyncError::BackupBootstrapBlocked` forever, on every cycle, with no
//! self-healing path.
//!
//! `keycheck.enc` closes that hole: genesis writes it the moment the prefix
//! is confirmed empty, *before* any other object lands, so every later cycle
//! has a small (bounded, sub-kilobyte) proof object to open regardless of
//! how large subsequent log segments or snapshots grow. See
//! `design-lastdb-cloud-genesis-initial-snapshot` section 3.2 in brain for
//! the full design; this is the minimal, independently-shippable slice of
//! it (the object + the proof-ladder entry), not the full claim/confirm/flip
//! genesis operation from section 3.3.

use super::super::*;
use crate::sync::error::SyncResult;
use crate::sync::org_sync::SyncTarget;
use serde::{Deserialize, Serialize};

/// Object name under a target's prefix. Deliberately outside `log/` and
/// `snapshots/` so nothing else can ever mistake it for log or snapshot
/// data, and so it always sorts first in a plain listing.
pub(crate) const KEYCHECK_SNAPSHOT: &str = "keycheck.enc";

/// `keycheck.enc` is expected to stay well below one KiB. Keep a generous
/// envelope cap so a corrupt or replaced remote object cannot turn the proof
/// fast path into another unbounded download.
pub(crate) const KEYCHECK_MAX_ENCRYPTED_BYTES: usize = 4 * 1024;

/// Fixed, tiny plaintext — this object exists purely to be opened, never to
/// carry information. Keeping it small and constant-shaped is what makes it
/// safe to fetch on every single proof call with no size cap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct KeycheckPayload {
    pub(crate) version: u8,
    pub(crate) device_id: String,
}

impl SyncEngine {
    /// Try to open `keycheck.enc`. `Ok(Some(()))` proves the current key
    /// opens the prefix; `Ok(None)` means it has not been written yet
    /// (pre-existing account, or genesis has not run under this fix) and the
    /// caller must fall back to the legacy log-head/snapshot ladder.
    pub(crate) async fn read_keycheck(&self, target: &SyncTarget) -> SyncResult<Option<()>> {
        let url = self
            .auth
            .presign_snapshot_download_for_target(target, KEYCHECK_SNAPSHOT)
            .await?;
        let Some(bytes) = self
            .s3
            .download_limited(&url, Some(KEYCHECK_MAX_ENCRYPTED_BYTES))
            .await?
        else {
            return Ok(None);
        };
        let plaintext = target.crypto.decrypt(&bytes).await?;
        let payload: KeycheckPayload = serde_json::from_slice(&plaintext)?;
        if payload.version != 1 || payload.device_id.is_empty() {
            return Err(crate::sync::error::SyncError::Storage(format!(
                "invalid keycheck.enc payload: version={} device_id_present={}",
                payload.version,
                !payload.device_id.is_empty()
            )));
        }
        Ok(Some(()))
    }

    /// Write `keycheck.enc`, sealed under `target`'s current key. Called once
    /// genesis has confirmed the prefix is genuinely empty (no log objects,
    /// no snapshot) — i.e. this is the very first write to the prefix, so it
    /// cannot race a legitimate history it might otherwise poison.
    ///
    /// Fail closed: the caller must not publish the first real object until
    /// this PUT succeeds. Otherwise an oversized first log segment recreates
    /// the permanently-indeterminate prefix this object exists to prevent.
    pub(crate) async fn write_keycheck(&self, target: &SyncTarget) -> SyncResult<()> {
        let payload = KeycheckPayload {
            version: 1,
            device_id: self.device_id.clone(),
        };
        let plaintext = serde_json::to_vec(&payload)?;
        let ciphertext = target.crypto.encrypt(&plaintext).await?;
        let url = self
            .auth
            .presign_snapshot_upload_for_target(target, KEYCHECK_SNAPSHOT)
            .await?;
        self.s3.upload(&url, ciphertext).await?;
        if let Err(e) = self
            .auth
            .confirm_snapshot_upload_for_target(target, KEYCHECK_SNAPSHOT)
            .await
        {
            tracing::warn!(
                target: "fold_db::sync",
                error = %e,
                "confirm_snapshot_upload metering failed for keycheck.enc (non-fatal)"
            );
        }
        Ok(())
    }
}
