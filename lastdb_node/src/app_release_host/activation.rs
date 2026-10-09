use super::*;

// ─── Install + activation ─────────────────────────────────────────────────

/// What one install run did.
#[derive(Debug, Clone, Serialize)]
pub struct ActivationOutcome {
    pub app_id: String,
    pub channel: String,
    pub release_id: String,
    /// The generation the channel read returned. Keep it for a later write.
    pub channel_generation: u64,
    pub execution_identity: String,
    pub activation_epoch: u64,
    pub version_dir: String,
    pub pruned: Vec<String>,
    /// Activation order step 6, run at the moment of activation: observe the
    /// live process, compare the four release ids, and run the probe.
    ///
    /// A release that was just unpacked has no live process yet, so this
    /// proof normally reads `UNKNOWN` until the release starts and writes its
    /// observation. That is the honest reading — `CURRENT` is a live claim —
    /// and it is what the recurring check at [`PROBE_INTERVAL`] then
    /// promotes. The install still succeeded; this field reports where the
    /// host stands, it does not gate the activation.
    pub proof: FourWayProof,
}

/// Resolve a channel, verify the release, and activate it through Host
/// Track.
///
/// The order is the design's activation order, and the two verification
/// faults stop the run at step 3 — before the `current` pointer moves.
///
/// # Errors
/// Returns the registry, verification, or filesystem failure. A digest or
/// signature fault surfaces as an [`ArtifactFault`] rendered into the
/// message, and leaves `current` where it was.
pub async fn install_and_activate(
    registry: &ReleaseRegistryClient,
    host: &HostTrack,
    app_id: &str,
    channel: &str,
) -> Result<ActivationOutcome, String> {
    // 1. Read the channel. Keep the generation.
    let channel_read = registry.get_channel(app_id, channel).await?;
    if channel_read.app_id != app_id {
        return Err(format!(
            "channel read returned app '{}', expected '{app_id}'",
            channel_read.app_id
        ));
    }

    // 2. Read the release manifest by release id.
    let release = registry.get_release(&channel_read.release_id).await?;
    if release.revoked {
        return Err(format!(
            "refusing to activate {}: the release is revoked",
            release.release_id
        ));
    }
    if release.manifest.app_id != app_id {
        return Err(format!(
            "release {} belongs to app '{}', not '{app_id}'",
            release.release_id, release.manifest.app_id
        ));
    }

    // 3. Download the artifact, compare the digest, verify the signature.
    //    Both faults stop here. Nothing below this point has run.
    let bytes = registry
        .fetch_artifact(&release.manifest.artifact_url)
        .await?;
    verify_artifact(&bytes, &release.manifest, &release.publisher_dev_pubkey)
        .map_err(|fault| fault.to_string())?;

    // 4. Unpack into versions/<release-id>/.
    let version_dir = host.version_dir(&release.release_id);
    unpack_artifact(&bytes, &version_dir)?;
    let receipt = ReleaseReceipt {
        release_id: release.release_id.clone(),
        manifest: release.manifest.clone(),
        verified_artifact_digest: sha256_hex(&bytes),
        installed_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };
    let receipt_bytes = serde_json::to_vec_pretty(&receipt)
        .map_err(|e| format!("failed to encode release receipt: {e}"))?;
    std::fs::write(version_dir.join("lastdb-release.json"), receipt_bytes)
        .map_err(|e| format!("failed to write release receipt: {e}"))?;

    // 5. Move the current pointer.
    let activation_epoch = host.activate(&release.release_id)?;

    // Retention: keep the active release and the two before it.
    let pruned = host.prune_versions()?;

    // 6. Observe the live process, compare the four release ids, run the
    //    probe. This is the "on activation" half of the probe cadence; the
    //    recurring half is one `check_cycle` every `PROBE_INTERVAL`.
    let proof = prove_four_way(host, Some(release.release_id.clone()));

    let identity = ExecutionIdentity::Release {
        app_uuid: release.manifest.app_uuid.clone(),
        release_id: release.release_id.clone(),
        activation_epoch,
    };
    Ok(ActivationOutcome {
        app_id: app_id.to_string(),
        channel: channel.to_string(),
        release_id: release.release_id,
        channel_generation: channel_read.generation,
        execution_identity: identity.to_string(),
        activation_epoch,
        version_dir: version_dir.display().to_string(),
        pruned,
        proof,
    })
}

/// Unpack verified artifact bytes into the version directory.
///
/// The bytes land in a sibling staging directory and are renamed into
/// place only after `tar` succeeds. Re-installing the release that is
/// currently active therefore never leaves the active directory deleted
/// while an unpack is in flight or after one fails.
fn unpack_artifact(bytes: &[u8], version_dir: &Path) -> Result<(), String> {
    let Some(parent) = version_dir.parent() else {
        return Err(format!(
            "version directory {} has no parent",
            version_dir.display()
        ));
    };
    let file_name = version_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("version directory {} has no name", version_dir.display()))?;
    let staging = parent.join(format!(".staging-{file_name}"));
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging)
        .map_err(|e| format!("failed to create {}: {e}", staging.display()))?;

    let unpack = || -> Result<(), String> {
        let tarball = staging.join(".artifact.tar.gz");
        std::fs::write(&tarball, bytes)
            .map_err(|e| format!("failed to stage artifact bytes: {e}"))?;
        let status = ProcessCommand::new("tar")
            .arg("-xzf")
            .arg(&tarball)
            .arg("-C")
            .arg(&staging)
            .status()
            .map_err(|e| format!("failed to run tar: {e}"))?;
        let _ = std::fs::remove_file(&tarball);
        if !status.success() {
            return Err(format!(
                "tar failed to unpack the artifact into {}",
                version_dir.display()
            ));
        }
        Ok(())
    };
    if let Err(e) = unpack() {
        let _ = std::fs::remove_dir_all(&staging);
        return Err(e);
    }

    // Swap only now that the staged tree is complete.
    if version_dir.exists() {
        std::fs::remove_dir_all(version_dir)
            .map_err(|e| format!("failed to clear {}: {e}", version_dir.display()))?;
    }
    std::fs::rename(&staging, version_dir).map_err(|e| {
        let _ = std::fs::remove_dir_all(&staging);
        format!(
            "failed to move the staged release into {}: {e}",
            version_dir.display()
        )
    })
}

// ─── Drift and rollback ───────────────────────────────────────────────────

/// One drift check and, when it finds drift, one restore.
#[derive(Debug, Clone, Serialize)]
pub struct DriftOutcome {
    pub app_id: String,
    /// The proof before any restore.
    pub before: FourWayProof,
    /// The release the host restored, when it restored one.
    pub restored_to: Option<String>,
    /// The proof after the restore. `None` when there was no drift.
    pub after: Option<FourWayProof>,
}

/// Compare the four release ids; on drift restore the prior verified
/// release and prove the match again.
///
/// `active_revoked` is the revocation read from the same cycle as the
/// channel read. A revoked active release runs the same restore path as an
/// id mismatch, so revocation needs no second mechanism.
///
/// On a clean check the active release becomes the rollback target for the
/// next one — only a release that proved `CURRENT` is ever restored.
///
/// # Errors
/// Returns the filesystem failure from the restore.
pub fn check_drift_and_restore(
    host: &HostTrack,
    app_id: &str,
    desired: Option<String>,
    active_revoked: bool,
) -> Result<DriftOutcome, String> {
    let mut before = prove_four_way(host, desired);
    if active_revoked {
        // A revoked active release is drift by definition, whatever the
        // four ids say. The host runs the same restore path rather than
        // needing a second mechanism.
        before.status = AppStatus::Drift(format!(
            "the active release {} is revoked",
            before.active.as_deref().unwrap_or("-")
        ));
        before.status_label = before.status.label().to_string();
    }
    if before.matches() {
        // A clean check promotes the active release to the rollback target.
        if let Some(active) = &before.active {
            host.set_prior_verified(active)?;
        }
        return Ok(DriftOutcome {
            app_id: app_id.to_string(),
            before,
            restored_to: None,
            after: None,
        });
    }

    // Only proven drift rolls back. `Unknown` means the host does not have
    // the readings to judge — a channel read that failed, or a release that
    // was just activated and has not started yet. Both are normal, and both
    // are transient. Rolling back on either would make the recurring check
    // the thing that breaks the host: a registry outage, or the seconds
    // between activation and the first observation, would undo a good
    // release. The next cycle re-reads and decides on real evidence.
    if !matches!(before.status, AppStatus::Drift(_)) {
        return Ok(DriftOutcome {
            app_id: app_id.to_string(),
            before,
            restored_to: None,
            after: None,
        });
    }

    let Some(prior) = host.prior_verified_release_id() else {
        // Nothing proven yet, so there is nothing to restore to. Report the
        // drift rather than pointing `current` at an unproven directory.
        return Ok(DriftOutcome {
            app_id: app_id.to_string(),
            before,
            restored_to: None,
            after: None,
        });
    };
    host.activate(&prior)?;
    // The host repeats the check after every restore.
    let after = prove_four_way(host, Some(prior.clone()));
    Ok(DriftOutcome {
        app_id: app_id.to_string(),
        before,
        restored_to: Some(prior),
        after: Some(after),
    })
}
