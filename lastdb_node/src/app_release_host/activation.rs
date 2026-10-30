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
    /// The release log pass this install rests on, and the log head the
    /// decision was made on.
    pub log: InstallVerdict,
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

/// Resolve a channel, check the release against the release log, verify
/// the release, and activate it through Host Track.
///
/// The order is the design's activation order. The release log decides at
/// step 2, and the two verification faults stop the run at step 3 — all
/// before the `current` pointer moves.
///
/// # Errors
/// Returns the registry, release log, verification, or filesystem failure.
/// A log that does not sync, a stale head, a release without a `pass` for
/// this node's contract version, or a revocation refuses the install. A
/// digest or signature fault surfaces as an [`ArtifactFault`] rendered into
/// the message. Every refusal leaves `current` where it was.
pub async fn install_and_activate(
    registry: &ReleaseRegistryClient,
    log: &mut ReleaseLog,
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

    // 2. Sync the release log and apply the install rule. The log, not the
    //    registry, decides. Then read the release manifest by release id
    //    and bind it to the pass.
    log.sync().await?;
    let verdict = check_release_log(log, host, app_id, &channel_read.release_id)?;
    let release = registry.get_release(&channel_read.release_id).await?;
    check_release_read(&release, &channel_read.release_id, app_id, &verdict)?;

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
        log: verdict,
        proof,
    })
}

/// Activation order step 2, the log half: the install rule for the
/// channel's release on this node's contract version, then the channel
/// move rule against the active release.
///
/// A channel move never goes to a release whose first `pass` is older
/// than the first `pass` of the active release, unless the active release
/// is revoked. A release with no `pass` in the log (installed before the
/// log) gives no order, so it does not block the move.
fn check_release_log(
    log: &ReleaseLog,
    host: &HostTrack,
    app_id: &str,
    release_id: &str,
) -> Result<InstallVerdict, String> {
    let verdict = log.require_installable(
        release_id,
        app_id,
        NODE_CONTRACT_VERSION,
        chrono::Utc::now(),
    )?;
    let Some(active) = host.active_release_id() else {
        return Ok(verdict);
    };
    if active == release_id || log.installed_release_revocation(&active, app_id).is_some() {
        return Ok(verdict);
    }
    let target_first = log.first_pass_seq(release_id);
    let active_first = log.first_pass_seq(&active);
    if let Some((target, installed)) = target_first
        .zip(active_first)
        .filter(|(target, installed)| target < installed)
    {
        return Err(format!(
            "refusing to move {app_id} from {active} to {release_id}: the release log first \
             passed {release_id} at entry {target}, before the active release (entry \
             {installed}); a channel move never goes back to an older release"
        ));
    }
    Ok(verdict)
}

/// Activation order step 2, the registry half. The registry is not on the
/// trust path: the manifest it serves must hash to the channel's release
/// id, and its publisher key must be the dev key the log `pass` names.
fn check_release_read(
    release: &ReleaseRead,
    release_id: &str,
    app_id: &str,
    verdict: &InstallVerdict,
) -> Result<(), String> {
    let served_id = compute_release_id_from_value(&release.manifest_value)?;
    if release.release_id != release_id || served_id != release_id {
        return Err(format!(
            "the registry served release {} with a manifest that hashes to {served_id}, \
             not to the channel's release {release_id}",
            release.release_id
        ));
    }
    if release.manifest.app_id != app_id {
        return Err(format!(
            "release {release_id} belongs to app '{}', not '{app_id}'",
            release.manifest.app_id
        ));
    }
    if dev_cert_of(&release.publisher_dev_pubkey)? != verdict.dev_cert {
        return Err(format!(
            "release {release_id} is published by a key that the release log pass does not \
             name (the pass names dev_cert {})",
            verdict.dev_cert
        ));
    }
    Ok(())
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
    /// The proof after the restore. `None` when there was no restore.
    pub after: Option<FourWayProof>,
    /// The revoked release the host took out of service when it had no
    /// prior verified release to restore. `current` no longer exists.
    pub deactivated: Option<String>,
    /// The version directories of revoked releases the host removed.
    pub removed: Vec<String>,
}

/// Compare the four release ids; on drift restore the prior verified
/// release and prove the match again.
///
/// `revocation` answers from the synced release log: for a release id, the
/// `revoke` entry that names the release, its app, or its dev key. A
/// revoked active release runs the same restore path as an id mismatch, so
/// revocation needs no second mechanism. A revoked release is never a
/// rollback target. With no prior verified release, a revoked active
/// release is taken out of service. The bytes of every revoked release
/// that is not active are removed.
///
/// On a clean check the active release becomes the rollback target for the
/// next one — only a release that proved `CURRENT` is ever restored.
///
/// # Errors
/// Returns the filesystem failure from the restore or the removal.
pub fn check_drift_and_restore(
    host: &HostTrack,
    app_id: &str,
    desired: Option<String>,
    revocation: &dyn Fn(&str) -> Option<String>,
) -> Result<DriftOutcome, String> {
    let mut before = prove_four_way(host, desired);
    let active_revocation = before.active.as_deref().and_then(revocation);
    if let Some(reason) = &active_revocation {
        // A revoked active release is drift by definition, whatever the
        // four ids say. The host runs the same restore path rather than
        // needing a second mechanism.
        before.status = AppStatus::Drift(format!(
            "the active release {} is revoked ({reason})",
            before.active.as_deref().unwrap_or("-")
        ));
        before.status_label = before.status.label().to_string();
    }
    if host
        .prior_verified_release_id()
        .is_some_and(|prior| revocation(&prior).is_some())
    {
        host.clear_prior_verified()?;
    }
    let mut outcome = DriftOutcome {
        app_id: app_id.to_string(),
        before,
        restored_to: None,
        after: None,
        deactivated: None,
        removed: Vec::new(),
    };
    if outcome.before.matches() {
        // A clean check promotes the active release to the rollback target.
        if let Some(active) = &outcome.before.active {
            host.set_prior_verified(active)?;
        }
    } else if matches!(outcome.before.status, AppStatus::Drift(_)) {
        // Only proven drift rolls back. `Unknown` means the host does not
        // have the readings to judge — a channel read that failed, or a
        // release that was just activated and has not started yet. Both are
        // normal, and both are transient. Rolling back on either would make
        // the recurring check the thing that breaks the host: a registry
        // outage, or the seconds between activation and the first
        // observation, would undo a good release. The next cycle re-reads
        // and decides on real evidence.
        restore_or_deactivate(host, &mut outcome, active_revocation.is_some())?;
    }
    outcome.removed = host.remove_revoked_versions(&|id: &str| revocation(id).is_some())?;
    Ok(outcome)
}

/// Restore the prior verified release. With nothing proven to restore,
/// report the drift rather than point `current` at an unproven directory —
/// except for a revoked active release, which the host takes out of
/// service.
fn restore_or_deactivate(
    host: &HostTrack,
    outcome: &mut DriftOutcome,
    active_revoked: bool,
) -> Result<(), String> {
    if let Some(prior) = host.prior_verified_release_id() {
        host.activate(&prior)?;
        // The host repeats the check after every restore.
        outcome.after = Some(prove_four_way(host, Some(prior.clone())));
        outcome.restored_to = Some(prior);
    } else if active_revoked {
        host.deactivate()?;
        outcome.deactivated.clone_from(&outcome.before.active);
    }
    Ok(())
}
