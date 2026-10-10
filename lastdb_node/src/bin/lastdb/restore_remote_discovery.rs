//! Remote recovery descriptor discovery and the remote restore entry points.

use super::*;

pub(crate) struct RemoteRecovery {
    pub(crate) descriptor: fold_db::sync::engine::RecoveryDescriptorV1,
    pub(crate) rescue: Option<fold_db::sync::auth::ops::RescueS0Pointer>,
    pub(crate) latest: Option<fold_db::sync::auth::ops::BackupLatestGetResponse>,
}

pub(crate) const RESCUE_S0_RESTORE_READY_FILE: &str = ".rescue_s0_restore_ready";
pub(crate) const NORMAL_LATEST_RESTORE_READY_FILE: &str = ".normal_latest_restore_ready";

#[derive(Clone, Copy, Default)]
pub(crate) struct RemoteRecoverySelector<'a> {
    pub(crate) db_hash: Option<&'a str>,
    pub(crate) manifest_sha256: Option<&'a str>,
}

#[derive(Clone, Copy)]
pub(crate) struct RemoteLatestOptions<'a> {
    pub(crate) selection: RemoteRecoverySelector<'a>,
    pub(crate) cache_home: Option<&'a Path>,
}

impl<'a> RemoteLatestOptions<'a> {
    pub(crate) fn new(
        db_hash: Option<&'a str>,
        manifest_sha256: Option<&'a str>,
        cache_home: Option<&'a Path>,
    ) -> Self {
        Self {
            selection: RemoteRecoverySelector {
                db_hash,
                manifest_sha256,
            },
            cache_home,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RestoreSourceMode<'a> {
    Normal {
        cache_home: Option<&'a Path>,
    },
    RemoteS0(RemoteRecoverySelector<'a>),
    RemoteLatest {
        selection: RemoteRecoverySelector<'a>,
        cache_home: Option<&'a Path>,
    },
}

/// Select one immutable account-root S0 rescue cut and authenticate its file.
pub(crate) async fn discover_remote_recovery_descriptor(
    unscoped: &fold_db::sync::auth::AuthClient,
    s3: &fold_db::sync::s3::S3Client,
    e2e_key: &[u8; 32],
    selection: RemoteRecoverySelector<'_>,
) -> Result<RemoteRecovery, String> {
    use fold_db::sync::engine::RecoveryDescriptorV1;

    const MAX_DESCRIPTOR_BYTES: usize = 65_536;
    let rescues = unscoped
        .rescue_s0_list()
        .await
        .map_err(|error| format!("list S0 rescue cuts: {error}"))?;
    let mut selected = None;
    let mut db_hashes = std::collections::HashSet::new();
    let mut manifest_hashes = std::collections::HashSet::new();
    for rescue in rescues {
        if selection
            .db_hash
            .is_some_and(|selected| selected != rescue.db_hash)
            || selection
                .manifest_sha256
                .is_some_and(|selected| selected != rescue.manifest_sha256)
        {
            continue;
        }
        db_hashes.insert(rescue.db_hash.clone());
        manifest_hashes.insert(rescue.manifest_sha256.clone());
        if selected
            .as_ref()
            .is_some_and(|current: &fold_db::sync::auth::ops::RescueS0Pointer| current != &rescue)
            && selected
                .as_ref()
                .is_some_and(|current| current.manifest_sha256 == rescue.manifest_sha256)
        {
            return Err("conflicting S0 rescue pointers for one manifest".into());
        }
        selected = Some(rescue);
    }
    if db_hashes.len() > 1 {
        return Err("several cloud databases match; specify --db-hash".into());
    }
    if manifest_hashes.len() > 1 {
        return Err("several S0 rescue cuts match; specify --manifest-sha256".into());
    }
    let listed = selected.ok_or_else(|| "S0 rescue cut is missing".to_string())?;
    let rescue = unscoped
        .rescue_s0_get(&listed.manifest_sha256)
        .await
        .map_err(|error| format!("read S0 rescue cut: {error}"))?;
    if rescue != listed {
        return Err("S0 rescue list and exact read disagree".into());
    }
    let url = unscoped
        .presign_snapshot_download(&rescue.descriptor_name)
        .await
        .map_err(|error| format!("presign recovery descriptor: {error}"))?;
    let ciphertext = s3
        .download_limited(&url, Some(MAX_DESCRIPTOR_BYTES))
        .await
        .map_err(|error| format!("download recovery descriptor: {error}"))?
        .ok_or_else(|| "recovery descriptor is missing".to_string())?;
    let descriptor = RecoveryDescriptorV1::open(&rescue.descriptor_name, &ciphertext, e2e_key)?;
    rescue
        .validate_descriptor(&descriptor)
        .map_err(|error| format!("check recovery descriptor: {error}"))?;
    if descriptor.mode != "s0_only" {
        return Err("S0 rescue descriptor has the wrong restore mode".into());
    }
    Ok(RemoteRecovery {
        descriptor,
        rescue: Some(rescue),
        latest: None,
    })
}

/// Find one normal cut from the account-root descriptors, then match the
/// exact database-scoped `backup/latest` pointer before the target is opened.
pub(crate) async fn discover_remote_latest_descriptor(
    unscoped: &fold_db::sync::auth::AuthClient,
    s3: &fold_db::sync::s3::S3Client,
    e2e_key: &[u8; 32],
    selection: RemoteRecoverySelector<'_>,
) -> Result<RemoteRecovery, String> {
    use fold_db::sync::engine::RecoveryDescriptorV1;

    const MAX_DESCRIPTOR_BYTES: usize = 65_536;
    const MAX_DESCRIPTOR_OBJECTS: usize = 10_000;
    let listed = unscoped
        .list_objects_legacy_personal_at_most(
            "snapshots/lastdb-recovery-v1-",
            MAX_DESCRIPTOR_OBJECTS,
        )
        .await
        .map_err(|error| format!("list normal recovery descriptors: {error}"))?;
    let mut databases = std::collections::BTreeSet::new();
    let mut candidates = Vec::new();
    for object in listed {
        let name = object
            .key
            .strip_prefix("snapshots/")
            .ok_or("recovery descriptor list returned a key outside snapshots")?;
        let (db_hash, manifest_sha256) = RecoveryDescriptorV1::identity_from_name(name)?;
        if selection
            .db_hash
            .is_some_and(|selected| selected != db_hash)
        {
            continue;
        }
        databases.insert(db_hash.to_string());
        candidates.push((
            name.to_string(),
            db_hash.to_string(),
            manifest_sha256.to_string(),
        ));
    }
    if databases.len() > 1 {
        return Err("several cloud databases match; specify --db-hash".into());
    }
    let db_hash = databases
        .into_iter()
        .next()
        .ok_or("normal recovery descriptor is missing")?;
    let scoped = unscoped
        .clone()
        .with_db_hash(Some(db_hash.clone()))
        .without_db_auto_claim();
    let latest = scoped
        .backup_latest_get()
        .await
        .map_err(|error| format!("read normal backup latest: {error}"))?;
    if selection
        .manifest_sha256
        .is_some_and(|selected| selected != latest.latest.manifest_sha256)
    {
        return Err("selected manifest does not match normal backup latest".into());
    }
    let mut selected: Option<RecoveryDescriptorV1> = None;
    for (name, candidate_db_hash, candidate_manifest_sha256) in candidates {
        if candidate_db_hash != db_hash
            || candidate_manifest_sha256 != latest.latest.manifest_sha256
        {
            continue;
        }
        let url = unscoped
            .presign_snapshot_download_legacy_personal(&name)
            .await
            .map_err(|error| format!("presign normal recovery descriptor: {error}"))?;
        let ciphertext = s3
            .download_limited(&url, Some(MAX_DESCRIPTOR_BYTES))
            .await
            .map_err(|error| format!("download normal recovery descriptor: {error}"))?
            .ok_or("normal recovery descriptor is missing")?;
        let descriptor = RecoveryDescriptorV1::open(&name, &ciphertext, e2e_key)?;
        if descriptor.mode != "replay_tail" {
            continue;
        }
        descriptor.validate_latest(&latest.latest)?;
        if selected
            .as_ref()
            .is_some_and(|existing| existing != &descriptor)
        {
            return Err("conflicting normal recovery descriptors for one backup".into());
        }
        selected = Some(descriptor);
    }
    let descriptor = selected.ok_or("normal backup has no matching recovery descriptor")?;
    Ok(RemoteRecovery {
        descriptor,
        rescue: None,
        latest: Some(latest),
    })
}

pub(crate) fn restore_remote_s0_only_command(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
    progress_json: bool,
    selection: RemoteRecoverySelector<'_>,
) -> Result<(), String> {
    if data_dir.is_none() {
        return Err("--remote-s0-only requires --data-dir with the recovered identity and cloud configuration".into());
    }
    let reporter = progress_json.then(restore_progress_reporter::Reporter::stderr);
    let result = restore_command_inner_with_cache(
        data_dir,
        into,
        env,
        api_url,
        json_only,
        reporter.as_ref().map(|reporter| reporter.progress.as_ref()),
        RestoreSourceMode::RemoteS0(selection),
    );
    if let Some(reporter) = reporter {
        reporter.finish(result.is_ok());
    }
    result.map_err(|failure| {
        if json_only {
            println!("{}", render_restore_failure_json(&failure));
        }
        failure.detail
    })
}

pub(crate) fn restore_remote_latest_command(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
    progress_json: bool,
    recovery: RemoteLatestOptions<'_>,
) -> Result<(), String> {
    if data_dir.is_none() {
        return Err(
            "--remote-latest requires --data-dir with the recovered identity and cloud configuration"
                .into(),
        );
    }
    let reporter = progress_json.then(restore_progress_reporter::Reporter::stderr);
    let result = restore_command_inner_with_cache(
        data_dir,
        into,
        env,
        api_url,
        json_only,
        reporter.as_ref().map(|reporter| reporter.progress.as_ref()),
        RestoreSourceMode::RemoteLatest {
            selection: recovery.selection,
            cache_home: recovery.cache_home,
        },
    );
    if let Some(reporter) = reporter {
        reporter.finish(result.is_ok());
    }
    result.map_err(|failure| {
        if json_only {
            println!("{}", render_restore_failure_json(&failure));
        }
        failure.detail
    })
}
