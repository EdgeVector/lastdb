use super::*;

/// Content key for one offline rewrite. Drop zeros the bytes.
pub(super) struct OfflineContentKey([u8; 32]);

impl OfflineContentKey {
    pub(super) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for OfflineContentKey {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

pub(super) fn load_offline_content_key(home: &Path) -> Result<OfflineContentKey, String> {
    let path = home.join("identity.key");
    let mut bytes = std::fs::read(&path).map_err(|_| "identity key is missing".to_string())?;
    if bytes.len() != 32 {
        bytes.fill(0);
        return Err("identity key length".to_string());
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    bytes.fill(0);
    let keys = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
        .map_err(|_| "identity key is not usable".to_string())?;
    seed.fill(0);
    Ok(OfflineContentKey(keys.encryption_key()))
}

pub(super) fn default_version_cutoff_nanos() -> u64 {
    fold_db::clock::unix_nanos().saturating_sub(laststore::SUPERSEDED_VERSION_RETENTION_NANOS)
}

pub(super) fn print_version_retention_report(
    report: &laststore::VersionRetentionReport,
    execute: bool,
    json: bool,
) -> Result<(), String> {
    if json {
        let body = serde_json::json!({
            "execute": execute,
            "version_cutoff_nanos": report.version_cutoff_nanos,
            "version_keys": report.version_keys,
            "heads_with_chain": report.heads_with_chain,
            "heads_truncated": report.heads_truncated,
            "heads_skipped_tombstoned": report.heads_skipped_tombstoned,
            "bodies_unopened": report.bodies_unopened,
            "versions_kept": report.versions_kept,
            "versions_dropped": report.versions_dropped,
            "links_rewritten": report.links_rewritten,
            "backrefs_dropped": report.backrefs_dropped,
            "groups_rewritten": report.groups_rewritten,
            "tips_bytes_before": report.tips_bytes_before,
            "tips_bytes_after": report.tips_bytes_after,
        });
        let text = serde_json::to_string_pretty(&body).map_err(|error| error.to_string())?;
        println!("{text}");
        return Ok(());
    }
    println!("execute={}", if execute { 1 } else { 0 });
    println!("version_cutoff_nanos={}", report.version_cutoff_nanos);
    println!("version_keys={}", report.version_keys);
    println!("heads_with_chain={}", report.heads_with_chain);
    println!("heads_truncated={}", report.heads_truncated);
    println!(
        "heads_skipped_tombstoned={}",
        report.heads_skipped_tombstoned
    );
    println!("bodies_unopened={}", report.bodies_unopened);
    println!("versions_kept={}", report.versions_kept);
    println!("versions_dropped={}", report.versions_dropped);
    println!("links_rewritten={}", report.links_rewritten);
    println!("backrefs_dropped={}", report.backrefs_dropped);
    println!("groups_rewritten={}", report.groups_rewritten);
    println!("tips_bytes_before={}", report.tips_bytes_before);
    println!("tips_bytes_after={}", report.tips_bytes_after);
    Ok(())
}

/// Rewrite tip files for versions of a live record. The daemon must already
/// be stopped. This command does not stop or start the daemon.
pub(super) fn db_retain_superseded_versions_offline(
    data_dir: Option<PathBuf>,
    execute: bool,
    version_cutoff_nanos: Option<u64>,
    json: bool,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let store_root = home.join("data");
    let socket = store_root.join("folddb.sock");
    if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
        return Err(format!(
            "refusing: the database socket is open at {}",
            socket.display()
        ));
    }
    let layout_path = store_root.join("laststore-layout-v1");
    let layout = std::fs::read_to_string(&layout_path)
        .map_err(|_| format!("layout file is missing: {}", layout_path.display()))?;
    if !layout.lines().any(|line| line == "packaging=plain") {
        return Err("refusing: packaging is not plain".to_string());
    }
    if laststore::home_has_frame_aead_segments(&store_root) {
        return Err("refusing: frame segments are present".to_string());
    }
    let key = load_offline_content_key(&home)?;
    let lock_path = store_root.join("maintenance.lock");
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .map_err(|error| format!("maintenance lock: {error}"))?;
    if let Err(error) = rustix::fs::flock(
        &lock_file,
        rustix::fs::FlockOperation::NonBlockingLockExclusive,
    ) {
        return Err(format!("maintenance lock is held: {error}"));
    }
    let cutoff = version_cutoff_nanos.unwrap_or_else(default_version_cutoff_nanos);
    eprintln!(
        "retain-superseded-versions-offline store={} execute={execute} version_cutoff_nanos={cutoff}",
        store_root.display()
    );
    let _ = std::io::stderr().flush();
    let store = laststore::LastStore::open(&store_root).map_err(|error| error.to_string())?;
    let open = |body: &[u8]| fold_db::crypto::open_at_rest(key.as_bytes(), body).ok();
    let seal = |plain: &[u8]| fold_db::crypto::seal_at_rest_raw(key.as_bytes(), plain).ok();
    let report = store
        .maintenance_drop_versions_with(cutoff, execute, &open, &seal)
        .map_err(|error| error.to_string())?;
    print_version_retention_report(&report, execute, json)?;
    drop(lock_file);
    drop(key);
    Ok(())
}
