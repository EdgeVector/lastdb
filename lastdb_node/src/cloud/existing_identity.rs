//! Connect with an existing identity on a dev home.

use super::*;

pub(super) const DEVICE_ID_FILE: &str = ".device_id";

/// Validate the copied-home invariants for the explicit DEV photograph path.
///
/// This check lives below the CLI boundary so another caller cannot bypass the
/// primary-home, socket, identity, config, or device protections. It returns a
/// canonical path, and the caller must use only that path after this point.
pub fn preflight_existing_identity_dev(home: &Path) -> Result<PathBuf, String> {
    let protected_homes = protected_primary_home_candidates()?;
    preflight_existing_identity_dev_with(
        home,
        &protected_homes,
        crate::crash_attribution::live_daemon_pid_alive,
        std::env::var_os("FOLD_SYNC_DEVICE_ID").is_some(),
    )
}

pub(super) fn protected_primary_home_candidates() -> Result<Vec<PathBuf>, String> {
    let mut candidates = vec![host::resolve_home(None)?];
    if let Some(user_home) = std::env::var_os("HOME") {
        let user_home = PathBuf::from(user_home);
        candidates.push(user_home.join(".lastdb"));
        candidates.push(user_home.join(".folddb"));
    }
    candidates.sort();
    candidates.dedup();
    Ok(candidates)
}

pub(super) fn preflight_existing_identity_dev_with(
    home: &Path,
    protected_homes: &[PathBuf],
    daemon_is_live: impl Fn(&Path) -> bool,
    device_override_present: bool,
) -> Result<PathBuf, String> {
    preflight_existing_identity_dev_with_device(
        home,
        protected_homes,
        daemon_is_live,
        device_override_present,
        None,
    )
}

pub(super) fn preflight_existing_identity_dev_with_pending_device(
    home: &Path,
    expected_device_id: &str,
) -> Result<PathBuf, String> {
    let protected_homes = protected_primary_home_candidates()?;
    preflight_existing_identity_dev_with_device(
        home,
        &protected_homes,
        crate::crash_attribution::live_daemon_pid_alive,
        std::env::var_os("FOLD_SYNC_DEVICE_ID").is_some(),
        Some(expected_device_id),
    )
}

// lint:fn-size-ok moved verbatim from its original module; no logic change
pub(super) fn preflight_existing_identity_dev_with_device(
    home: &Path,
    protected_homes: &[PathBuf],
    daemon_is_live: impl Fn(&Path) -> bool,
    device_override_present: bool,
    expected_device_id: Option<&str>,
) -> Result<PathBuf, String> {
    if !home.is_absolute() {
        return Err("--use-existing-identity requires an absolute --data-dir path".to_string());
    }

    let home_meta = std::fs::symlink_metadata(home)
        .map_err(|e| format!("cannot inspect copied LastDB home {}: {e}", home.display()))?;
    if !home_meta.file_type().is_dir() {
        return Err(format!(
            "copied LastDB home {} must be a real directory",
            home.display()
        ));
    }
    let canonical_home = std::fs::canonicalize(home).map_err(|e| {
        format!(
            "cannot canonicalize copied LastDB home {}: {e}",
            home.display()
        )
    })?;

    for protected in protected_homes {
        let protected_meta = match std::fs::symlink_metadata(protected) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(format!(
                    "cannot inspect protected primary home {}: {e}",
                    protected.display()
                ))
            }
        };
        if !protected_meta.file_type().is_dir() && !protected_meta.file_type().is_symlink() {
            return Err(format!(
                "protected primary home {} has an unexpected file type",
                protected.display()
            ));
        }
        let canonical_protected = std::fs::canonicalize(protected).map_err(|e| {
            format!(
                "cannot canonicalize protected primary home {}: {e}",
                protected.display()
            )
        })?;
        if canonical_home == canonical_protected
            || canonical_home.starts_with(&canonical_protected)
            || canonical_protected.starts_with(&canonical_home)
        {
            return Err(format!(
                "refusing a path that overlaps the live primary LastDB home: {}",
                canonical_home.display()
            ));
        }
    }

    if daemon_is_live(&canonical_home)
        && !session_marker_matches_protected_home(&canonical_home, protected_homes)
    {
        return Err(format!(
            "refusing copied home with a live lastdbd process: {}",
            canonical_home.display()
        ));
    }

    let data_dir = canonical_home.join("data");
    let data_meta = std::fs::symlink_metadata(&data_dir).map_err(|e| {
        format!(
            "cannot inspect copied data directory {}: {e}",
            data_dir.display()
        )
    })?;
    if !data_meta.file_type().is_dir() {
        return Err(format!(
            "copied data path {} must be a real directory",
            data_dir.display()
        ));
    }
    let canonical_data = std::fs::canonicalize(&data_dir)
        .map_err(|e| format!("cannot canonicalize copied data directory: {e}"))?;
    if !canonical_data.starts_with(&canonical_home) {
        return Err("copied data directory escapes the copied LastDB home".to_string());
    }

    for socket_name in [
        lastdb_uds::uds::SOCKET_FILE_NAME,
        lastdb_uds::uds::FULL_SOCKET_FILE_NAME,
    ] {
        refuse_existing_entry(
            &canonical_data.join(socket_name),
            "copied home contains a daemon socket path",
        )?;
    }

    let (active_config, paused_config) = cloud_sync_paths(&canonical_home);
    refuse_existing_entry(
        &active_config,
        "copied home already contains active cloud credentials",
    )?;
    refuse_existing_entry(
        &paused_config,
        "copied home already contains paused cloud credentials",
    )?;
    let device_path = canonical_data.join(DEVICE_ID_FILE);
    if let Some(expected) = expected_device_id {
        validate_pending_device_file(&device_path, expected)?;
    } else {
        refuse_existing_entry(
            &device_path,
            "copied home still contains its production device ID",
        )?;
    }

    if device_override_present {
        return Err("FOLD_SYNC_DEVICE_ID must be unset for --use-existing-identity".to_string());
    }

    validate_existing_identity_file(&canonical_home)?;
    Ok(canonical_home)
}

/// A whole-home CoW retains `current-session.json` from the primary. That
/// marker names the primary's live PID, but it does not make the copy live.
/// Match the stable session identity (PID + start time) against each protected
/// primary home before the generic live-PID guard rejects the copy.
pub(super) fn session_marker_matches_protected_home(
    home: &Path,
    protected_homes: &[PathBuf],
) -> bool {
    let Some(candidate) = crate::session_ledger::read_live_session(home) else {
        return false;
    };
    protected_homes.iter().any(|protected| {
        crate::session_ledger::read_live_session(protected).is_some_and(|primary| {
            primary.pid == candidate.pid && primary.start_ts == candidate.start_ts
        })
    })
}

pub(super) fn validate_pending_device_file(path: &Path, expected: &str) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|e| {
        format!(
            "cannot inspect fresh DEV device ID at {}: {e}",
            path.display()
        )
    })?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "fresh DEV device ID {} is not a regular file",
            path.display()
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            return Err(format!(
                "fresh DEV device ID {} is not owner-only",
                path.display()
            ));
        }
    }
    let persisted = std::fs::read(path).map_err(|e| {
        format!(
            "cannot verify fresh DEV device ID at {}: {e}",
            path.display()
        )
    })?;
    if persisted != expected.as_bytes() {
        return Err("fresh DEV device ID changed during registration".to_string());
    }
    Ok(())
}

pub(super) fn refuse_existing_entry(path: &Path, reason: &str) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Err(format!("{reason}: {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("cannot inspect {}: {e}", path.display())),
    }
}

pub(super) fn validate_existing_identity_file(home: &Path) -> Result<[u8; 32], String> {
    let path = home.join(IDENTITY_KEY_FILE);
    let metadata = std::fs::symlink_metadata(&path)
        .map_err(|e| format!("cannot inspect copied identity {}: {e}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "copied identity {} must be a regular file, not a symlink or directory",
            path.display()
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(format!(
                "copied identity {} has a different owner",
                path.display()
            ));
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(format!(
                "copied identity {} must not grant group or other permissions",
                path.display()
            ));
        }
    }
    lastdb_identity::load_seed(home)?
        .ok_or_else(|| format!("copied identity {} is missing", path.display()))
}
