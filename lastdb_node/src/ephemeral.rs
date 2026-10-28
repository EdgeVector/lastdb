//! Detection and isolation for synthetic/ephemeral nodes (CoW clones).
//!
//! A synthetic or ephemeral node is a copy-on-write clone of an existing node
//! home, used for safe-upgrade probes and other temporary operations. Ephemeral
//! nodes must not write to the shared schema catalog, preserving the isolation
//! guarantee safe-upgrade probes and other ephemeral-node tooling depend on.
//!
//! Ephemeral status is signaled by the `LASTDB_EPHEMERAL` environment variable
//! (any non-empty value). When set, the node refuses catalog-mutating
//! operations (schema sync/registration and the DB catalog membership routes)
//! so the shared/primary catalog never sees a write from a throwaway clone.

use std::path::Path;

/// Marker that the installed `lastdb-dev` clone helper writes into each copy.
#[cfg(debug_assertions)]
const LASTDB_DEV_OWNER_MARKER: &str = ".lastdb-dev-owner";

/// Whether this node is running as an ephemeral/synthetic instance.
///
/// Ephemeral nodes are CoW clones or other temporary instances that must not
/// persist catalog writes to the shared catalog. This prevents isolation
/// violations when an ephemeral node boots and tries to register schemas.
pub fn is_ephemeral() -> bool {
    std::env::var_os("LASTDB_EPHEMERAL").is_some()
}

/// Verify that a raw-value debug route serves a real `lastdb-dev` copy.
///
/// Environment flags can request the route, but they cannot prove isolation.
/// This gate binds access to the paths that [`crate::host::Host`] actually
/// opened. Release builds reject the route. Debug builds require a canonical
/// home outside every standard primary location, an internal real data
/// directory, the clone helper's owned marker, and no cloud configuration.
pub fn verify_dev_field_index_debug_home(home: &Path, data_dir: &Path) -> Result<(), String> {
    #[cfg(not(debug_assertions))]
    {
        let _ = (home, data_dir);
        Err("field-index raw debug mode is unavailable in release builds".to_string())
    }

    #[cfg(debug_assertions)]
    {
        let user_home = std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .ok_or_else(|| "cannot verify debug isolation because HOME is unset".to_string())?;
        let protected = [user_home.join(".lastdb"), user_home.join(".folddb")];
        verify_dev_field_index_debug_home_with(home, data_dir, &protected)
    }
}

#[cfg(debug_assertions)]
fn verify_dev_field_index_debug_home_with(
    home: &Path,
    data_dir: &Path,
    protected_homes: &[std::path::PathBuf],
) -> Result<(), String> {
    if !home.is_absolute() || !data_dir.is_absolute() {
        return Err("field-index raw debug mode requires absolute database paths".to_string());
    }

    let canonical_home = canonical_real_dir(home, "debug home")?;
    let canonical_data = canonical_real_dir(data_dir, "debug data directory")?;
    if canonical_data != canonical_home.join("data") {
        return Err("debug data directory must be the selected home/data path".to_string());
    }

    reject_protected_home_overlap(&canonical_home, protected_homes)?;

    let canonical_source = canonical_clone_source(&canonical_home)?;
    if canonical_home == canonical_source
        || canonical_home.starts_with(&canonical_source)
        || canonical_source.starts_with(&canonical_home)
    {
        return Err(format!(
            "field-index raw debug home overlaps its clone source: {}",
            canonical_home.display()
        ));
    }

    if crate::cloud::cloud_credentials_path(&canonical_home).exists() {
        return Err("field-index raw debug home must not contain cloud configuration".to_string());
    }
    Ok(())
}

#[cfg(debug_assertions)]
/// Canonicalize `path` after proving it is a real directory, not a symlink.
fn canonical_real_dir(path: &Path, what: &str) -> Result<std::path::PathBuf, String> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect {what} {}: {error}", path.display()))?;
    if !meta.file_type().is_dir() {
        return Err(format!(
            "{what} {} must be a real directory",
            path.display()
        ));
    }
    std::fs::canonicalize(path)
        .map_err(|error| format!("cannot canonicalize {what} {}: {error}", path.display()))
}

#[cfg(debug_assertions)]
/// Refuse a debug home that is, contains, or sits inside a protected primary home.
fn reject_protected_home_overlap(
    canonical_home: &Path,
    protected_homes: &[std::path::PathBuf],
) -> Result<(), String> {
    for protected in protected_homes {
        let protected_meta = match std::fs::symlink_metadata(protected) {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "cannot inspect protected primary home {}: {error}",
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
        let canonical_protected = std::fs::canonicalize(protected).map_err(|error| {
            format!(
                "cannot canonicalize protected primary home {}: {error}",
                protected.display()
            )
        })?;
        if canonical_home == canonical_protected
            || canonical_home.starts_with(&canonical_protected)
            || canonical_protected.starts_with(canonical_home)
        {
            return Err(format!(
                "field-index raw debug mode refuses primary-home overlap: {}",
                canonical_home.display()
            ));
        }
    }
    Ok(())
}

#[cfg(debug_assertions)]
/// Follow the owner marker to the lastdb-dev clone stamp and return the
/// canonical path of the primary the home was cloned from.
fn canonical_clone_source(canonical_home: &Path) -> Result<std::path::PathBuf, String> {
    let marker = canonical_home.join(LASTDB_DEV_OWNER_MARKER);
    verify_owner_controlled_file(&marker, "field-index raw debug marker")?;
    let marker_body = std::fs::read_to_string(&marker)
        .map_err(|error| format!("cannot read debug marker {}: {error}", marker.display()))?;
    for key in ["created_at", "creator_pid", "creator_cwd"] {
        marker_value(&marker_body, key).ok_or_else(|| {
            format!(
                "field-index raw debug marker {} lacks {key}",
                marker.display()
            )
        })?;
    }
    let state = std::path::PathBuf::from(marker_value(&marker_body, "state").ok_or_else(|| {
        format!(
            "field-index raw debug marker {} lacks state",
            marker.display()
        )
    })?);
    if !state.is_absolute() {
        return Err("field-index raw debug marker state path must be absolute".to_string());
    }
    let clone_stamp = state.join("clone.stamp");
    verify_owner_controlled_file(&clone_stamp, "lastdb-dev clone stamp")?;
    let clone_body = std::fs::read_to_string(&clone_stamp).map_err(|error| {
        format!(
            "cannot read lastdb-dev clone stamp {}: {error}",
            clone_stamp.display()
        )
    })?;
    let source = std::path::PathBuf::from(
        clone_body
            .lines()
            .find_map(|line| line.split_once(" primary=").map(|(_, value)| value.trim()))
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                format!(
                    "lastdb-dev clone stamp {} lacks primary source",
                    clone_stamp.display()
                )
            })?,
    );
    if !source.is_absolute() {
        return Err("lastdb-dev clone source path must be absolute".to_string());
    }
    std::fs::canonicalize(&source).map_err(|error| {
        format!(
            "cannot canonicalize lastdb-dev clone source {}: {error}",
            source.display()
        )
    })
}

#[cfg(debug_assertions)]
fn marker_value<'a>(body: &'a str, key: &str) -> Option<&'a str> {
    body.lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

#[cfg(debug_assertions)]
fn verify_owner_controlled_file(path: &Path, label: &str) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("{label} {} is unavailable: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("{label} {} must be a regular file", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err(format!(
                "{label} {} must be owner-controlled",
                path.display()
            ));
        }
    }
    Ok(())
}
