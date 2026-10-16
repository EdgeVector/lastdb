//! Guard for the two-home proof tooling.
//!
//! Two hidden owner-socket verbs let a developer move real sealed
//! mutation-log segments between two homes of one owner through a plain
//! directory, with no cloud:
//!
//! - `lastdb db export-mutation-log --to <dir>` seals this home's pending
//!   mutation-log records and writes the segments under `<dir>`.
//! - `lastdb db replay-mutation-log --from <dir>` feeds the segments in
//!   `<dir>` to this home through the same peer-apply path the live cloud
//!   cycle uses.
//!
//! A verb that applies mutations from a directory must never reach a real
//! home. Every condition below must hold, or the daemon refuses:
//!
//! 1. The daemon was started with `LASTDB_PROOF_REPLAY=1` in its environment.
//! 2. The home holds a regular file `.synthetic-home` that the operator made.
//! 3. The home, its `data` directory and its `identity.key` are not symlinks,
//!    and no entry directly inside `data` is a symlink.
//! 4. By file identity (device and inode, see [`primary`]), the home and its
//!    `data` directory are not a primary home (`~/.lastdb`, `~/.folddb`, as
//!    written or resolved, under `$HOME` and under the OS account home), do
//!    not sit inside one, and do not hold one.
//! 5. The home has no cloud sync configuration (`cloud_sync.json`, its paused
//!    form, or a cloud resume marker).
//! 6. The daemon booted with the local proof engine. The engine is only built
//!    when 1 to 5 hold at boot, so the verbs cannot run on a cloud engine.
//!
//! What the guard does not cover: a real-data home that is not a primary
//! candidate. For such a home the marker is the only protection, so never
//! create the marker in a home that holds real data.
//!
//! The verbs exist only on the owner socket. Every use logs at WARN.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::host::Host;

mod primary;

/// Environment variable that arms the proof tooling on a daemon.
pub const PROOF_REPLAY_ENV: &str = "LASTDB_PROOF_REPLAY";
/// File the operator creates in a synthetic home to allow the proof tooling.
pub const SYNTHETIC_HOME_MARKER: &str = ".synthetic-home";

/// Refuse a path that is not a real directory (a symlink or a file).
fn require_real_dir(path: &Path, what: &str) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_dir() => Ok(()),
        Ok(_) => Err(format!(
            "{what} is not a real directory (a symlink or a file)"
        )),
        Err(error) => Err(format!("cannot inspect {what}: {error}")),
    }
}

/// Refuse a home whose files can lead outside it: the home, `data` and
/// `identity.key` must not be symlinks, and no entry directly inside `data`
/// may be one. A missing `data` or `identity.key` is a fresh home.
fn require_plain_layout(home: &Path) -> Result<(), String> {
    require_real_dir(home, "the home")?;
    let data = home.join("data");
    match std::fs::symlink_metadata(&data) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => {
            require_real_dir(&data, "<home>/data")?;
            let entries = std::fs::read_dir(&data)
                .map_err(|error| format!("cannot read <home>/data: {error}"))?;
            for entry in entries {
                let entry = entry.map_err(|error| format!("cannot read <home>/data: {error}"))?;
                let is_link = entry.file_type().is_ok_and(|kind| kind.is_symlink());
                if is_link {
                    return Err("an entry inside <home>/data is a symlink".to_string());
                }
            }
        }
    }
    let seed = home.join("identity.key");
    if std::fs::symlink_metadata(&seed).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return Err("<home>/identity.key is a symlink".to_string());
    }
    Ok(())
}

/// Check every static condition for `home`. Conditions 1 to 5 of the module
/// doc. Returns the first reason the proof tooling must refuse.
pub fn check_home(home: &Path) -> Result<(), String> {
    if std::env::var(PROOF_REPLAY_ENV).as_deref() != Ok("1") {
        return Err(format!(
            "{PROOF_REPLAY_ENV}=1 is not set in the daemon environment"
        ));
    }
    if !home.is_absolute() {
        return Err("the home path is not absolute".to_string());
    }
    let marker = home.join(SYNTHETIC_HOME_MARKER);
    let marker_is_file =
        std::fs::symlink_metadata(&marker).is_ok_and(|meta| meta.file_type().is_file());
    if !marker_is_file {
        return Err(format!(
            "the home has no regular file {SYNTHETIC_HOME_MARKER} (the operator must create it)"
        ));
    }
    require_plain_layout(home)?;
    let primary_error = "the home is, or overlaps, a primary home (~/.lastdb or ~/.folddb)";
    for path in [home.to_path_buf(), home.join("data")] {
        if primary::overlaps_primary(&path)? {
            return Err(primary_error.to_string());
        }
    }
    if crate::cloud::cloud_sync_file_state(home) != "unset"
        || crate::cloud::cloud_resume_required_path(home).exists()
    {
        return Err("the home has a cloud sync configuration".to_string());
    }
    Ok(())
}

/// Decide at boot whether to build the local proof engine for `home`.
///
/// A home that does not ask for it boots as before, silently. A home that
/// sets the variable but fails the guard also boots as before, with a loud
/// refusal: a stray variable must never stop or change a real daemon.
#[must_use]
pub fn boot_engine_requested(home: &Path) -> bool {
    if std::env::var_os(PROOF_REPLAY_ENV).is_none() {
        return false;
    }
    match check_home(home) {
        Ok(()) => {
            tracing::warn!(
                target: "lastdbd::proof_replay",
                home = %home.display(),
                "PROOF TOOLING ARMED: booting the local proof engine on a synthetic home. \
                 This daemon can export and replay mutation-log segments from a directory."
            );
            eprintln!(
                "lastdbd: PROOF TOOLING ARMED for synthetic home {} (no cloud; local proof engine)",
                home.display()
            );
            true
        }
        Err(reason) => {
            tracing::warn!(
                target: "lastdbd::proof_replay",
                home = %home.display(),
                %reason,
                "{PROOF_REPLAY_ENV} is set but the proof tooling is REFUSED; booting normally"
            );
            false
        }
    }
}

/// A caller-visible reason a proof verb was refused.
#[derive(Debug)]
pub struct Refusal(pub String);

/// Authorize one proof verb. Re-checks every condition, so a marker removed
/// or a cloud config added after boot stops the verb at once.
pub fn authorize(host: &Host, verb: &str) -> Result<Arc<fold_db::sync::SyncEngine>, Refusal> {
    if !host.proof_replay_engine {
        return Err(Refusal(
            "this daemon did not boot with the local proof engine".to_string(),
        ));
    }
    check_home(&host.home).map_err(Refusal)?;
    let engine = host
        .db
        .sync_engine()
        .ok_or_else(|| Refusal("the local proof engine is absent".to_string()))?;
    tracing::warn!(
        target: "lastdbd::proof_replay",
        home = %host.home.display(),
        verb,
        "PROOF TOOLING USED: owner-socket verb on a synthetic home"
    );
    Ok(engine)
}

/// The longest existing ancestor of `path` (the path itself when it exists).
fn nearest_existing(path: &Path) -> PathBuf {
    path.ancestors()
        .find(|ancestor| ancestor.exists())
        .map_or_else(|| PathBuf::from("/"), Path::to_path_buf)
}

/// Validate the directory argument of a proof verb.
///
/// The directory must be absolute and plain: no `..` component and no symlink
/// as its last component. A replay needs it to exist. It must not touch a
/// primary home, and an export checks that BEFORE it creates anything.
/// Returns the resolved path.
pub fn resolve_directory(raw: &str, must_exist: bool) -> Result<PathBuf, Refusal> {
    let path = PathBuf::from(raw);
    if !path.is_absolute() {
        return Err(Refusal("the directory path must be absolute".to_string()));
    }
    if path
        .components()
        .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(Refusal("the directory path has a .. component".to_string()));
    }
    if std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return Err(Refusal("the directory is a symlink".to_string()));
    }
    let overlap = Refusal("the directory is, or overlaps, a primary home".to_string());
    if must_exist {
        if !path.is_dir() {
            return Err(Refusal(format!("{} is not a directory", path.display())));
        }
    } else {
        // A new directory sits below its nearest existing ancestor, so only
        // "inside a primary home" can hold. Check before any create.
        let existing = nearest_existing(&path);
        if primary::inside_primary(&existing).map_err(Refusal)? {
            return Err(overlap);
        }
        std::fs::create_dir_all(&path)
            .map_err(|e| Refusal(format!("create {}: {e}", path.display())))?;
    }
    let resolved = std::fs::canonicalize(&path)
        .map_err(|e| Refusal(format!("resolve {}: {e}", path.display())))?;
    if primary::overlaps_primary(&path).map_err(Refusal)?
        || primary::overlaps_primary(&resolved).map_err(Refusal)?
    {
        return Err(overlap);
    }
    Ok(resolved)
}
