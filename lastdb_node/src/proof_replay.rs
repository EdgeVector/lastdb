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
//! 3. The home is not `~/.lastdb` or `~/.folddb`, nor a symlink target of
//!    either, nor inside one, nor above one.
//! 4. The home has no cloud sync configuration (`cloud_sync.json`, its paused
//!    form, or a cloud resume marker).
//! 5. The daemon booted with the local proof engine. The engine is only built
//!    when 1 to 4 hold at boot, so the verbs cannot run on a cloud engine.
//!
//! The verbs exist only on the owner socket. Every use logs at WARN.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::host::Host;

/// Environment variable that arms the proof tooling on a daemon.
pub const PROOF_REPLAY_ENV: &str = "LASTDB_PROOF_REPLAY";
/// File the operator creates in a synthetic home to allow the proof tooling.
pub const SYNTHETIC_HOME_MARKER: &str = ".synthetic-home";

/// Default primary home paths, as `$HOME`-relative names.
const PRIMARY_HOME_NAMES: [&str; 2] = [".lastdb", ".folddb"];

/// The home directory the OS account database names for this user.
///
/// `$HOME` can be rewritten per process. The account database cannot, so a
/// daemon started with a fake `$HOME` still cannot hide the real primary.
fn os_account_home() -> Option<PathBuf> {
    use std::os::unix::ffi::OsStrExt as _;

    let mut account = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut buffer = vec![0_u8; 16_384];
    let mut result = std::ptr::null_mut();
    // SAFETY: `getpwuid_r` fills `account` and `buffer` and sets `result`;
    // `account` is read only after a zero return and a non-null `result`.
    let owner = unsafe { libc::geteuid() };
    let rc = unsafe {
        libc::getpwuid_r(
            owner,
            account.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if rc != 0 || result.is_null() {
        return None;
    }
    // SAFETY: the call above succeeded, so `account` is initialized and
    // `pw_dir` points into `buffer`, which is still alive here.
    let account = unsafe { account.assume_init() };
    if account.pw_dir.is_null() {
        return None;
    }
    let dir = unsafe { std::ffi::CStr::from_ptr(account.pw_dir) };
    let path = PathBuf::from(std::ffi::OsStr::from_bytes(dir.to_bytes()));
    path.is_absolute().then_some(path)
}

/// User home directories to guard: `$HOME` and the OS account home.
fn user_homes() -> Vec<PathBuf> {
    let from_env = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute());
    let mut homes: Vec<PathBuf> = from_env.into_iter().chain(os_account_home()).collect();
    homes.dedup();
    homes
}

/// Every path that names a primary home, as written and fully resolved.
/// `None` when no user home is known: the caller must then refuse.
fn primary_home_paths() -> Option<Vec<PathBuf>> {
    let homes = user_homes();
    if homes.is_empty() {
        return None;
    }
    let mut paths = Vec::new();
    for user in homes {
        for name in PRIMARY_HOME_NAMES {
            let path = user.join(name);
            if let Ok(resolved) = std::fs::canonicalize(&path) {
                paths.push(resolved);
            }
            paths.push(path);
        }
    }
    Some(paths)
}

/// True when `candidate` is a primary home, sits inside one, or holds one.
/// Fails closed: an unknown `$HOME` counts as touching a primary home.
fn touches_primary_home(candidate: &Path) -> bool {
    let resolved = std::fs::canonicalize(candidate).unwrap_or_else(|_| candidate.to_path_buf());
    let Some(primaries) = primary_home_paths() else {
        return true;
    };
    primaries.iter().any(|primary| {
        [candidate, resolved.as_path()].iter().any(|path| {
            path.starts_with(primary) || primary.starts_with(path) || *path == primary.as_path()
        })
    })
}

/// Check every static condition for `home`. Conditions 1 to 4 of the module
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
    if touches_primary_home(home) {
        return Err(
            "the home is, or overlaps, a primary home (~/.lastdb or ~/.folddb)".to_string(),
        );
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

/// Validate the directory argument of a proof verb.
///
/// The directory must be absolute, must exist for a replay, and must not
/// touch a primary home. Returns the resolved path.
pub fn resolve_directory(raw: &str, must_exist: bool) -> Result<PathBuf, Refusal> {
    let path = PathBuf::from(raw);
    if !path.is_absolute() {
        return Err(Refusal("the directory path must be absolute".to_string()));
    }
    if must_exist && !path.is_dir() {
        return Err(Refusal(format!("{} is not a directory", path.display())));
    }
    if !must_exist {
        std::fs::create_dir_all(&path)
            .map_err(|e| Refusal(format!("create {}: {e}", path.display())))?;
    }
    let resolved = std::fs::canonicalize(&path)
        .map_err(|e| Refusal(format!("resolve {}: {e}", path.display())))?;
    if touches_primary_home(&resolved) {
        return Err(Refusal(
            "the directory is, or overlaps, a primary home".to_string(),
        ));
    }
    Ok(resolved)
}
