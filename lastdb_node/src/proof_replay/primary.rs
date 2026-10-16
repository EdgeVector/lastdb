//! Primary-home identity checks for the proof guard.
//!
//! A path string cannot tell two spellings of one directory apart: a symlink,
//! or an alias such as a macOS firmlink, names the same directory in a second
//! way. So the checks compare file identity, the pair (device, inode), and
//! never path strings.
//!
//! A path touches a primary home when it is a primary home, sits inside one
//! (an ancestor of the path has a primary identity), or holds one (the path
//! has the identity of an ancestor of a primary home). The primary candidates
//! are `.lastdb` and `.folddb` under the `$HOME` directory and under the home
//! that the OS account database names, each as written and fully resolved.

use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

/// Default primary home names, relative to a user home directory.
const PRIMARY_HOME_NAMES: [&str; 2] = [".lastdb", ".folddb"];

/// File identity: (device, inode).
type FileId = (u64, u64);

/// The home directory the OS account database names for this user.
///
/// It comes from `geteuid` and `getpwuid_r`. `$HOME` can be rewritten per
/// process. The account database cannot, so a daemon started with a fake
/// `$HOME` still cannot hide the real primary.
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

/// Identity of `path` after symlinks. `None` when it does not exist. Any
/// other error refuses: an identity that cannot be read cannot be compared.
fn file_id(path: &Path) -> Result<Option<FileId>, String> {
    match std::fs::metadata(path) {
        Ok(meta) => Ok(Some((meta.dev(), meta.ino()))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("cannot inspect {}: {error}", path.display())),
    }
}

/// The path as written, and its fully resolved form when it resolves.
fn spellings(path: &Path) -> Vec<PathBuf> {
    let mut all = vec![path.to_path_buf()];
    if let Ok(resolved) = std::fs::canonicalize(path) {
        if resolved != path {
            all.push(resolved);
        }
    }
    all
}

/// Identities of the primary candidates, and of every ancestor of one.
struct Primaries {
    homes: Vec<FileId>,
    ancestors: Vec<FileId>,
}

impl Primaries {
    /// Fails closed: with no known user home there is nothing to compare.
    fn load() -> Result<Self, String> {
        let users = user_homes();
        if users.is_empty() {
            return Err("no user home directory is known".to_string());
        }
        let mut found = Self {
            homes: Vec::new(),
            ancestors: Vec::new(),
        };
        for user in users {
            for name in PRIMARY_HOME_NAMES {
                for spelling in spellings(&user.join(name)) {
                    found.homes.extend(file_id(&spelling)?);
                    for ancestor in spelling.ancestors().skip(1) {
                        found.ancestors.extend(file_id(ancestor)?);
                    }
                }
            }
        }
        Ok(found)
    }
}

/// True when `path` is a primary home or sits inside one.
pub(super) fn inside_primary(path: &Path) -> Result<bool, String> {
    let primaries = Primaries::load()?;
    for spelling in spellings(path) {
        for ancestor in spelling.ancestors() {
            if file_id(ancestor)?.is_some_and(|id| primaries.homes.contains(&id)) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// True when `path` is a primary home, sits inside one, or holds one.
pub(super) fn overlaps_primary(path: &Path) -> Result<bool, String> {
    if inside_primary(path)? {
        return Ok(true);
    }
    let primaries = Primaries::load()?;
    for spelling in spellings(path) {
        if file_id(&spelling)?.is_some_and(|id| primaries.ancestors.contains(&id)) {
            return Ok(true);
        }
    }
    Ok(false)
}
