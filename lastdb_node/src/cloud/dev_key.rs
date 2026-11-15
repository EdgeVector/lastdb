//! Saved Exemem developer API key validation, atomic 0600 persistence, and prefix revocation.
use super::{open_owned_home_dir, read_private_named};
use rustix::fs::{self as rfs, AtFlags, Mode, OFlags};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

pub const DEVELOPER_API_KEY_FILE: &str = "developer-api-key";

pub fn is_valid_developer_key(key: &str) -> bool {
    key.strip_prefix("em_")
        .is_some_and(|h| h.len() == 48 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

pub fn developer_key_prefix(key: &str) -> Option<String> {
    let hex_part = key.strip_prefix("em_")?;
    if hex_part.len() < 10 || !hex_part.bytes().take(10).all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let short: String = hex_part.chars().take(10).collect();
    Some(format!("em_{short}..."))
}

pub fn is_valid_key_prefix(prefix: &str) -> bool {
    prefix
        .strip_prefix("em_")
        .and_then(|s| s.strip_suffix("..."))
        .is_some_and(|h| h.len() == 10 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

pub fn load_saved_developer_key(home: &Path) -> Result<Option<Zeroizing<String>>, String> {
    const PRIV_ERR: &str = "developer API key file must be a private 0600 owner file";
    const LIM_ERR: &str = "developer API key file exceeds its byte limit";
    let Some(raw) = read_private_named(&home.join(DEVELOPER_API_KEY_FILE), 256, PRIV_ERR, LIM_ERR)?
    else {
        return Ok(None);
    };
    let raw = Zeroizing::new(raw);
    let trimmed = std::str::from_utf8(&raw)
        .map_err(|_| "saved developer API key file is not UTF-8")?
        .trim();
    is_valid_developer_key(trimmed)
        .then(|| Some(Zeroizing::new(trimmed.to_owned())))
        .ok_or_else(|| "saved developer API key file has an invalid key format".into())
}

pub fn save_developer_key(home: &Path, key: &str) -> Result<PathBuf, String> {
    if !is_valid_developer_key(key) {
        return Err("developer API key has an invalid format".into());
    }
    let dir = open_owned_home_dir(home)?;
    match rfs::statat(&dir, DEVELOPER_API_KEY_FILE, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(st) => {
            let reg = rfs::FileType::from_raw_mode(st.st_mode) == rfs::FileType::RegularFile;
            if !reg || st.st_uid != unsafe { libc::geteuid() } || st.st_nlink != 1 {
                return Err("refusing to replace non-regular developer-api-key entry".into());
            }
        }
        Err(rustix::io::Errno::NOENT) => {}
        Err(_) => return Err("cannot inspect existing developer-api-key entry".into()),
    }
    let temp = format!(".developer-api-key.{}.pending", uuid::Uuid::new_v4());
    let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut file = File::from(
        rfs::openat(&dir, temp.as_str(), flags, Mode::from_raw_mode(0o600))
            .map_err(|_| "cannot create staged developer-api-key file")?,
    );
    let write_res = (|| -> Result<(), String> {
        rfs::fchmod(&file, Mode::from_raw_mode(0o600))
            .map_err(|_| "cannot set 0600 mode on developer-api-key")?;
        let line = Zeroizing::new(format!("{key}\n").into_bytes());
        file.write_all(&line)
            .and_then(|()| file.sync_all())
            .map_err(|_| "cannot write developer-api-key")?;
        rfs::renameat(&dir, temp.as_str(), &dir, DEVELOPER_API_KEY_FILE)
            .map_err(|_| "cannot publish developer-api-key".to_owned())
    })();
    if write_res.is_err() {
        let _ = rfs::unlinkat(&dir, temp.as_str(), AtFlags::empty());
    }
    write_res?;
    dir.sync_all()
        .map_err(|_| "cannot sync home directory after saving developer-api-key".to_owned())?;
    Ok(home.join(DEVELOPER_API_KEY_FILE))
}

pub fn remove_saved_developer_key_if_prefix(home: &Path, prefix: &str) -> Result<bool, String> {
    let Some(saved) = load_saved_developer_key(home)? else {
        return Ok(false);
    };
    if developer_key_prefix(&saved).as_deref() != Some(prefix) {
        return Ok(false);
    }
    let dir = open_owned_home_dir(home)?;
    match rfs::unlinkat(&dir, DEVELOPER_API_KEY_FILE, AtFlags::empty()) {
        Ok(()) => {}
        Err(rustix::io::Errno::NOENT) => return Ok(false),
        Err(_) => return Err("cannot remove revoked developer-api-key file".into()),
    }
    dir.sync_all()
        .map_err(|_| "cannot sync home directory after removing developer-api-key".to_owned())?;
    Ok(true)
}
