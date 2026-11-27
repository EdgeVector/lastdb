//! Host state of `lastdb upgrade`: paths, config, install id, rejected versions,
//! history, and the lock. The layout is contract section 8.

use std::fs::{self, File};
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

pub(super) const DEFAULT_LABEL: &str = "com.edgevector.lastdbd";

/// The longest `detail` a history line keeps.
const DETAIL_CAP: usize = 500;

pub(super) struct Paths {
    pub(super) home: PathBuf,
}

impl Paths {
    pub(super) fn new(home: &Path) -> Self {
        Self {
            home: home.to_path_buf(),
        }
    }

    pub(super) fn programs(&self) -> PathBuf {
        self.home.join("programs")
    }

    pub(super) fn program(&self, version: &str) -> PathBuf {
        self.programs().join(version)
    }

    pub(super) fn current(&self) -> PathBuf {
        self.home.join("current")
    }

    pub(super) fn previous(&self) -> PathBuf {
        self.home.join("previous")
    }

    pub(super) fn dir(&self) -> PathBuf {
        self.home.join("upgrade")
    }

    pub(super) fn cache_files(&self, channel: &str, target: &str) -> (PathBuf, PathBuf) {
        let dir = self.dir().join("list-cache");
        (
            dir.join(format!("{channel}-{target}.json")),
            dir.join(format!("{channel}-{target}.json.sig")),
        )
    }

    pub(super) fn socket(&self) -> PathBuf {
        lastdb_node::lastdb_uds::socket_path(&self.home.join("data"))
    }

    /// The program folder name a `current` or `previous` link names.
    pub(super) fn link_version(&self, link: &Path) -> Option<String> {
        let target = fs::read_link(link).ok()?;
        let mut parts = target.components();
        let first = parts.next()?.as_os_str().to_str()?;
        let version = parts.next()?.as_os_str().to_str()?;
        (first == "programs" && parts.next().is_none() && valid_version_dir(version))
            .then(|| version.to_owned())
    }
}

/// A version is a path segment under `programs/`. This allows no separator and no
/// leading dot.
pub(super) fn valid_version_dir(version: &str) -> bool {
    !version.is_empty()
        && version.len() <= 64
        && !version.starts_with('.')
        && version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'+' | b'_'))
}

pub(super) fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Write `bytes` to `path` through a temporary file in the same folder, so a reader
/// sees the old file or the new one.
pub(super) fn atomic_write(path: &Path, bytes: &[u8], mode: u32) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| format!("{} has no folder", path.display()))?;
    fs::create_dir_all(parent).map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("invalid file name")?;
    let tmp = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let _ = fs::remove_file(&tmp);
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(mode)
        .open(&tmp)
        .map_err(|e| format!("cannot create {}: {e}", tmp.display()))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| format!("cannot write {}: {e}", tmp.display()))?;
    fs::rename(&tmp, path).map_err(|e| {
        let _ = fs::remove_file(&tmp);
        format!("cannot replace {}: {e}", path.display())
    })?;
    File::open(parent)
        .and_then(|d| d.sync_all())
        .map_err(|e| format!("cannot sync {}: {e}", parent.display()))
}

/// Point `link` at `target` (a relative path) by renaming a new link over it.
pub(super) fn atomic_symlink(link: &Path, target: &Path) -> Result<(), String> {
    let parent = link.parent().ok_or("link has no folder")?;
    let name = link
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("invalid link name")?;
    let tmp = parent.join(format!(".{name}.{}.new", std::process::id()));
    let _ = fs::remove_file(&tmp);
    std::os::unix::fs::symlink(target, &tmp)
        .map_err(|e| format!("cannot create link {}: {e}", tmp.display()))?;
    fs::rename(&tmp, link).map_err(|e| {
        let _ = fs::remove_file(&tmp);
        format!("cannot switch link {}: {e}", link.display())
    })?;
    File::open(parent)
        .and_then(|d| d.sync_all())
        .map_err(|e| format!("cannot sync {}: {e}", parent.display()))
}

/// The install id when one exists. It reads and creates nothing.
pub(super) fn read_install_id(paths: &Paths) -> Option<String> {
    let text = fs::read_to_string(paths.dir().join("install-id")).ok()?;
    let id = text.trim();
    (id.len() == 32 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
        .then(|| id.to_owned())
}

/// The install id: 32 lowercase hex, created on first need, never sent anywhere.
pub(super) fn install_id(paths: &Paths) -> Result<String, String> {
    if let Some(id) = read_install_id(paths) {
        return Ok(id);
    }
    let path = paths.dir().join("install-id");
    let id = fold_db::hex::hex_lower(&rand::random::<[u8; 16]>());
    atomic_write(&path, format!("{id}\n").as_bytes(), 0o600)?;
    Ok(id)
}

pub(super) fn rejected(paths: &Paths) -> Vec<String> {
    fs::read_to_string(paths.dir().join("rejected"))
        .map(|text| {
            text.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn add_rejected(paths: &Paths, version: &str) -> Result<(), String> {
    let mut all = rejected(paths);
    if !all.iter().any(|v| v == version) {
        all.push(version.to_owned());
        write_rejected(paths, &all)?;
    }
    Ok(())
}

pub(super) fn clear_rejected(paths: &Paths, version: &str) -> Result<bool, String> {
    let all = rejected(paths);
    let kept: Vec<String> = all.iter().filter(|v| *v != version).cloned().collect();
    if kept.len() == all.len() {
        return Ok(false);
    }
    write_rejected(paths, &kept)?;
    Ok(true)
}

fn write_rejected(paths: &Paths, versions: &[String]) -> Result<(), String> {
    let mut text = versions.join("\n");
    if !text.is_empty() {
        text.push('\n');
    }
    atomic_write(&paths.dir().join("rejected"), text.as_bytes(), 0o600)
}

/// One upgrade attempt, as `history.jsonl` records it. It holds no record content.
pub(super) struct Attempt {
    pub(super) started_at: String,
    pub(super) from: String,
    pub(super) to: String,
    pub(super) trigger: &'static str,
}

impl Attempt {
    pub(super) fn begin(from: &str, to: &str, trigger: &'static str) -> Self {
        Self {
            started_at: now_rfc3339(),
            from: from.to_owned(),
            to: to.to_owned(),
            trigger,
        }
    }

    pub(super) fn finish(
        &self,
        paths: &Paths,
        result: &str,
        step: &str,
        detail: &str,
    ) -> Result<(), String> {
        let detail: String = detail.chars().take(DETAIL_CAP).collect();
        let line = json!({
            "started_at": self.started_at, "finished_at": now_rfc3339(),
            "from": self.from, "to": self.to, "trigger": self.trigger,
            "result": result, "step": step, "detail": detail,
        });
        let path = paths.dir().join("history.jsonl");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&path)
            .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
        writeln!(file, "{line}")
            .and_then(|()| file.sync_all())
            .map_err(|e| format!("cannot write {}: {e}", path.display()))
    }
}

/// The last history line, if any.
pub(super) fn last_history(paths: &Paths) -> Option<Value> {
    let text = fs::read_to_string(paths.dir().join("history.jsonl")).ok()?;
    let line = text.lines().rev().find(|l| !l.trim().is_empty())?;
    serde_json::from_str(line).ok()
}

pub(super) enum LockError {
    Busy,
    Other(String),
}

/// Take `upgrade/lock` without blocking. Keep the file open for as long as the lock
/// must hold.
pub(super) fn acquire_lock(paths: &Paths) -> Result<File, LockError> {
    let dir = paths.dir();
    fs::create_dir_all(&dir)
        .map_err(|e| LockError::Other(format!("cannot create upgrade folder: {e}")))?;
    let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o700));
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(dir.join("lock"))
        .map_err(|e| LockError::Other(format!("cannot open the upgrade lock: {e}")))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(fs::TryLockError::WouldBlock) => Err(LockError::Busy),
        Err(fs::TryLockError::Error(e)) => Err(LockError::Other(format!("cannot lock: {e}"))),
    }
}
