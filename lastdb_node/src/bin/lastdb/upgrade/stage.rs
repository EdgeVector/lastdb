//! Download, verify and stage a program pair. Nothing in a download runs before its
//! archive hash, binary hashes and sizes pass. On macOS the code signature and
//! notarization pass before the binaries run, then `--version`, then the home
//! format. Design section 8, step 2.

use std::fs::{self, File};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fold_db::storage::disk_pack::NODE_MARKER;
use sha2::{Digest as _, Sha256};

use super::list::Release;
use super::procs;
use super::state::{valid_version_dir, Paths};

/// The most bytes an archive may hold. The list's `archive_bytes` is checked too.
const MAX_ARCHIVE_BYTES: u64 = 512 * 1024 * 1024;

pub(super) enum StageError {
    /// The network did not deliver. Another check may succeed.
    Transient(String),
    /// The release or pair fails a check. Taking it again would fail again.
    Refused(String),
}

impl StageError {
    pub(super) fn message(&self) -> &str {
        match self {
            Self::Transient(m) | Self::Refused(m) => m,
        }
    }
}

fn refused(text: impl Into<String>) -> StageError {
    StageError::Refused(text.into())
}

pub(super) struct Staged {
    pub(super) version: String,
    pub(super) dir: PathBuf,
}

/// Removes the staging folder when it goes out of scope, on success and on failure.
struct Staging(PathBuf);

impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = File::open(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(fold_db::hex::hex_lower(&hasher.finalize()))
}

/// The two binaries are regular files with the listed size and hash.
fn check_binaries(dir: &Path, release: &Release) -> Result<(), StageError> {
    for binary in &release.binaries {
        let path = dir.join(&binary.name);
        let meta = fs::symlink_metadata(&path)
            .map_err(|e| refused(format!("{} is missing: {e}", binary.name)))?;
        if !meta.is_file() || meta.len() != binary.size {
            return Err(refused(format!(
                "{} is not a regular file of the listed size",
                binary.name
            )));
        }
        if sha256_file(&path).map_err(StageError::Refused)? != binary.sha256 {
            return Err(refused(format!(
                "{} does not match its listed hash",
                binary.name
            )));
        }
    }
    Ok(())
}

async fn download(url: &str, dest: &Path, expect_bytes: u64) -> Result<String, StageError> {
    let client = crate::app_registry_index::archive_http_client().map_err(StageError::Transient)?;
    let mut resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| StageError::Transient(format!("download failed: {}", e.without_url())))?;
    if !resp.status().is_success() {
        return Err(StageError::Transient(format!(
            "download returned {}",
            resp.status().as_u16()
        )));
    }
    if resp.content_length().is_some_and(|n| n > expect_bytes) {
        return Err(refused("the archive is larger than the list says"));
    }
    let mut file =
        File::create(dest).map_err(|e| refused(format!("cannot create archive file: {e}")))?;
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| StageError::Transient(format!("download failed: {}", e.without_url())))?
    {
        total += chunk.len() as u64;
        if total > expect_bytes {
            return Err(refused("the archive is larger than the list says"));
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .map_err(|e| refused(format!("cannot write archive file: {e}")))?;
    }
    file.sync_all()
        .map_err(|e| refused(format!("cannot write archive file: {e}")))?;
    if total != expect_bytes {
        return Err(refused("the archive is smaller than the list says"));
    }
    Ok(fold_db::hex::hex_lower(&hasher.finalize()))
}

/// The archive holds exactly `lastdb` and `lastdbd`, as top-level names.
fn check_archive_listing(archive: &Path) -> Result<(), StageError> {
    let archive_text = archive
        .to_str()
        .ok_or_else(|| refused("archive path is not UTF-8"))?;
    let out = procs::run(
        Path::new("tar"),
        &["-tzf", archive_text],
        Duration::from_secs(120),
    )
    .map_err(StageError::Refused)?;
    if !out.ok {
        return Err(refused(format!(
            "the archive cannot be read: {}",
            out.stderr.trim()
        )));
    }
    let mut names: Vec<String> = out
        .stdout_text()
        .lines()
        .map(|l| l.trim().trim_start_matches("./").to_owned())
        .filter(|l| !l.is_empty())
        .collect();
    names.sort();
    if names != ["lastdb", "lastdbd"] {
        return Err(refused("the archive must hold exactly lastdb and lastdbd"));
    }
    Ok(())
}

/// Developer ID signature, then notarization. A `--pair` is only code-sign verified:
/// a local build is not notarized.
fn check_code_signature(dir: &Path, notarized: bool) -> Result<(), StageError> {
    if !cfg!(target_os = "macos") {
        return Ok(());
    }
    for name in ["lastdb", "lastdbd"] {
        let path = dir.join(name);
        let text = path.to_str().ok_or_else(|| refused("path is not UTF-8"))?;
        let verify = procs::run(
            Path::new("codesign"),
            &["--verify", "--strict", text],
            Duration::from_secs(60),
        )
        .map_err(StageError::Refused)?;
        if !verify.ok {
            return Err(refused(format!("{name} fails codesign --verify --strict")));
        }
        if !notarized {
            continue;
        }
        let detail = procs::run(
            Path::new("codesign"),
            &["-dvv", text],
            Duration::from_secs(60),
        )
        .map_err(StageError::Refused)?;
        if !detail.stderr.contains("Authority=Developer ID Application") {
            return Err(refused(format!("{name} is not signed with a Developer ID")));
        }
        let assess = procs::run(
            Path::new("spctl"),
            &["--assess", "--type", "execute", text],
            Duration::from_secs(120),
        )
        .map_err(StageError::Refused)?;
        if !assess.ok {
            return Err(refused(format!(
                "{name} is not notarized (spctl --assess failed)"
            )));
        }
    }
    Ok(())
}

/// `build` as the daemon reports it equals the list version, up to a suffix after a
/// space, a plus or an opening bracket.
pub(super) fn build_matches(build: &str, version: &str) -> bool {
    build
        .split(|c: char| c.is_whitespace() || c == '+' || c == '(')
        .next()
        == Some(version)
}

/// Both binaries report the version. Returns it.
fn check_versions(dir: &Path, expect: Option<&str>) -> Result<String, StageError> {
    let daemon = procs::version_word(&dir.join("lastdbd")).map_err(StageError::Refused)?;
    let cli = procs::version_word(&dir.join("lastdb")).map_err(StageError::Refused)?;
    if daemon != cli {
        return Err(refused(format!(
            "lastdb reports {cli} but lastdbd reports {daemon}"
        )));
    }
    if let Some(version) = expect {
        if !build_matches(&daemon, version) {
            return Err(refused(format!(
                "the pair reports version {daemon}, not the listed {version}"
            )));
        }
    }
    if !valid_version_dir(&daemon) {
        return Err(refused(
            "the pair reports a version that cannot name a folder",
        ));
    }
    Ok(daemon)
}

/// `lastdbd --accepted-home-format` must print the home's marker byte for byte. A home
/// with no marker holds no data yet, so there is nothing to compare.
pub(super) fn check_home_format(dir: &Path, home: &Path) -> Result<(), StageError> {
    let marker = home.join("data").join(NODE_MARKER);
    let Ok(have) = fs::read(&marker) else {
        return Ok(());
    };
    let out = procs::run(
        &dir.join("lastdbd"),
        &["--accepted-home-format"],
        Duration::from_secs(15),
    )
    .map_err(StageError::Refused)?;
    if !out.ok {
        return Err(refused(
            "this build cannot say which home format it opens (no --accepted-home-format); it is refused",
        ));
    }
    if out.stdout != have {
        return Err(refused(
            "this build opens a different home format than this home holds. A release with a new home format is never an upgrade. Make the one-time copy: start the new build on a new home and copy the data across, as the upgrade documentation describes",
        ));
    }
    Ok(())
}

fn finalize_modes(dir: &Path, release_json: Option<&[u8]>) -> Result<(), StageError> {
    for name in ["lastdb", "lastdbd"] {
        fs::set_permissions(dir.join(name), fs::Permissions::from_mode(0o555))
            .map_err(|e| refused(format!("cannot set mode on {name}: {e}")))?;
    }
    if let Some(bytes) = release_json {
        let path = dir.join("release.json");
        fs::write(&path, bytes).map_err(|e| refused(format!("cannot write release.json: {e}")))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o444))
            .map_err(|e| refused(format!("cannot set mode on release.json: {e}")))?;
    }
    fs::set_permissions(dir, fs::Permissions::from_mode(0o755))
        .map_err(|e| refused(format!("cannot set mode on the program folder: {e}")))
}

fn new_staging(paths: &Paths, version: &str) -> Result<(Staging, PathBuf), StageError> {
    fs::create_dir_all(paths.programs())
        .map_err(|e| refused(format!("cannot create the programs folder: {e}")))?;
    let root = paths
        .programs()
        .join(format!(".staging-{version}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir(&root).map_err(|e| refused(format!("cannot create the staging folder: {e}")))?;
    let guard = Staging(root.clone());
    let pair = root.join("pair");
    fs::create_dir(&pair).map_err(|e| refused(format!("cannot create the staging folder: {e}")))?;
    Ok((guard, pair))
}

fn install_dir(paths: &Paths, pair: &Path, version: &str) -> Result<PathBuf, StageError> {
    let target = paths.program(version);
    fs::rename(pair, &target)
        .map_err(|e| refused(format!("cannot place {}: {e}", target.display())))?;
    Ok(target)
}

/// Download, verify and place a list entry as `programs/<version>`.
pub(super) fn stage_release(
    paths: &Paths,
    release: &Release,
    home: &Path,
) -> Result<Staged, StageError> {
    let version = release.version.as_str();
    let target = paths.program(version);
    if target.exists() {
        // An earlier install of this version: keep it only if it is exactly the entry.
        check_binaries(&target, release)?;
        check_home_format(&target, home)?;
        return Ok(Staged {
            version: version.to_owned(),
            dir: target,
        });
    }
    if release.archive_bytes > MAX_ARCHIVE_BYTES {
        return Err(refused("the archive is larger than this updater accepts"));
    }
    let (guard, pair) = new_staging(paths, version)?;
    let archive = guard.0.join("archive.tar.gz");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| refused(format!("cannot construct download runtime: {e}")))?;
    let hash = runtime.block_on(download(
        &release.archive_url,
        &archive,
        release.archive_bytes,
    ))?;
    if hash != release.archive_sha256 {
        return Err(refused("the archive does not match its listed hash"));
    }
    check_archive_listing(&archive)?;
    let archive_text = archive
        .to_str()
        .ok_or_else(|| refused("archive path is not UTF-8"))?;
    let pair_text = pair
        .to_str()
        .ok_or_else(|| refused("staging path is not UTF-8"))?;
    let out = procs::run(
        Path::new("tar"),
        &["-xzf", archive_text, "-C", pair_text],
        Duration::from_secs(120),
    )
    .map_err(StageError::Refused)?;
    if !out.ok {
        return Err(refused(format!(
            "the archive cannot be unpacked: {}",
            out.stderr.trim()
        )));
    }
    let _ = fs::remove_file(&archive);
    check_binaries(&pair, release)?;
    check_code_signature(&pair, release.signing == "developer-id")?;
    check_versions(&pair, Some(version))?;
    check_home_format(&pair, home)?;
    let entry = serde_json::to_vec_pretty(release)
        .map_err(|e| refused(format!("cannot encode release.json: {e}")))?;
    finalize_modes(&pair, Some(&entry))?;
    let dir = install_dir(paths, &pair, version)?;
    Ok(Staged {
        version: version.to_owned(),
        dir,
    })
}

/// Copy a local pair (a folder with `lastdb` and `lastdbd`) into `programs/<version>`.
/// The version is what the pair reports. There is no `release.json`.
pub(super) fn stage_pair(paths: &Paths, source: &Path, home: &Path) -> Result<Staged, StageError> {
    let probe_version = check_versions(source, None)?;
    let target = paths.program(&probe_version);
    if target.exists() {
        for name in ["lastdb", "lastdbd"] {
            let (a, b) = (
                sha256_file(&source.join(name)).map_err(StageError::Refused)?,
                sha256_file(&target.join(name)).map_err(StageError::Refused)?,
            );
            if a != b {
                return Err(refused(format!(
                    "programs/{probe_version} already holds a different {name}"
                )));
            }
        }
        check_home_format(&target, home)?;
        return Ok(Staged {
            version: probe_version,
            dir: target,
        });
    }
    let (_guard, pair) = new_staging(paths, &probe_version)?;
    for name in ["lastdb", "lastdbd"] {
        fs::copy(source.join(name), pair.join(name))
            .map_err(|e| refused(format!("cannot copy {name}: {e}")))?;
    }
    check_code_signature(&pair, false)?;
    let version = check_versions(&pair, Some(probe_version.as_str()))?;
    check_home_format(&pair, home)?;
    finalize_modes(&pair, None)?;
    let dir = install_dir(paths, &pair, &version)?;
    Ok(Staged { version, dir })
}

/// Write `release.json` into an installed program folder when its binaries are exactly
/// the ones the list entry names.
pub(super) fn attach_release(dir: &Path, release: &Release) -> Result<(), String> {
    check_binaries(dir, release).map_err(|e| e.message().to_owned())?;
    let entry = serde_json::to_vec_pretty(release).map_err(|e| e.to_string())?;
    let path = dir.join("release.json");
    if path.exists() {
        return Ok(());
    }
    fs::write(&path, entry).map_err(|e| format!("cannot write release.json: {e}"))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o444))
        .map_err(|e| format!("cannot set mode on release.json: {e}"))
}

/// The release record of a program folder, if it has one.
pub(super) fn read_release(dir: &Path) -> Option<Release> {
    let bytes = fs::read(dir.join("release.json")).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Remove program folders that are neither `current` nor `previous`. Run only after a
/// successful upgrade.
pub(super) fn prune(paths: &Paths) {
    let keep: Vec<String> = [paths.current(), paths.previous()]
        .iter()
        .filter_map(|link| paths.link_version(link))
        .collect();
    let Ok(entries) = fs::read_dir(paths.programs()) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') || keep.contains(&name) || !entry.path().is_dir() {
            continue;
        }
        let _ = fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o755));
        let _ = fs::remove_dir_all(entry.path());
    }
}
