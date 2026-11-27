//! The checks a staged pair passes before it is installed. Hashes and sizes come
//! first. On macOS the code signature and notarization pass before any downloaded
//! binary runs; then `--version`, then the home format (design section 8, step 2).

use std::fs::{self, File};
use std::io::Read as _;
use std::path::Path;
use std::time::Duration;

use fold_db::storage::disk_pack::NODE_MARKER;
use sha2::{Digest as _, Sha256};

use super::list::Release;
use super::procs;
use super::stage::{refused, StageError};
use super::state::valid_version_dir;

pub(super) fn sha256_file(path: &Path) -> Result<String, String> {
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
    Ok(fold_db::hex::hex_lower(hasher.finalize()))
}

/// The two binaries are regular files with the listed size and hash.
pub(super) fn check_binaries(dir: &Path, release: &Release) -> Result<(), StageError> {
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

/// The archive holds exactly `lastdb` and `lastdbd`, as top-level names.
pub(super) fn check_archive_listing(archive: &Path) -> Result<(), StageError> {
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
pub(super) fn check_code_signature(dir: &Path, notarized: bool) -> Result<(), StageError> {
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
pub(super) fn check_versions(dir: &Path, expect: Option<&str>) -> Result<String, StageError> {
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
