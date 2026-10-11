//! Owner-only copies and resumable directory switches.

use super::*;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

pub(super) const SIDECARS: &[&str] = &[
    "laststore_high_water.json",
    "laststore_backup_manifest.json",
    "laststore_backup_known_present.json",
    "laststore_chunk_sha_memo.json",
    "laststore_pending_purged_atom_retirements.json",
    ".cloud_primary_resume_job_v1.json",
    ".cloud_backup_unproved_flush_claim",
];

pub(super) fn copy_data(source: &Path, destination: &Path) -> Result<(), String> {
    copy_directory(source, destination, 0)
}

fn copy_directory(source: &Path, destination: &Path, depth: u32) -> Result<(), String> {
    if depth > 64 {
        return Err("cloud adoption data directory exceeds the depth limit".into());
    }
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(destination)
        .map_err(message)?;
    for entry in fs::read_dir(source).map_err(message)? {
        let entry = entry.map_err(message)?;
        let kind = entry.file_type().map_err(message)?;
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_directory(&entry.path(), &target, depth + 1)?;
        } else if kind.is_file() {
            copy_file(&entry.path(), &target)?;
        } else if kind.is_symlink() {
            return Err("cloud adoption data contains a symbolic link".into());
        }
    }
    File::open(destination)
        .and_then(|file| file.sync_all())
        .map_err(message)
}

fn copy_file(source: &Path, target: &Path) -> Result<(), String> {
    if !fs::symlink_metadata(source)
        .map_err(message)?
        .file_type()
        .is_file()
    {
        return Err("cloud adoption source is not a regular file".into());
    }
    if clone_file(source, target)? {
        fs::set_permissions(target, fs::Permissions::from_mode(0o600)).map_err(message)?;
        return File::open(target)
            .and_then(|file| file.sync_all())
            .map_err(message);
    }
    let mut input = File::open(source).map_err(message)?;
    let mut output = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(target)
        .map_err(message)?;
    std::io::copy(&mut input, &mut output).map_err(message)?;
    output
        .flush()
        .and_then(|()| output.sync_all())
        .map_err(message)
}

#[cfg(target_os = "macos")]
fn clone_file(source: &Path, target: &Path) -> Result<bool, String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    extern "C" {
        fn clonefile(
            source: *const libc::c_char,
            target: *const libc::c_char,
            flags: libc::c_int,
        ) -> libc::c_int;
    }
    let source = CString::new(source.as_os_str().as_bytes()).map_err(message)?;
    let target = CString::new(target.as_os_str().as_bytes()).map_err(message)?;
    if unsafe { clonefile(source.as_ptr(), target.as_ptr(), 0) } == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        return Err("cloud adoption copy destination already exists".into());
    }
    Ok(false)
}

#[cfg(not(target_os = "macos"))]
fn clone_file(_source: &Path, _target: &Path) -> Result<bool, String> {
    Ok(false)
}

pub(super) fn copy_identity(home: &Path, candidate: &Path) -> Result<(), String> {
    copy_file(
        &home.join(crate::host::IDENTITY_KEY_FILE),
        &candidate.join(crate::host::IDENTITY_KEY_FILE),
    )
}

pub(super) fn copy_device_id(home: &Path, candidate: &Path) -> Result<(), String> {
    let source = home.join("data/.device_id");
    let target = candidate.join("data/.device_id");
    if !fs::symlink_metadata(&source)
        .map_err(message)?
        .file_type()
        .is_file()
    {
        return Err("the stopped peer device identity is absent".into());
    }
    if fs::metadata(&source).map_err(message)?.len() > 256 {
        return Err("the stopped peer device identity is too large".into());
    }
    let bytes = fs::read(&source).map_err(message)?;
    if target.exists() {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&target)
            .map_err(message)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(message)?;
    } else {
        copy_file(&source, &target)?;
    }
    File::open(candidate.join("data"))
        .and_then(|file| file.sync_all())
        .map_err(message)
}

pub(super) fn retain_sidecars(
    home: &Path,
    retained: &Path,
) -> Result<Vec<proof::FileProof>, String> {
    let destination = retained.join("sidecars");
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&destination)
        .map_err(message)?;
    let facts = proof::sidecars_in(home)?;
    for file in &facts {
        let target = destination.join(&file.name);
        if target.exists() {
            proof::verify_file(&destination, file)?;
        } else {
            copy_file(&home.join(&file.name), &target)?;
        }
    }
    File::open(&destination)
        .and_then(|file| file.sync_all())
        .map_err(message)?;
    Ok(facts)
}

pub(super) fn copy_root_sidecars(root: &Path, candidate: &Path) -> Result<(), String> {
    for name in &SIDECARS[..2] {
        let source = root.join(name);
        if source.is_file() {
            copy_file(&source, &candidate.join(name))?;
        }
    }
    File::open(candidate)
        .and_then(|file| file.sync_all())
        .map_err(message)
}

pub(super) fn switch_data(
    home: &Path,
    candidate: &Path,
    retained: &Path,
    proof: &proof::SwitchProof,
) -> Result<(), String> {
    let active = home.join("data");
    let replacement = candidate.join("data");
    let prior = retained.join("prior-active-data");
    if replacement.is_dir() && proof::data_sha256(&replacement)? != proof.installed_data_sha256 {
        return Err("cloud adoption candidate changed before the switch".into());
    }
    if !prior.exists() {
        if !replacement.is_dir() || !active.is_dir() {
            return Err("cloud adoption directory switch has an ambiguous source".into());
        }
        fs::rename(&active, &prior).map_err(message)?;
        File::open(home)
            .and_then(|file| file.sync_all())
            .map_err(message)?;
        File::open(retained)
            .and_then(|file| file.sync_all())
            .map_err(message)?;
    }
    if replacement.exists() {
        if active.exists() {
            return Err("both cloud adoption data destinations are active".into());
        }
        fs::rename(&replacement, &active).map_err(message)?;
        File::open(home)
            .and_then(|file| file.sync_all())
            .map_err(message)?;
        File::open(candidate)
            .and_then(|file| file.sync_all())
            .map_err(message)?;
    }
    if !active.is_dir() {
        return Err("cloud adoption replacement data is absent".into());
    }
    switch_sidecars(home, candidate, retained, proof)?;
    proof::verify_installed(home, proof)
}

fn switch_sidecars(
    home: &Path,
    candidate: &Path,
    retained: &Path,
    proof: &proof::SwitchProof,
) -> Result<(), String> {
    let archive = retained.join("retired-sidecars");
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&archive)
        .map_err(message)?;
    for name in SIDECARS {
        let current = home.join(name);
        let old = archive.join(name);
        let replacement = candidate.join(name);
        if !old.exists() {
            if let Some(file) = proof
                .retained_sidecars
                .iter()
                .find(|file| file.name == Path::new(name))
            {
                proof::verify_file(home, file)?;
                fs::rename(&current, &old).map_err(message)?;
                File::open(&archive)
                    .and_then(|file| file.sync_all())
                    .map_err(message)?;
            } else if current.exists() {
                if replacement.exists() {
                    return Err("cloud adoption found an unrecorded old sidecar".into());
                }
                let installed = proof
                    .installed_sidecars
                    .iter()
                    .find(|file| file.name == Path::new(name))
                    .ok_or("cloud adoption found an unrecorded installed sidecar")?;
                proof::verify_file(home, installed)?;
            }
        }
        if replacement.exists() {
            if current.exists() {
                return Err("cloud adoption sidecar switch is ambiguous".into());
            }
            if let Some(file) = proof
                .installed_sidecars
                .iter()
                .find(|file| file.name == Path::new(name))
            {
                proof::verify_file(candidate, file)?;
            } else {
                return Err("cloud adoption found an unrecorded candidate sidecar".into());
            }
            fs::rename(&replacement, &current).map_err(message)?;
        }
    }
    for directory in [home, candidate, retained, archive.as_path()] {
        File::open(directory)
            .and_then(|file| file.sync_all())
            .map_err(message)?;
    }
    Ok(())
}
