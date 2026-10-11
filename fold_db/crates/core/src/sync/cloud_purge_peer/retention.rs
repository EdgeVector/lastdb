//! Durable retention of the peer's old local data and cloud queues.

use super::state::{account_key, private_directory, write_atomic};
use super::{held, AdoptionPlan, AdoptionReceipt, CloudPurgePeer};
use crate::sync::error::SyncResult;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

mod verify;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedHome {
    pub directory: PathBuf,
    pub receipt_sha256: String,
    pub files: u64,
    pub bytes: u64,
}

#[derive(Serialize, Deserialize)]
struct RetentionManifest {
    format_version: u32,
    plan: AdoptionPlan,
    files: Vec<RetainedFile>,
}

#[derive(Serialize, Deserialize)]
struct RetainedFile {
    relative_path: PathBuf,
    sha256: String,
    bytes: u64,
}

impl CloudPurgePeer {
    pub fn verify_retained_home(
        &self,
        plan: &AdoptionPlan,
        retained: &RetainedHome,
    ) -> SyncResult<()> {
        let encoded = fs::read(retained.directory.join("retention.json"))?;
        if crate::hex::sha256_hex(&encoded) != retained.receipt_sha256 {
            return Err(held("the peer retention receipt changed"));
        }
        let manifest: RetentionManifest = serde_json::from_slice(&encoded)?;
        if manifest.format_version != 1
            || manifest.plan.account != plan.account
            || manifest.plan.operation_id != plan.operation_id
            || manifest.plan.generation != plan.generation
            || manifest.plan.source_id != plan.source_id
            || manifest.plan.replacement_manifest_sha256 != plan.replacement_manifest_sha256
        {
            return Err(held(
                "the peer retention receipt belongs to another adoption",
            ));
        }
        Ok(())
    }

    /// Flush a bounded archive page before the caller removes its active rows.
    /// No cloud receipt is accepted as a substitute for these local bytes.
    pub fn retain_queue_page(
        &self,
        plan: &AdoptionPlan,
        retained: &RetainedHome,
        namespace: &str,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> SyncResult<String> {
        self.verify_retained_home(plan, retained)?;
        let encoded_rows = rows
            .iter()
            .map(|(key, value)| {
                (
                    crate::sync::log::LogOp::encode_bytes(key),
                    crate::sync::log::LogOp::encode_bytes(value),
                )
            })
            .collect::<Vec<_>>();
        let value =
            serde_json::json!({"format_version":1,"namespace":namespace,"rows":encoded_rows});
        let encoded = serde_json::to_vec(&value)?;
        let digest = crate::hex::sha256_hex(&encoded);
        let archive = retained.directory.join("detached-queue");
        private_directory(&archive)?;
        write_atomic(&archive.join(format!("{digest}.json")), &encoded)?;
        Ok(digest)
    }

    /// `immutable_prior_home` must be a stopped home or a host-created cut
    /// under its mutation fence. This retains all files, including sync queues.
    /// It never reads through symlinks or accepts an archive inside its source.
    pub fn retain_directory(
        &self,
        plan: &AdoptionPlan,
        immutable_prior_home: &Path,
    ) -> SyncResult<RetainedHome> {
        let source = fs::canonicalize(immutable_prior_home)?;
        let base = self.directory.join("retained");
        private_directory(&base)?;
        let destination = base
            .join(crate::hex::sha256_hex(
                account_key(&plan.account)?.as_bytes(),
            ))
            .join(plan.generation.to_string())
            .join(&plan.operation_id);
        private_directory(&destination)?;
        let destination = fs::canonicalize(destination)?;
        if destination.starts_with(&source) || source.starts_with(&destination) {
            return Err(held("the retained peer home must be outside its source"));
        }
        let receipt_path = destination.join("retention.json");
        if receipt_path.exists() {
            return load_existing(plan, &destination);
        }
        let data = destination.join("home");
        private_directory(&data)?;
        let mut files = Vec::new();
        copy_directory(&source, &source, &data, &mut files, 0)?;
        files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        let count = files.len() as u64;
        let bytes = files.iter().map(|file| file.bytes).sum();
        let manifest = RetentionManifest {
            format_version: 1,
            plan: plan.clone(),
            files,
        };
        let encoded = serde_json::to_vec(&manifest)?;
        write_atomic(&receipt_path, &encoded)?;
        Ok(RetainedHome {
            directory: destination,
            receipt_sha256: crate::hex::sha256_hex(&encoded),
            files: count,
            bytes,
        })
    }
}

pub(super) fn verify_retention_receipt(receipt: &AdoptionReceipt) -> SyncResult<()> {
    let encoded = fs::read(receipt.retained_directory.join("retention.json"))?;
    if crate::hex::sha256_hex(&encoded) != receipt.retention_sha256 {
        return Err(held("the retained peer home receipt changed"));
    }
    let manifest: RetentionManifest = serde_json::from_slice(&encoded)?;
    if manifest.format_version != 1
        || !receipt.matches(&manifest.plan)
        || !receipt.retained_directory.join("home").is_dir()
    {
        return Err(held("the retained peer home does not match the adoption"));
    }
    Ok(())
}

fn load_existing(plan: &AdoptionPlan, destination: &Path) -> SyncResult<RetainedHome> {
    let encoded = fs::read(destination.join("retention.json"))?;
    let manifest: RetentionManifest = serde_json::from_slice(&encoded)?;
    if manifest.format_version != 1
        || manifest.plan.account != plan.account
        || manifest.plan.generation != plan.generation
        || manifest.plan.operation_id != plan.operation_id
        || manifest.plan.replacement_manifest_sha256 != plan.replacement_manifest_sha256
        || manifest.plan.source_id != plan.source_id
    {
        return Err(held("a different adoption already owns this retained home"));
    }
    verify::verify_files(destination, &manifest)?;
    Ok(RetainedHome {
        directory: destination.into(),
        receipt_sha256: crate::hex::sha256_hex(&encoded),
        files: manifest.files.len() as u64,
        bytes: manifest.files.iter().map(|file| file.bytes).sum(),
    })
}

fn copy_directory(
    root: &Path,
    source: &Path,
    destination: &Path,
    files: &mut Vec<RetainedFile>,
    depth: usize,
) -> SyncResult<()> {
    if depth > 64 {
        return Err(held(
            "the retained peer home exceeds the directory depth limit",
        ));
    }
    private_directory(destination)?;
    let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let metadata = entry.file_type()?;
        let target = destination.join(entry.file_name());
        if metadata.is_dir() {
            copy_directory(root, &entry.path(), &target, files, depth + 1)?;
        } else if metadata.is_file() {
            let (sha256, bytes) = copy_file(&entry.path(), &target)?;
            files.push(RetainedFile {
                relative_path: entry
                    .path()
                    .strip_prefix(root)
                    .map_err(|_| held("invalid retained path"))?
                    .into(),
                sha256,
                bytes,
            });
        } else if metadata.is_symlink() {
            return Err(held("the immutable peer home contains a symbolic link"));
        }
        // A stopped home may retain its old socket pathname. It contains no
        // bytes and is not part of the database or a durable upload queue.
    }
    File::open(destination)?.sync_all()?;
    Ok(())
}

fn copy_file(source: &Path, target: &Path) -> SyncResult<(String, u64)> {
    let mut input = File::open(source)?;
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut output = options.open(target)?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let length = input.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        output.write_all(&buffer[..length])?;
        digest.update(&buffer[..length]);
        bytes += length as u64;
    }
    output.sync_all()?;
    Ok((crate::hex::hex_lower(digest.finalize()), bytes))
}

fn digest_file(path: &Path) -> SyncResult<(String, u64)> {
    let mut input = File::open(path)?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let length = input.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        digest.update(&buffer[..length]);
        bytes += length as u64;
    }
    Ok((crate::hex::hex_lower(digest.finalize()), bytes))
}
