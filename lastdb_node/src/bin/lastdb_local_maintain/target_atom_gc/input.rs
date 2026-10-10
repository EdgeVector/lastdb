//! Immutable exact UUID and provenance inputs, checked again before deletion.

use super::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;

const MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Input {
    pub uuid_file: PathBuf,
    pub uuid_file_sha256: String,
    pub evidence_file: PathBuf,
    pub evidence_file_sha256: String,
    pub uuids: BTreeSet<String>,
}

pub(super) fn read(args: &TargetAtomGcArgs) -> Result<Input, String> {
    let bytes = read_exact(&args.target_atom_ids_file, &args.target_atom_ids_sha256)?;
    let mut uuids = BTreeSet::new();
    for uuid in std::str::from_utf8(&bytes).map_err(err)?.lines() {
        if !is_sha256(uuid) || !uuids.insert(uuid.to_string()) {
            return Err("target UUIDs require unique lowercase 64-character hex values".into());
        }
    }
    if uuids.is_empty() {
        return Err("target UUID input is empty".into());
    }
    let evidence = read_exact(&args.source_evidence_file, &args.source_evidence_sha256)?;
    if evidence.is_empty() {
        return Err("source evidence input is empty".into());
    }
    Ok(Input {
        uuid_file: std::fs::canonicalize(&args.target_atom_ids_file).map_err(err)?,
        uuid_file_sha256: args.target_atom_ids_sha256.clone(),
        evidence_file: std::fs::canonicalize(&args.source_evidence_file).map_err(err)?,
        evidence_file_sha256: args.source_evidence_sha256.clone(),
        uuids,
    })
}

fn read_exact(path: &Path, expected: &str) -> Result<Vec<u8>, String> {
    if !is_sha256(expected) {
        return Err("input digest requires lowercase SHA-256 hex".into());
    }
    let metadata = std::fs::symlink_metadata(path).map_err(err)?;
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() > MAX_INPUT_BYTES
    {
        return Err("input requires a private regular file within the size bound".into());
    }
    let bytes = std::fs::read(path).map_err(err)?;
    if bytes.len() as u64 > MAX_INPUT_BYTES || model::digest(&bytes) != expected {
        return Err("exact input file digest differs".into());
    }
    Ok(bytes)
}

pub(super) fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
