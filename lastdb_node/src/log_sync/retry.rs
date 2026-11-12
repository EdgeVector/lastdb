//! Exact sealed payload for one accepted mutation.
//!
//! A retry sends these bytes again. A new nonce is used only when no seal
//! has been stored. The writer sequence stored here is the cloud sequence.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::disk;

const META_LIMIT: usize = 4096;
const SEAL_LIMIT: usize = 1024 * 1024 + 4096;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Seal {
    pub writer_seq: u64,
    pub authored_at_ms: u64,
    pub namespace: String,
}

pub(super) fn load(
    home: &Path,
    database_id: &str,
    mutation_id: &str,
) -> Result<Option<(Seal, Vec<u8>)>, ()> {
    if !id_ok(mutation_id) {
        return Err(());
    }
    let Some(meta) = disk::read_limited(&meta_path(home, database_id, mutation_id), META_LIMIT)
        .map_err(|_| ())?
    else {
        return Ok(None);
    };
    let seal: Seal = serde_json::from_slice(&meta).map_err(|_| ())?;
    if seal.writer_seq == 0 || seal.namespace.is_empty() {
        return Err(());
    }
    let Some(bytes) = disk::read_limited(&seal_path(home, database_id, mutation_id), SEAL_LIMIT)
        .map_err(|_| ())?
    else {
        return Err(());
    };
    if bytes.is_empty() {
        return Err(());
    }
    Ok(Some((seal, bytes)))
}

pub(super) fn store(
    home: &Path,
    database_id: &str,
    mutation_id: &str,
    seal: &Seal,
    envelope: &[u8],
) -> Result<(), ()> {
    if !id_ok(mutation_id) || envelope.is_empty() || seal.writer_seq == 0 {
        return Err(());
    }
    let meta = serde_json::to_vec(seal).map_err(|_| ())?;
    disk::write_private(&seal_path(home, database_id, mutation_id), envelope).map_err(|_| ())?;
    disk::write_private(&meta_path(home, database_id, mutation_id), &meta).map_err(|_| ())?;
    Ok(())
}

pub(super) fn store_file(
    home: &Path,
    database_id: &str,
    mutation_id: &str,
    ciphertext: &[u8],
) -> Result<(), ()> {
    if !id_ok(mutation_id) || ciphertext.is_empty() {
        return Err(());
    }
    disk::write_private(&file_path(home, database_id, mutation_id), ciphertext).map_err(|_| ())
}

fn id_ok(mutation_id: &str) -> bool {
    !mutation_id.is_empty()
        && mutation_id.len() <= 128
        && mutation_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn meta_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
    seal_dir(home, database_id).join(format!("{mutation_id}.json"))
}

fn seal_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
    seal_dir(home, database_id).join(format!("{mutation_id}.bin"))
}

fn file_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
    seal_dir(home, database_id).join(format!("{mutation_id}.file"))
}

fn seal_dir(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id).join("seals")
}
