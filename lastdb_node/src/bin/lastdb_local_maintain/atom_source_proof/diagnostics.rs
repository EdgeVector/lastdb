//! Bounded private identity evidence for an aborted physical source decoder.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;

const KEY_LIMIT: usize = 64 * 1024;
const COLLECTION_LIMIT: usize = 1024;
const SAVE_ERROR: &str = "physical source decode refused; private diagnostic save failed";

pub(super) struct Sink {
    dir: PathBuf,
    identity: (u64, u64),
}

#[derive(Serialize)]
struct SourceError {
    version: u8,
    event: &'static str,
    error_kind: &'static str,
    collection: Option<String>,
    collection_sha256: String,
    shard: u16,
    group_id: Option<u32>,
    key_b64: Option<String>,
    key_complete: bool,
    key_bytes: u64,
    key_sha256: String,
    value_bytes: u64,
    value_sha256: String,
    raw_value_bytes: u64,
    raw_value_sha256: String,
}

impl Sink {
    pub(super) fn new(dir: &Path, home: &Path, root: &Path) -> Result<Self, String> {
        let meta = std::fs::symlink_metadata(dir).map_err(|_| SAVE_ERROR)?;
        let dir = std::fs::canonicalize(dir).map_err(|_| SAVE_ERROR)?;
        let home = std::fs::canonicalize(home).map_err(|_| SAVE_ERROR)?;
        let root = std::fs::canonicalize(root).map_err(|_| SAVE_ERROR)?;
        if dir.starts_with(home) || dir.starts_with(root) {
            return Err(SAVE_ERROR.into());
        }
        let sink = Self {
            dir,
            identity: (meta.dev(), meta.ino()),
        };
        sink.validate()?;
        Ok(sink)
    }

    fn validate(&self) -> Result<(), String> {
        let meta = std::fs::symlink_metadata(&self.dir).map_err(|_| SAVE_ERROR)?;
        if !meta.is_dir()
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.permissions().mode() & 0o077 != 0
            || (meta.dev(), meta.ino()) != self.identity
            || std::fs::canonicalize(&self.dir).map_err(|_| SAVE_ERROR)? != self.dir
        {
            return Err(SAVE_ERROR.into());
        }
        Ok(())
    }

    pub(super) fn record(
        &self,
        collection: &str,
        handle: (u16, Option<u32>),
        key: &[u8],
        values: (&[u8], &[u8]),
        error: &str,
    ) -> Result<(), String> {
        self.validate()?;
        let (raw, plain) = values;
        let error_kind = if error == "Invalid data: unsupported atom reference root key" {
            "unsupported_atom_reference_root"
        } else {
            "physical_source_decode_refused"
        };
        let report = SourceError {
            version: 1,
            event: "atom_source_decode_refused",
            error_kind,
            collection: (collection.len() <= COLLECTION_LIMIT).then(|| collection.into()),
            collection_sha256: digest(collection.as_bytes()),
            shard: handle.0,
            group_id: handle.1,
            key_b64: (key.len() <= KEY_LIMIT).then(|| STANDARD.encode(key)),
            key_complete: key.len() <= KEY_LIMIT,
            key_bytes: key.len() as u64,
            key_sha256: digest(key),
            value_bytes: plain.len() as u64,
            value_sha256: digest(plain),
            raw_value_bytes: raw.len() as u64,
            raw_value_sha256: digest(raw),
        };
        let bytes = serde_json::to_vec_pretty(&report).map_err(|_| SAVE_ERROR)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(self.dir.join("source-error.json"))
            .map_err(|_| SAVE_ERROR)?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| SAVE_ERROR)?;
        File::open(&self.dir)
            .and_then(|file| file.sync_all())
            .map_err(|_| SAVE_ERROR)?;
        self.validate()
    }
}
