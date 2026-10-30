//! Explicit stopped-home feature activation; encrypted data files stay unchanged.

use super::{map, run_admission, run_node, PackRuntime, StorageError, StorageResult, NODE_MARKER};
use laststore::disk_run::home::{ReadBudget, RunRead};
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

const PENDING: &str = ".laststore-alternate-format.pending";

#[derive(Debug, Serialize)]
pub struct FormatUpgrade {
    pub previous_format: u16,
    pub current_format: u16,
    pub changed: bool,
    pub encrypted_files_unchanged: bool,
}

/// The caller first refuses an active daemon and verifies its shutdown receipt.
/// Native owner locks then exclude writers throughout proof and marker publication.
pub fn enable_alternate_keys(root: &Path, key: &[u8; 32]) -> StorageResult<FormatUpgrade> {
    if root.canonicalize()? != root {
        return Err(invalid("format activation requires a canonical directory"));
    }
    let folder = File::open(root)?;
    folder
        .try_lock()
        .map_err(|_| invalid("node directory is owned"))?;
    let marker = marker_file(&root.join(NODE_MARKER), false)?;
    let mut bytes = Vec::new();
    (&marker).take(129).read_to_end(&mut bytes)?;
    let profile =
        run_admission::profile(&bytes).ok_or_else(|| invalid("unsupported node format"))?;
    let budget = ReadBudget {
        pages: 16384,
        blocks: 65536,
        bytes: 8 * 1024 * 1024,
        keys: 16,
    };
    let mut main = RunRead::open_existing(
        &root.join(run_node::RUN_DIR),
        &run_node::home_key(key),
        budget,
    )
    .map_err(map)?;
    let _cas = RunRead::open_existing(
        &root.join(run_node::CAS_DIR),
        &run_node::cas_key(key),
        budget,
    )
    .map_err(map)?;
    run_admission::verify_proofs(&mut main, key)?;
    if std::fs::read(root.join(NODE_MARKER))? != bytes {
        return Err(invalid("node marker changed during activation"));
    }
    let previous_format = profile + 2;
    if profile == 4 {
        return Ok(FormatUpgrade {
            previous_format,
            current_format: 6,
            changed: false,
            encrypted_files_unchanged: true,
        });
    }
    let pending = root.join(PENDING);
    let mut file = marker_file(&pending, true)?;
    let mut previous = Vec::new();
    Read::by_ref(&mut file)
        .take(129)
        .read_to_end(&mut previous)?;
    if !run_admission::TYPED_NODE_BYTES.starts_with(&previous)
        && !run_admission::FORMAT_FIVE_BYTES.starts_with(&previous)
    {
        return Err(invalid("format activation has an unknown pending marker"));
    }
    file.set_len(0)?;
    std::io::Seek::rewind(&mut file)?;
    file.write_all(run_admission::TYPED_NODE_BYTES)?;
    file.sync_all()?;
    std::fs::rename(&pending, root.join(NODE_MARKER))?;
    folder.sync_all()?;
    Ok(FormatUpgrade {
        previous_format,
        current_format: 6,
        changed: true,
        encrypted_files_unchanged: true,
    })
}

fn marker_file(path: &Path, create: bool) -> StorageResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(create)
        .create(create)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    // Existing format-4 creation uses the process umask for this public header.
    // Refuse another writer, but permit its normal read permissions. New pending
    // markers remain private and cannot reuse a hard-linked file.
    let forbidden = if create { 0o077 } else { 0o022 };
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & forbidden != 0
        || meta.len() > 128
    {
        return Err(invalid(
            "format marker must be bounded, owned, and protected from other writers",
        ));
    }
    if create && meta.nlink() != 1 {
        return Err(invalid("pending format marker has another link"));
    }
    Ok(file)
}

impl PackRuntime {
    pub(crate) fn require_alternate_format(&self, compound: bool) -> StorageResult<()> {
        let root = self
            .physical
            .path()
            .parent()
            .ok_or_else(|| invalid("native node parent is absent"))?;
        let bytes = std::fs::read(root.join(NODE_MARKER))?;
        if bytes != run_admission::TYPED_NODE_BYTES
            && (compound || bytes != run_admission::FORMAT_FIVE_BYTES)
        {
            return Err(invalid("stop this node and run lastdb db enable-alternate-keys before alternate declarations"));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}
