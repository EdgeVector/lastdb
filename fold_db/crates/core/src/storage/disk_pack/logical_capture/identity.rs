//! A device signer is local identity, outside the native backup file allowlist.

use super::*;
use ed25519_dalek::{Signer, SigningKey};
pub(super) use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Read, Write};
pub(super) use std::io::{Seek, SeekFrom};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use zeroize::Zeroizing;

pub(super) const FILE: &str = "logical-device-capture-v1";
const MARKER: &[u8] = b"lastdb-logical-device-capture-v1\n";
const BODY_BYTES: usize = 81;

#[path = "identity_prepared.rs"]
mod prepared;

pub(super) struct Identity {
    seed: Zeroizing<[u8; 32]>,
    generation: uuid::Uuid,
    main: uuid::Uuid,
    cas: uuid::Uuid,
    pub(super) ready: bool,
}

impl Identity {
    pub(super) fn load(v2: &physical::V2Home) -> StorageResult<Option<Self>> {
        let path = control_path(v2)?;
        Self::load_at(&path, v2.home.identity(), v2.cas.identity())
    }

    pub(super) fn load_source(
        data: &Path,
        main: uuid::Uuid,
        cas: uuid::Uuid,
    ) -> StorageResult<Self> {
        let identity = Self::load_at(&data.join(FILE), main, cas)?
            .ok_or_else(|| invalid("snapshot identity source signer is absent"))?;
        if !identity.ready {
            return Err(invalid("snapshot identity source signer is not ready"));
        }
        Ok(identity)
    }

    fn load_at(path: &Path, main: uuid::Uuid, cas: uuid::Uuid) -> StorageResult<Option<Self>> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let meta = file.metadata()?;
        let expected = MARKER.len() + BODY_BYTES;
        if !private_owned_file(&meta) || meta.len() != expected as u64 {
            return Err(invalid(
                "logical capture control must be an exact private owned file",
            ));
        }
        let mut bytes = Zeroizing::new(Vec::new());
        Read::by_ref(&mut file)
            .take(expected as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() != expected || !bytes.starts_with(MARKER) {
            return Err(invalid("logical capture control format differs"));
        }
        let body = &bytes[MARKER.len()..];
        let identity = Self {
            main: uuid(&body[..16])?,
            cas: uuid(&body[16..32])?,
            seed: Zeroizing::new(
                body[32..64]
                    .try_into()
                    .map_err(|_| invalid("logical capture signer length differs"))?,
            ),
            generation: uuid(&body[64..80])?,
            ready: match body[80] {
                0 => false,
                1 => true,
                _ => return Err(invalid("logical capture enrollment state differs")),
            },
        };
        if identity.main != main || identity.cas != cas {
            return Err(invalid(
                "logical capture signer belongs to different native homes",
            ));
        }
        Ok(Some(identity))
    }

    pub(super) fn birth_homes(&self) -> (uuid::Uuid, uuid::Uuid) {
        (self.main, self.cas)
    }

    pub(super) fn origin(&self) -> String {
        let public = SigningKey::from_bytes(&self.seed).verifying_key();
        format!("{}:{}", hex(public.as_bytes()), self.generation)
    }

    pub(super) fn sign(&self, bytes: &[u8]) -> String {
        hex(&SigningKey::from_bytes(&self.seed).sign(bytes).to_bytes())
    }
}

pub(super) fn anchor_for(origin: &str, main: uuid::Uuid, cas: uuid::Uuid) -> StorageResult<String> {
    use sha2::{Digest, Sha256};
    Ok(hex(&Sha256::digest(encode(&(
        "lastdb-logical-capture-anchor-v1",
        origin,
        main,
        cas,
    ))?)))
}

/// Write one private stage, then rename it onto a path that is still absent.
/// A failed install discards the stage and returns the first error.
fn install_private_file(
    path: &Path,
    staged: &Path,
    bytes: &[u8],
    discard: impl Fn(&Path) -> StorageResult<()>,
    sync_dir: impl FnOnce() -> StorageResult<()>,
) -> StorageResult<()> {
    require_absent(path)?;
    discard(staged)?;
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(staged)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        require_absent(path)?;
        std::fs::rename(staged, path)?;
        sync_dir()
    })();
    if result.is_err() {
        let _ = discard(staged);
    }
    result
}

fn require_absent(path: &Path) -> StorageResult<()> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => Err(invalid("logical capture control already exists")),
    }
}

/// A capture file is regular, mode 0600, one link, and owned by this user.
fn private_owned_file(meta: &std::fs::Metadata) -> bool {
    meta.is_file()
        && meta.mode() & 0o777 == 0o600
        && meta.nlink() == 1
        && meta.uid() == unsafe { libc::geteuid() }
}

fn stage_metadata(path: &Path) -> StorageResult<Option<std::fs::Metadata>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => Ok(Some(meta)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Discard one absent or private stage. No directory walk occurs.
fn discard_private_stage(path: &Path, max_len: u64, message: &str) -> StorageResult<()> {
    let Some(meta) = stage_metadata(path)? else {
        return Ok(());
    };
    if !private_owned_file(&meta) || meta.len() > max_len {
        return Err(invalid(message));
    }
    std::fs::remove_file(path)?;
    Ok(())
}

fn uuid(bytes: &[u8]) -> StorageResult<uuid::Uuid> {
    uuid::Uuid::from_slice(bytes)
        .ok()
        .filter(|id| !id.is_nil())
        .ok_or_else(|| invalid("logical capture control UUID is invalid"))
}

fn control_path(v2: &physical::V2Home) -> StorageResult<std::path::PathBuf> {
    Ok(v2
        .home
        .path()
        .parent()
        .ok_or_else(|| invalid("native home needs a parent"))?
        .join(FILE))
}
