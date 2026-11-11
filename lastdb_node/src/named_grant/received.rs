//! Accepted named-database keys. This is not the publication share cache.
//!
//! The stored role is the invitation bearer, which is always `read`.
//! Submit authority is the member list. `access: reader` is not a live
//! member-list check and goes stale after revoke.

use super::files;
use super::invite::open_content_key_invitation;
use crate::registered_share;
use crate::share_invitation::ShareRecipient;
use crate::share_publication::MARKER_FILE;
use cloud_backup_protocol::Digest;
use fold_db::security::Ed25519KeyPair;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use zeroize::Zeroizing;

const DIRECTORY: &str = "share-received";
const KEY_FILE: &str = "content.key";
const REGISTRATION_FILE: &str = "registration.json";
const REGISTRATION_LIMIT: u64 = 4096;

/// Why a named-database key was not stored. The message is static.
#[derive(Debug)]
pub enum AcceptError {
    /// The seal, name, or home cannot be accepted.
    Invalid(&'static str),
    /// The id or name is already in use, or the stored key differs.
    Conflict(&'static str),
    /// The registry or a received registration cannot be read.
    Unavailable(&'static str),
}

impl AcceptError {
    /// HTTP status for this failure.
    pub fn status(&self) -> u16 {
        match self {
            Self::Invalid(_) => 400,
            Self::Conflict(_) => 409,
            Self::Unavailable(_) => 503,
        }
    }

    /// Static message. It contains no key material.
    pub fn message(&self) -> &'static str {
        match self {
            Self::Invalid(message) | Self::Conflict(message) | Self::Unavailable(message) => {
                message
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    database_id: String,
    publisher: Digest,
    name: Option<String>,
    role: String,
}

/// Store the opened AES key. Does not write the registry or the share cache.
pub fn accept_content_key(
    home: &Path,
    signer: &Ed25519KeyPair,
    sealed: &[u8],
    publisher: &Digest,
    name: Option<&str>,
) -> Result<Value, AcceptError> {
    refuse_publication(home)?;
    if let Some(name) = name {
        require_name(name)?;
    }
    let opened = open_sealed(signer, sealed, publisher)?;
    let database_id = opened.database_id;
    collide(home, &database_id, name)?;
    store(home, &database_id, publisher, name, &opened.key)
}

/// Rows for `GET /api/databases`. Incomplete directories are omitted.
pub fn received_rows(home: &Path) -> Result<Vec<Value>, ()> {
    let root = home.join(DIRECTORY);
    if !received_root(&root)? {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    for entry in fs::read_dir(&root).map_err(|_| ())? {
        let entry = entry.map_err(|_| ())?;
        let Some(id) = entry_id(&entry)? else {
            continue;
        };
        match read_registration(&entry.path()) {
            Ok(Some(registration)) if registration.database_id == id => {
                rows.push(row(&registration));
            }
            Ok(Some(_)) | Err(_) => return Err(()),
            Ok(None) => {}
        }
    }
    Ok(rows)
}

/// True when a received registration already uses `name`.
pub fn received_name_taken(home: &Path, name: &str) -> Result<bool, ()> {
    Ok(received_rows(home)?
        .iter()
        .any(|value| value.get("name").and_then(Value::as_str) == Some(name)))
}

struct OpenedKey {
    database_id: String,
    key: Zeroizing<[u8; 32]>,
}

fn open_sealed(
    signer: &Ed25519KeyPair,
    sealed: &[u8],
    publisher: &Digest,
) -> Result<OpenedKey, AcceptError> {
    let seed = Zeroizing::new(signer.secret_key_bytes());
    let recipient = ShareRecipient::from_identity_seed(seed.as_ref())
        .map_err(|_| AcceptError::Invalid("invalid sealed share invitation"))?;
    let opened = open_content_key_invitation(&recipient, sealed, publisher)
        .map_err(|_| AcceptError::Invalid("invalid sealed share invitation"))?;
    let database_id = opened.permission.stream().stream_id.as_str().to_owned();
    if cloud_backup_protocol::Id::try_from(database_id.clone()).is_err() {
        return Err(AcceptError::Invalid("invalid sealed share invitation"));
    }
    Ok(OpenedKey {
        database_id,
        key: opened.publication_key,
    })
}

fn store(
    home: &Path,
    database_id: &str,
    publisher: &Digest,
    name: Option<&str>,
    key: &Zeroizing<[u8; 32]>,
) -> Result<Value, AcceptError> {
    let dir = home.join(DIRECTORY).join(database_id);
    if let Some(existing) = read_registration(&dir)? {
        if existing.database_id != database_id {
            return Err(AcceptError::Unavailable(
                "database registration is unavailable",
            ));
        }
        return finish_existing(&dir, &existing, publisher, name, key);
    }
    if let Some(stored) = read_key(&dir)? {
        if stored.as_ref() != key.as_ref() {
            return Err(AcceptError::Conflict("database name is already registered"));
        }
    }
    write_key(&dir, key)?;
    let registration = Registration {
        database_id: database_id.to_owned(),
        publisher: publisher.clone(),
        name: name.map(str::to_owned),
        role: "read".to_owned(),
    };
    write_registration(&dir, &registration)?;
    Ok(row(&registration))
}

fn finish_existing(
    dir: &Path,
    existing: &Registration,
    publisher: &Digest,
    name: Option<&str>,
    key: &Zeroizing<[u8; 32]>,
) -> Result<Value, AcceptError> {
    if &existing.publisher != publisher || existing.name.as_deref() != name {
        return Err(AcceptError::Conflict("database name is already registered"));
    }
    match read_key(dir)? {
        Some(stored) if stored.as_ref() == key.as_ref() => Ok(row(existing)),
        Some(_) => Err(AcceptError::Conflict("database name is already registered")),
        None => {
            write_key(dir, key)?;
            Ok(row(existing))
        }
    }
}

fn collide(home: &Path, database_id: &str, name: Option<&str>) -> Result<(), AcceptError> {
    match crate::host::owned::registry_claims(home, database_id, name) {
        Ok(true) => return Err(AcceptError::Conflict("database name is already registered")),
        Ok(false) => {}
        Err(()) => return Err(AcceptError::Unavailable("database registry is unavailable")),
    }
    let shares = registered_share::list(home)
        .map_err(|_| AcceptError::Unavailable("database registry is unavailable"))?;
    let share_taken = shares.iter().any(|share| {
        share.id.as_str() == database_id
            || name.is_some_and(|name| share.name.as_deref() == Some(name))
    });
    if share_taken || other_received_name(home, database_id, name)? {
        return Err(AcceptError::Conflict("database name is already registered"));
    }
    Ok(())
}

fn other_received_name(
    home: &Path,
    database_id: &str,
    name: Option<&str>,
) -> Result<bool, AcceptError> {
    let Some(name) = name else {
        return Ok(false);
    };
    let rows = received_rows(home)
        .map_err(|_| AcceptError::Unavailable("database registration is unavailable"))?;
    Ok(rows.iter().any(|value| {
        value.get("name").and_then(Value::as_str) == Some(name)
            && value.get("id").and_then(Value::as_str) != Some(database_id)
    }))
}

fn refuse_publication(home: &Path) -> Result<(), AcceptError> {
    match fs::symlink_metadata(home.join(MARKER_FILE)) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(AcceptError::Invalid("invalid sealed share invitation"))
        }
        Ok(_) => Err(AcceptError::Invalid(
            "a publication database cannot accept a named database key",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(AcceptError::Unavailable(
            "database registration is unavailable",
        )),
    }
}

fn require_name(name: &str) -> Result<(), AcceptError> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        && !name.eq_ignore_ascii_case("personal")
        && uuid::Uuid::parse_str(name).is_err();
    if valid {
        Ok(())
    } else {
        Err(AcceptError::Invalid("invalid database name"))
    }
}

fn received_root(root: &Path) -> Result<bool, ()> {
    match fs::symlink_metadata(root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(true),
        _ => Err(()),
    }
}

fn entry_id(entry: &fs::DirEntry) -> Result<Option<String>, ()> {
    let name = entry.file_name();
    let Some(name) = name.to_str() else {
        return Err(());
    };
    if name.starts_with('.') {
        return Ok(None);
    }
    let metadata = fs::symlink_metadata(entry.path()).map_err(|_| ())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(());
    }
    if cloud_backup_protocol::Id::try_from(name.to_owned()).is_err() {
        return Err(());
    }
    Ok(Some(name.to_owned()))
}

fn read_registration(dir: &Path) -> Result<Option<Registration>, AcceptError> {
    let bytes = files::read_at(&dir.join(REGISTRATION_FILE), REGISTRATION_LIMIT)
        .map_err(|_| AcceptError::Unavailable("database registration is unavailable"))?;
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let registration: Registration = serde_json::from_slice(&bytes)
        .map_err(|_| AcceptError::Unavailable("database registration is unavailable"))?;
    if registration.role != "read" {
        return Err(AcceptError::Unavailable(
            "database registration is unavailable",
        ));
    }
    Ok(Some(registration))
}

fn read_key(dir: &Path) -> Result<Option<Zeroizing<[u8; 32]>>, AcceptError> {
    let bytes = files::read_at(&dir.join(KEY_FILE), 32)
        .map_err(|_| AcceptError::Unavailable("database registration is unavailable"))?;
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let key: [u8; 32] = bytes
        .try_into()
        .map_err(|_| AcceptError::Unavailable("database registration is unavailable"))?;
    Ok(Some(Zeroizing::new(key)))
}

fn write_key(dir: &Path, key: &[u8; 32]) -> Result<(), AcceptError> {
    files::replace(dir, KEY_FILE, key)
        .map_err(|_| AcceptError::Unavailable("database registration is unavailable"))
}

fn write_registration(dir: &Path, registration: &Registration) -> Result<(), AcceptError> {
    let bytes = serde_json::to_vec(registration)
        .map_err(|_| AcceptError::Unavailable("database registration is unavailable"))?;
    files::replace(dir, REGISTRATION_FILE, &bytes)
        .map_err(|_| AcceptError::Unavailable("database registration is unavailable"))
}

fn row(registration: &Registration) -> Value {
    json!({
        "id": registration.database_id,
        "publisher": registration.publisher,
        "name": registration.name,
        "snapshot": null,
        "state": "restoring",
        "access": "reader",
        "kind": "received",
    })
}
