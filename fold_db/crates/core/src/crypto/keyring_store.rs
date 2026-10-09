//! On-disk lifecycle for the wrapped-DEK [`Keyring`] (`keyring.enc`),
//! the persistence half of Gap G5 (at-rest threat model §5.5/§5.6,
//! `docs/security/at-rest-threat-model.md`).
//!
//! [`super::keyring`] owns the in-memory keyring and the `keyring.enc`
//! *wire format* ([`Keyring::serialize`] / [`Keyring::deserialize`]).
//! This module owns where that file lives (`$FOLDDB_HOME/keyring.enc`)
//! and how it is read and written: an atomic, owner-only persist, a
//! load that distinguishes "no keyring yet" from "corrupt / wrong KEK",
//! an explicit first-run bootstrap, and on-disk KEK rotation.
//!
//! ## Scope of this slice
//!
//! Persistence only. Nothing here resolves the KEK (that is the master
//! key from `secure_store`) or wires the keyring into the `KvStore`
//! seam — that wiring, plus registering the legacy
//! `E2eKeys`-derived key under [`Keyring::LEGACY_KEY_ID`], is Gap G1
//! (`fold-encrypt-main-kv-store`). The KEK is taken as `&[u8; 32]` so
//! this layer stays free of the node's keychain machinery and fully
//! testable on CI.
//!
//! ## No-silent-mint
//!
//! The discipline carried from `secure_store.rs` (`get_master_key` vs
//! `initialize_master_key`) is mirrored here:
//!
//! - [`load`] never mints — an absent file is `Ok(None)`, and a file
//!   that no KEK can unwrap is a loud `Err`, never a fresh keyring.
//! - [`load_or_init`] is the *single* mint point: the only call that
//!   creates DEKs, and only when no `keyring.enc` exists yet.

use super::error::{CryptoError, CryptoResult};
use super::keyring::{KeyPurpose, Keyring};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Canonical filename of the wrapped-DEK keyring inside `$FOLDDB_HOME`.
pub const KEYRING_FILENAME: &str = "keyring.enc";

/// The path `keyring.enc` lives at, given the fold home directory.
pub fn keyring_path(home: &Path) -> PathBuf {
    home.join(KEYRING_FILENAME)
}

/// Load and unwrap the keyring under `kek`.
///
/// Returns:
/// - `Ok(Some(keyring))` — `keyring.enc` exists and unwrapped cleanly.
/// - `Ok(None)` — no `keyring.enc` yet (first run for this home). The
///   caller decides whether to bootstrap via [`load_or_init`]; loading
///   never mints.
/// - `Err(_)` — the file exists but is unreadable, corrupt, tampered, or
///   the `kek` is wrong. Fail loud: a present-but-unopenable keyring is
///   never treated as "no keyring".
pub fn load(home: &Path, kek: &[u8; 32]) -> CryptoResult<Option<Keyring>> {
    let path = keyring_path(home);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(CryptoError::KeyError(format!(
                "reading {}: {e}",
                path.display()
            )))
        }
    };
    Keyring::deserialize(&bytes, kek).map(Some)
}

/// Atomically persist `keyring` to `keyring.enc`, wrapping every DEK
/// under `kek`. Writing under a *different* KEK than the one the keyring
/// was loaded with is exactly a KEK rotation — see [`rewrap`].
///
/// The write is crash-safe (tmpfile in the same directory, fsync, then
/// rename over the destination) so a partially written keyring can never
/// orphan the data it unlocks. On Unix the file is created `0o600`
/// (owner-only): even wrapped, the keyring is sensitive material and gets
/// the same treatment as the rest of `secure_store`'s files.
pub fn persist(home: &Path, keyring: &Keyring, kek: &[u8; 32]) -> CryptoResult<()> {
    let bytes = keyring.serialize(kek)?;
    let path = keyring_path(home);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| CryptoError::KeyError(format!("creating {}: {e}", parent.display())))?;
    }
    let dir = path.parent().unwrap_or_else(|| Path::new("."));

    let mut tmp = tempfile::Builder::new()
        .prefix(".keyring.enc.")
        .tempfile_in(dir)
        .map_err(|e| {
            CryptoError::KeyError(format!("creating temp keyring in {}: {e}", dir.display()))
        })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tmp.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|e| CryptoError::KeyError(format!("setting keyring perms: {e}")))?;
    }

    tmp.write_all(&bytes)
        .map_err(|e| CryptoError::KeyError(format!("writing temp keyring: {e}")))?;
    tmp.as_file()
        .sync_all()
        .map_err(|e| CryptoError::KeyError(format!("fsync temp keyring: {e}")))?;
    tmp.persist(&path)
        .map_err(|e| CryptoError::KeyError(format!("renaming keyring into place: {}", e.error)))?;

    Ok(())
}

/// Load the keyring, or bootstrap a fresh one on first run.
///
/// This is the **single mint point** for the keyring (no-silent-mint).
/// If `keyring.enc` exists it is loaded and returned unchanged
/// (`created = false`) — even if it is missing some of `purposes`;
/// minting an additional purpose into an existing keyring is a separate
/// explicit step, not a side effect of loading. If it does *not* exist,
/// a fresh keyring is created with one active DEK minted per requested
/// purpose, persisted, and returned (`created = true`).
///
/// Pass [`KeyPurpose::ALL`] for the full per-purpose hierarchy
/// (store / index / blob / identity).
///
/// The returned `bool` is `true` exactly when a new keyring was minted,
/// so the caller can, e.g., surface a one-time recovery code at init.
pub fn load_or_init(
    home: &Path,
    kek: &[u8; 32],
    purposes: &[KeyPurpose],
) -> CryptoResult<(Keyring, bool)> {
    if let Some(existing) = load(home, kek)? {
        return Ok((existing, false));
    }
    let mut keyring = Keyring::new();
    for &purpose in purposes {
        keyring.mint_dek(purpose);
    }
    persist(home, &keyring, kek)?;
    Ok((keyring, true))
}

/// Rotate the KEK: re-wrap the on-disk keyring from `old_kek` to
/// `new_kek`. Cheap by construction — only the handful of wrapped DEKs
/// are re-sealed; no data is re-encrypted (the DEKs, and therefore every
/// envelope sealed under them, are untouched).
///
/// Errors loudly if there is no keyring to rotate (`Ok(None)` from
/// [`load`] becomes an error here) or if `old_kek` cannot unwrap it —
/// rotation must never silently create a new keyring.
pub fn rewrap(home: &Path, old_kek: &[u8; 32], new_kek: &[u8; 32]) -> CryptoResult<()> {
    let keyring = load(home, old_kek)?.ok_or_else(|| {
        CryptoError::KeyError(format!(
            "no {KEYRING_FILENAME} to rotate at {}",
            home.display()
        ))
    })?;
    persist(home, &keyring, new_kek)
}
