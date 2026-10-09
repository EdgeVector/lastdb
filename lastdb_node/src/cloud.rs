//! Cloud-sync connection for the minimal daemon: join an EXISTING LastDB
//! account as a second device.
//!
//! The canonical-auth model (design-canonical-cloud-auth):
//! - **L0 root**: the account's Ed25519 keypair. The 24-word BIP39 recovery
//!   phrase IS the account — its 32-byte entropy is the Ed25519 seed, and
//!   `E2eKeys::from_ed25519_seed` derives the encryption root from the same
//!   seed. Restoring the phrase here therefore yields byte-identical E2E
//!   keys to the primary node, which is what makes its cloud snapshots and
//!   log entries decryptable on this device.
//! - **L1 session (cache)**: a per-device `api_key`/`session_token` minted by
//!   the signed register call. Losing it is never data loss — it re-mints
//!   from L0 (the auth-refresh callback below).
//! - **L2 intent**: `<home>/cloud_sync.json`. Present = sync on.
//!   Durable pause renames to `cloud_sync.json.paused` (`lastdb cloud off` /
//!   `on`); do not hand-delete credentials.
//!
//! `lastdbd connect` usually performs phrase → seed → `identity.key` →
//! signed register → `cloud_sync.json`. With an invite code on a fresh home, it
//! bootstraps a new account seed first and prints the 24-word recovery phrase.
//!
//! Restore an existing account with `lastdb restore --into` before daemon
//! boot. The factory-wired sync engine then keeps the nodes converging.
//!
//! Scope note: personal-prefix sync only. Org-share sync targets (the desktop
//! node's phase-2 bootstrap) are not configured by the minimal daemon yet.

use std::future::Future;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fold_db::security::Ed25519KeyPair;
use fold_db::sync::auth::SyncAuth;
use fold_db::sync::AuthRefreshCallback;

use crate::host::{self, CLOUD_SYNC_CONFIG_FILE, IDENTITY_KEY_FILE};

mod backup_markers;
mod billing;
mod config;
mod connect;
mod existing_identity;
mod pending_file;
mod register;
mod resume;
mod subscription;

pub use backup_markers::*;
pub use billing::*;
pub use config::*;
pub use connect::*;
pub use existing_identity::*;
use pending_file::*;
pub use register::*;
pub use resume::*;
pub use subscription::*;
