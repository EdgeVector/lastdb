//! Account identity, protected credentials, and explicit backup enrollment.
//! Legacy cloud activation remains unavailable.
use std::path::{Path, PathBuf};

use fold_db::security::Ed25519KeyPair;

use crate::host::{self, CLOUD_SYNC_CONFIG_FILE};

fn require_cloud_activation_supported() -> Result<(), String> {
    Err(fold_db::FoldDB::CLOUD_SYNC_DISABLED.into())
}

mod backup_markers;
mod config;
mod register;
mod resume;

pub use backup_markers::*;
pub use config::*;
pub use register::*;
pub use resume::*;
