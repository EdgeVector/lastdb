//! Account identity, protected credentials, and explicit backup enrollment.
//! Legacy cloud activation remains unavailable.
use std::path::{Path, PathBuf};

use fold_db::security::Ed25519KeyPair;

use crate::host::{self, CLOUD_SYNC_CONFIG_FILE};

mod backup_markers;
mod config;
mod register;
mod resume;

pub use backup_markers::*;
pub use config::*;
pub use register::*;
pub use resume::*;
