//! Account identity, protected credentials, and explicit backup enrollment.
//! Legacy cloud activation remains unavailable.
use std::path::{Path, PathBuf};

use fold_db::security::Ed25519KeyPair;

use crate::host::{self, CLOUD_SYNC_CONFIG_FILE};

mod config;
mod register;
mod resume;

/// Protected credentials stay paused until an explicit backup request.
pub const CLOUD_SYNC_PAUSED_FILE: &str = "cloud_sync.json.paused";
/// Keep the existing local-write barrier; startup does not replay or clear it.
pub const CLOUD_RESUME_REQUIRED_FILE: &str = ".cloud_resume_required";
pub use config::*;
pub use register::*;
pub use resume::*;
