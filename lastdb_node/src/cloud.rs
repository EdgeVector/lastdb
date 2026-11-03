//! Account identity, the saved credentials loader, and explicit backup enrollment.
use std::path::{Path, PathBuf};

use fold_db::security::Ed25519KeyPair;

mod config;
mod credentials;
mod register;

/// The saved account credentials and, after an owner connect, the manual
/// backup consent: one file, replaced atomically. No cloud work starts from
/// it alone.
pub const CLOUD_CREDENTIALS_FILE: &str = "cloud-credentials.json";
pub use config::*;
pub use credentials::*;
pub use register::*;
