//! Passive retirement compatibility. No old capture, upload, restore or scheduler exists.
mod model;
mod store;
mod v2_cleanup;

use crate::backup_runtime::{BackupError, Result};
pub use model::{NativeBackupCaptureRequest, Phase, Saved as LegacyBackupRecord};
pub(crate) use v2_cleanup::require_complete as require_v1_complete;
