//! Local shared-surface attachment store for Mini.
//!
//! Attachments map a private local schema id to a shared schema hash after
//! an explicit publish/attach. Persistence is local only
//! (`{home}/shared_surface_attachments.json`); the cloud service does not
//! store these records in this PR.
//!
//! Private declare routes never call this module.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use schema_service_client::SharedSurfaceAttachmentRecord;
use serde::{Deserialize, Serialize};

/// File name under the node home.
pub const ATTACHMENTS_FILE: &str = "shared_surface_attachments.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedSurfaceAttachmentStore {
    pub attachments: Vec<SharedSurfaceAttachmentRecord>,
}

impl SharedSurfaceAttachmentStore {
    pub fn path_for_home(home: &Path) -> PathBuf {
        home.join(ATTACHMENTS_FILE)
    }

    pub fn load(path: &Path) -> io::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes = fs::read(path)?;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(Self::default());
        }
        serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub fn save_atomic(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::write(&tmp, &bytes)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Upsert by `local_schema_id` (last write wins).
    pub fn upsert(&mut self, record: SharedSurfaceAttachmentRecord) {
        if let Some(existing) = self
            .attachments
            .iter_mut()
            .find(|a| a.local_schema_id == record.local_schema_id)
        {
            *existing = record;
        } else {
            self.attachments.push(record);
        }
    }

    pub fn list(&self) -> &[SharedSurfaceAttachmentRecord] {
        &self.attachments
    }
}

/// Validate publish-attach request body shape before facade work.
///
/// Returns a human-readable error string on failure.
pub fn validate_publish_body(
    local_schema_id: &str,
    descriptive_name: &str,
    fields: &[String],
) -> Result<(), String> {
    if local_schema_id.trim().is_empty() {
        return Err("local_schema_id must be non-empty".into());
    }
    if descriptive_name.trim().is_empty() {
        return Err("descriptive_name must be non-empty".into());
    }
    if fields.is_empty() || fields.iter().any(|f| f.trim().is_empty()) {
        return Err("fields must be non-empty without blank names".into());
    }
    Ok(())
}
