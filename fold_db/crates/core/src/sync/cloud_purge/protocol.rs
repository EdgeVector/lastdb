//! Account identity stored on a foreign receiver binding.
//!
//! The unused cloud-purge control, upload, download, and inventory types are
//! gone. No caller in this workspace constructed them.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudAccountKind {
    Personal,
    Org,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CloudAccount {
    pub kind: CloudAccountKind,
    pub id: String,
}
