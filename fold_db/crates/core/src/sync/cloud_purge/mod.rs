//! Account cloud replacement from one retained local source.

pub(crate) mod blob_copy;
mod checkpoint;
mod download;
mod file_verification;
mod files;
pub mod inventory;
mod job;
pub(crate) mod memory;
pub mod org_export;
mod org_files;
mod org_key_recode;
mod org_proteins;
mod org_rekey;
pub(crate) mod org_rows;
mod preflight;
pub mod protocol;
mod restore;
pub(crate) mod row_copy;
mod source;
mod source_roots;
mod source_verify;
mod upload;
mod upload_transport;

pub use checkpoint::{CloudPurgeJob, CloudPurgeJobPhase};
pub use download::download_replacement_objects;
pub use job::{CloudPurgeCapabilityStore, CloudPurgeDriver};
pub use restore::{
    restore_cloud_purge_root, restore_cloud_purge_root_with_report, CloudPurgeRootRestore,
};
pub use source::{CloudSourceCut, RetainedSource, RetainedSourceFile};
pub use source_roots::RetainedShareSnapshot;
pub type CloudPurgeCapability = zeroize::Zeroizing<String>;
