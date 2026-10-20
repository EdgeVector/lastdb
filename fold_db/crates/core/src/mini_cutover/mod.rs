//! Mini cutover helpers (Sled → Last Store). Always compiled; no cloud-sync gate.
//!
//! Main-key classification and plane breakdown live here so they build under default
//! `fold_db` features.

pub mod main_keys;
pub mod plane_roles;

pub use main_keys::classify_main_key;
pub use plane_roles::{
    classify_collection_plane, plane_breakdown_for_store_root, plane_status_lines,
    CollectionPlaneEntry, CollectionPlaneRole, PlaneBreakdown, PlaneMapCollection, PlaneMapReport,
    PlaneMapRoleTotal, SOT_NAMED_COLLECTIONS,
};
