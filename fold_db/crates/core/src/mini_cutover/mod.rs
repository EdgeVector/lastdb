//! Mini cutover helpers (Sled → Last Store). Always compiled; no cloud-sync gate.
//!
//! Main-key classification and plane breakdown live here so unit tests run under default
//! `fold_db` features (`cargo test -p fold_db migrate`).

pub mod main_keys;
pub mod plane_roles;

pub use main_keys::{
    classify_main_key, LEGACY_MAIN_MIGRATION_COLLECTIONS, MAIN_MIGRATION_COLLECTIONS,
};
pub use plane_roles::{
    classify_collection_plane, dual_read_hits_by_plane, plane_breakdown_for_store_root,
    plane_status_lines, CollectionPlaneEntry, CollectionPlaneRole, DualReadPlaneHit,
    PlaneBreakdown, PlaneMapCollection, PlaneMapReport, PlaneMapRoleTotal, SOT_NAMED_COLLECTIONS,
};
