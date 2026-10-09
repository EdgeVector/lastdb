//! Cross-user sharing and consent-gated delivery support.
//!
//! This module is compiled only with the `sharing` feature. Default local
//! kernel builds omit it; product hosts opt in explicitly.

pub mod blob_cas;
pub mod delivery_wire;
pub mod file_blob_seal;
pub mod org_sync_target;
pub mod query_slice;
pub mod signing;
pub mod store;
pub mod types;

pub use org_sync_target::{
    deactivate_org_sync_target, deactivate_org_sync_target_in_ops,
    deactivate_org_sync_targets_matching_in_ops, e2e_key_bytes, list_active_org_sync_targets,
    list_active_org_sync_targets_in_ops, list_org_sync_targets, list_org_sync_targets_in_ops,
    repair_org_sync_targets, repair_org_sync_targets_in_ops, upsert_org_sync_target,
    upsert_org_sync_target_for_storage_prefix,
    upsert_org_sync_target_for_storage_prefix_and_schema,
    upsert_org_sync_target_for_storage_prefix_and_schema_in_ops,
    upsert_org_sync_target_for_storage_prefix_in_ops, upsert_org_sync_target_in_ops, OrgSyncTarget,
};
