//! Exact durable receipt admission occurs before a cloud hold.

use super::inventory::{
    bounded_json, canonical_inventory, CloudInventoryRef, INVENTORY_METADATA_MAX_BYTES,
    INVENTORY_PAGE_MAX_BYTES,
};
use super::protocol::{
    CloudPurgeControl, CloudPurgePhase, CloudPurgePreparation, CloudReplacementManifest,
    CloudSelectedWriter,
};
use super::{CloudPurgeJob, CloudPurgeJobPhase, RetainedSource};
use crate::sync::{SyncError, SyncResult};
use serde_json::{json, Value};
use std::collections::BTreeSet;

const PEER_MANIFEST_MAX_BYTES: usize = 31 * 1024 * 1024;
const RECEIPT_ENVELOPE_RESERVE_BYTES: usize = 1024 * 1024;

impl CloudPurgePreparation {
    pub fn validate_inventory_capabilities(&self) -> SyncResult<()> {
        if self.capabilities["purge_resync"].as_bool() != Some(true)
            || self.capabilities["erase_first"].as_bool() != Some(true)
            || self.capabilities["inventory_metadata_max_bytes"].as_u64()
                != Some(INVENTORY_METADATA_MAX_BYTES as u64)
            || self.capabilities["inventory_page_max_bytes"].as_u64()
                != Some(INVENTORY_PAGE_MAX_BYTES as u64)
            || self.capabilities["scope_metadata_max_bytes"].as_u64() != Some(160_000)
        {
            return Err(SyncError::Storage(
                "cloud purge readiness or metadata budgets are unsupported before source cut"
                    .into(),
            ));
        }
        Ok(())
    }
}

pub(super) fn admit_job(
    preparation: &CloudPurgePreparation,
    job: &CloudPurgeJob,
    source: &RetainedSource,
) -> SyncResult<()> {
    preparation.validate_inventory_capabilities()?;
    if preparation.account != job.account || preparation.scope_version != job.expected_scope_version
    {
        return Err(SyncError::Storage(
            "cloud account scope changed before hold".into(),
        ));
    }
    bounded_json(source, INVENTORY_METADATA_MAX_BYTES)?;
    canonical_inventory(&source.prepared, INVENTORY_METADATA_MAX_BYTES)?;
    let (scope_manifest, removed_roots) = scoped_preview(preparation, source)?;
    let replacement = CloudReplacementManifest {
        format_version: 1,
        account: job.account.clone(),
        operation_id: job.operation_id.clone(),
        generation: u64::MAX,
        source_manifest_sha256: job.source_manifest_sha256.clone(),
        roots: source.prepared.roots.clone(),
        removed_roots,
    };
    let replacement_bytes = canonical_inventory(&replacement, PEER_MANIFEST_MAX_BYTES)?;
    let sha256 = crate::hex::sha256_hex(&replacement_bytes);
    let reference = CloudInventoryRef {
        key: super::inventory::inventory_key(
            &job.account,
            &job.operation_id,
            "replacement",
            &sha256,
        ),
        sha256,
        bytes: replacement_bytes.len() as u64,
    };
    let control = CloudPurgeControl {
        format_version: 1,
        account: job.account.clone(),
        operation_id: job.operation_id.clone(),
        generation: u64::MAX,
        revision: u64::MAX,
        phase: CloudPurgePhase::Ready,
        selected_writer: CloudSelectedWriter {
            principal_hash: "0".repeat(256),
            source_id: job.source_id.clone(),
        },
        scope_manifest,
        replacement_manifest: Some(serde_json::to_value(&replacement)?),
        replacement_manifest_ref: Some(reference),
        deletion_receipt: Some(json!({"complete":true,
            "providers":["r2","b2","connection_messages"],"versions_deleted":true,
            "multipart_aborted":true,"completed_at":u64::MAX,"operation_id":job.operation_id,"generation":u64::MAX})),
        last_error: Some(json!("0".repeat(16_384))),
        data_generation: Some(u64::MAX),
        continuation: None,
        continuation_sha256: None,
        previous_ready_control: None,
        last_ready_generation: Some(u64::MAX),
    };
    let mut ready = job.clone();
    ready.phase = CloudPurgeJobPhase::Ready;
    ready.control = Some(control);
    ready.replacement = Some(replacement);
    ready.capability_locator = Some(format!(
        "lastsecrets://lastdb-cloud-purge-{}",
        job.operation_id
    ));
    ready.uploaded_objects = source
        .prepared
        .objects
        .iter()
        .map(super::checkpoint::object_identity)
        .collect();
    ready.updated_at_unix_secs = u64::MAX;
    ready.last_error = Some("0".repeat(16_384));
    bounded_json(
        &ready,
        INVENTORY_METADATA_MAX_BYTES - RECEIPT_ENVELOPE_RESERVE_BYTES,
    )?;
    admit_prior_continuation(preparation, &ready)
}

fn scoped_preview(
    preparation: &CloudPurgePreparation,
    source: &RetainedSource,
) -> SyncResult<(Value, Vec<String>)> {
    let source_roots: BTreeSet<_> = source
        .prepared
        .roots
        .iter()
        .map(|root| root.logical_root.clone())
        .collect();
    let mut scope = preparation.scope_manifest.clone();
    if !scope.is_object() {
        return Err(SyncError::Storage(
            "cloud account scope preview absent before hold".into(),
        ));
    }
    let removed: Vec<_> = scope["roots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|root| root["root"].as_str())
        .filter(|root| {
            !source_roots.contains(*root)
                && !root.starts_with("bulletin/")
                && !root.starts_with("__lastdb_refs/")
        })
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    scope["source_roots"] = json!(source_roots);
    scope["removed_roots"] = json!(removed);
    canonical_inventory(&scope, 160_000)?;
    Ok((scope, removed))
}

fn admit_prior_continuation(
    preparation: &CloudPurgePreparation,
    ready: &CloudPurgeJob,
) -> SyncResult<()> {
    let Some(old) = preparation.control.as_ref() else {
        return Ok(());
    };
    let previous = if old["phase"].as_str() == Some("ready") {
        old.clone()
    } else {
        old["previous_ready_control"].clone()
    };
    if previous.is_null() {
        return Ok(());
    }
    if let Some(manifest) = previous
        .get("replacement_manifest")
        .filter(|value| !value.is_null())
    {
        canonical_inventory(manifest, PEER_MANIFEST_MAX_BYTES)?;
    }
    let mut cancelled = ready.clone();
    cancelled.replacement = None;
    cancelled.uploaded_objects.clear();
    if let Some(control) = cancelled.control.as_mut() {
        control.phase = CloudPurgePhase::Cancelled;
        control.replacement_manifest = None;
        control.replacement_manifest_ref = None;
        control.previous_ready_control = Some(previous);
        control.continuation = Some(json!("0".repeat(16_384)));
        control.continuation_sha256 = Some("0".repeat(64));
    }
    bounded_json(
        &cancelled,
        INVENTORY_METADATA_MAX_BYTES - RECEIPT_ENVELOPE_RESERVE_BYTES,
    )?;
    Ok(())
}
