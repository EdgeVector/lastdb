//! One normal cut/CAS followed by strict stopped-map and durable mirror checks.

use super::{admission, engine, err, io, model, NormalSnapshotArgs};
use crate::home::{open_home_for_offline_read, HomeStore};
use crate::reap::cloud_gate::{self, CloudGateSummary};
use fold_db::storage::laststore::{manifest_sha256_hex, BackupManifest};
use fold_db::storage::traits::NamespacedStore;
use fold_db::sync::engine::{
    decode_offline_pin_log_row, offline_pin_log_restore_frontier_key, OfflinePinLogRow,
    PIN_LOG_NAMESPACE,
};
use std::collections::BTreeMap;

pub(super) async fn run(
    args: &NormalSnapshotArgs,
    inputs: &admission::Inputs,
) -> Result<model::PublicReport, String> {
    let requested_at = chrono::Utc::now().to_rfc3339();
    let before = {
        let opened = open_home_for_offline_read(&inputs.home)?;
        cloud(inputs, &opened).await?
    };
    admission::unchanged(args, inputs, true)?;
    let (auth, s3) = engine::clients(inputs);
    let previous_latest = engine::latest(&auth, &inputs.previous).await?;
    admission::unchanged(args, inputs, true)?;
    io::write(
        &args.report_dir,
        "snapshot-intent.json",
        &model::Intent {
            version: 1,
            requested_at: requested_at.clone(),
            home: &inputs.home,
            store_root: &inputs.store_root,
            expected_pid: args.expected_pid,
            expected_start_ts: args.expected_start_ts,
            expected_build_version: &args.expected_build_version,
            cloud_config_sha256: &args.cloud_config_sha256,
            identity_sha256: &inputs.identity_sha256,
            device_file_sha256: &inputs.device_file_sha256,
            device_id: &inputs.device_id,
            previous_manifest_sha256: &args.previous_manifest_sha256,
            previous_cache_sha256: &inputs.previous_cache_sha256,
            historical_unproved_flush_claim_sha256: &inputs.historical_unproved_flush_claim_sha256,
            operator_evidence_sha256: &args.operator_evidence_sha256,
            operator: &inputs.operator,
            cloud_gate: &before,
            cloud_latest: &previous_latest,
            normal_mode: true,
            background_workers_started: false,
        },
    )?;
    let (raw, opened) = engine::open(inputs)?;
    let writable_gate = cloud(inputs, &opened).await?;
    same_maps(&before, &writable_gate)?;
    admission::unchanged(args, inputs, true)?;
    engine::latest(&auth, &inputs.previous).await?;
    let publisher = engine::construct(args, inputs, raw.clone(), &opened, auth.clone(), s3).await?;
    admission::unchanged(args, inputs, true)?;
    let (manifest, report) = publisher
        .laststore_cloud_snapshot(Some(&inputs.previous))
        .await
        .map_err(err)?;
    // Preserve the actual CAS return even if a subsequent admission check
    // fails. A returned CAS does not grant this tool deletion authority.
    io::write(
        &args.report_dir,
        "snapshot-cas-return.json",
        &serde_json::json!({
            "version": 1, "returned_at": chrono::Utc::now().to_rfc3339(),
            "manifest": manifest, "report": report, "delete_authority": false,
        }),
    )?;
    raw.restore_durability_barrier().await.map_err(err)?;
    admission::unchanged(args, inputs, false)?;
    let after = cloud(inputs, &opened).await?;
    same_maps(&before, &after)?;
    let marker_sha256 = normal_marker(&opened, &before.published_maps["personal"]).await?;
    verify_result(&inputs.previous, &manifest, &report)?;
    durable_mirror(inputs, &manifest, &raw)?;
    let post_cas_latest = engine::latest(&auth, &manifest).await?;
    admission::unchanged(args, inputs, false)?;
    let final_gate = cloud(inputs, &opened).await?;
    same_maps(&after, &final_gate)?;
    normal_marker(&opened, &before.published_maps["personal"]).await?;
    let result = model::ResultReport {
        version: 1,
        requested_at,
        completed_at: chrono::Utc::now().to_rfc3339(),
        cloud_before: before,
        cloud_after: final_gate,
        manifest,
        report,
        post_cas_latest,
        marker_sha256,
        cloud_config_sha256: args.cloud_config_sha256.clone(),
        operator_evidence_sha256: args.operator_evidence_sha256.clone(),
        personal_map_unchanged: true,
        normal_marker_matches: true,
        clean_stop_unchanged: true,
        background_workers_started: false,
    };
    io::write(&args.report_dir, "normal-snapshot-result.json", &result)?;
    Ok(model::PublicReport::from_result(&result))
}

async fn cloud(inputs: &admission::Inputs, opened: &HomeStore) -> Result<CloudGateSummary, String> {
    let gate = cloud_gate::check(&inputs.home, opened).await.map_err(err)?;
    if !gate.complete
        || !gate.cloud_configured
        || gate.cloud_paused
        || gate.personal_pending_rows != 0
        || gate.capture_reexport_keys != 0
        || gate.unreadable_rows != 0
        || gate
            .published_maps
            .get("personal")
            .is_none_or(|map| map.len() != 1 || !map.contains_key(&inputs.device_id))
    {
        return Err(
            "normal snapshot requires complete confirmed current personal cloud metadata".into(),
        );
    }
    Ok(gate)
}

fn same_maps(left: &CloudGateSummary, right: &CloudGateSummary) -> Result<(), String> {
    if left.published_maps != right.published_maps {
        return Err(
            "durable published writer maps changed during stopped normal publication".into(),
        );
    }
    Ok(())
}

async fn normal_marker(
    opened: &HomeStore,
    published: &BTreeMap<String, u64>,
) -> Result<String, String> {
    let store = opened
        .base
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(err)?;
    let key = offline_pin_log_restore_frontier_key();
    let bytes = store
        .get(key)
        .await
        .map_err(err)?
        .ok_or("the genuine normal snapshot marker is absent")?;
    if !matches!(
        decode_offline_pin_log_row(key, &bytes)?,
        OfflinePinLogRow::RestoreFrontier
    ) {
        return Err("the normal snapshot marker has a wrong key kind".into());
    }
    #[derive(serde::Serialize)]
    struct NormalMarker<'a> {
        version: u32,
        by_writer: &'a BTreeMap<String, u64>,
    }
    let expected = serde_json::to_vec(&NormalMarker {
        version: 1,
        by_writer: published,
    })
    .map_err(err)?;
    if bytes != expected {
        return Err(
            "the genuine normal snapshot marker differs from the unchanged stopped writer map"
                .into(),
        );
    }
    Ok(io::digest(&bytes))
}

fn verify_result(
    previous: &BackupManifest,
    manifest: &BackupManifest,
    report: &fold_db::sync::engine::LastStoreCloudSnapshotReport,
) -> Result<(), String> {
    let previous_sha = manifest_sha256_hex(previous).map_err(err)?;
    if manifest.version != 1
        || manifest.store_uuid != previous.store_uuid
        || manifest.epoch != previous.epoch
        || manifest.counter <= previous.counter
        || manifest.previous_manifest_sha256.as_deref() != Some(previous_sha.as_str())
        || report.counter != manifest.counter
        || report.cas_counter != manifest.counter
        || report.cut_csn != manifest.cut_csn
        || report.frontier_through != manifest.cut_csn
        || report.manifest_sha256 != manifest_sha256_hex(manifest).map_err(err)?
        || report.chunks_referenced != manifest.mutable_chunks.len() + manifest.atom_chunks.len()
    {
        return Err(
            "the normal publisher result differs from the exact predecessor or manifest".into(),
        );
    }
    fold_db::storage::laststore::validate_manifest_chain(Some(previous), manifest).map_err(err)
}

fn durable_mirror(
    inputs: &admission::Inputs,
    manifest: &BackupManifest,
    raw: &fold_db::storage::LastStoreNamespacedStore,
) -> Result<(), String> {
    let bytes = io::read(
        &lastdb_node::host::backup_manifest_cache_path(&inputs.home),
        16 * 1024 * 1024,
        false,
    )?;
    let mirrored: BackupManifest = serde_json::from_slice(&bytes).map_err(err)?;
    let durability = raw
        .backup_durability()
        .map_err(err)?
        .ok_or("no durable normal snapshot commit")?;
    if &mirrored != manifest
        || durability.backup_manifest_counter != manifest.counter
        || durability.last_backup_commit_unix_secs.is_none()
        || durability.sealed_base_abandoned_outstanding()
        || raw.cloud_db_hash().map_err(err)?.as_ref() != Some(&inputs.db_hash)
    {
        return Err("the normal CAS mirror or durable backup identity is incomplete".into());
    }
    Ok(())
}
