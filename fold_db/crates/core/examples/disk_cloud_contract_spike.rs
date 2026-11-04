//! Disk-to-cloud contract spike.
//!
//! CI creates a synthetic format 12 home, pins the frozen run capture, and
//! derives a cloud plan from the disk export spec. Placement changes the plan.
//! It does not change the pinned bytes. This command does not upload, and it
//! does not open a real LastDB home.

use std::collections::BTreeMap;
use std::sync::Arc;

use fold_db::db_operations::DbOperations;
use fold_db::storage::cloud_contract::{
    derive_cloud_format, export_part_ids, plan, ArtifactDescriptor, CloudEnvelope, CloudFormat,
    CloudManifest, CloudObject, Destination, ExportPresence, ExportRole, PartReport,
    PlacementPolicy, BACKUP_PROFILE, CLOUD_PROTOCOL,
};
use fold_db::storage::disk_pack::{
    pin_disk_export, PackNamespacedStore, PinnedDiskExport, CAS_COLLECTION, HOME_FORMAT,
};
use fold_db::storage::traits::{KvStore, NamespacedStore};
use sha2::Digest as _;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const NODE_KEY: [u8; 32] = [0x11; 32];

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var_os("GITHUB_ACTIONS").is_none() {
        eprintln!("disk_cloud_contract_spike runs only in CI (GITHUB_ACTIONS is not set)");
        std::process::exit(2);
    }
    run().await
}

async fn run() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let node = dir.path().join("node");
    let hold = dir.path().join("hold");
    let store = PackNamespacedStore::create_run(&node, &NODE_KEY)?;
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    DbOperations::from_namespaced_store(Arc::clone(&shared)).await?;
    write_generated(&shared).await?;
    let pin = tokio::task::block_in_place(|| pin_disk_export(&store, &hold))?;
    let records_on_r2 = derive_cloud_format(envelope(BACKUP_PROFILE), records_on_r2())?;
    let records_on_b2 = derive_cloud_format(envelope(BACKUP_PROFILE), records_on_b2())?;
    let profile_one = derive_cloud_format(envelope(1), records_on_r2())?;
    let selected = [ExportRole::Snapshot, ExportRole::RecordOwnedBlob];
    let plan_r2 = plan(pin.descriptors(), &records_on_r2, &selected)?;
    let plan_b2 = plan(pin.descriptors(), &records_on_b2, &selected)?;
    check_derived(&records_on_r2, &profile_one)?;
    check_capture(&plan_r2)?;
    check_placement(&plan_r2, &plan_b2)?;
    check_bytes(&pin, &plan_r2, &plan_b2)?;
    check_refusals(pin.descriptors(), &records_on_r2)?;
    check_snapshot_only(pin.descriptors(), &records_on_r2)?;
    println!("PASS: pinned frozen run capture");
    println!("PASS: placement change keeps source bytes");
    println!("PASS: replay gap refused");
    println!("PASS: unsupported disk format refused");
    println!("PASS: unclassified part refused");
    println!("PASS: blob-only closure refused");
    println!("PASS: backup profile does not describe the disk format");
    println!("{}", summary(&plan_r2, &plan_b2));
    Ok(())
}

fn envelope(backup_profile: u16) -> CloudEnvelope {
    CloudEnvelope {
        protocol: CLOUD_PROTOCOL,
        backup_profile,
    }
}

fn records_on_r2() -> PlacementPolicy {
    PlacementPolicy {
        snapshot: Destination::R2,
        replay_log: Destination::R2,
        record_owned_blob: Destination::B2,
    }
}

fn records_on_b2() -> PlacementPolicy {
    PlacementPolicy {
        snapshot: Destination::B2,
        replay_log: Destination::B2,
        record_owned_blob: Destination::R2,
    }
}

async fn write_generated(store: &Arc<dyn NamespacedStore>) -> Result<()> {
    let records = store.open_namespace("spike-records").await?;
    records
        .batch_put(vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
            (b"c".to_vec(), b"three".to_vec()),
            (b"d".to_vec(), b"four".to_vec()),
        ])
        .await?;
    let blobs = store.open_namespace(CAS_COLLECTION).await?;
    blobs
        .batch_put(vec![
            (b"blob-1".to_vec(), b"blob-bytes-1".to_vec()),
            (b"blob-2".to_vec(), b"blob-bytes-2".to_vec()),
        ])
        .await?;
    store.restore_durability_barrier().await?;
    Ok(())
}

fn check_derived(format: &CloudFormat, profile_one: &CloudFormat) -> Result<()> {
    if HOME_FORMAT != 12
        || format.disk_format != HOME_FORMAT
        || profile_one.disk_format != HOME_FORMAT
    {
        return Err("derived disk format is not format 12".into());
    }
    if format.protocol != CLOUD_PROTOCOL
        || format.profile_describes_disk()
        || profile_one.profile_describes_disk()
    {
        return Err("backup profile was treated as the disk format".into());
    }
    if profile_one.backup_profile != 1 || format.backup_profile != BACKUP_PROFILE {
        return Err("envelope profile was not recorded".into());
    }
    expect_err(
        derive_cloud_format(envelope_protocol(1), records_on_r2()),
        "unsupported protocol",
    )?;
    Ok(())
}

fn envelope_protocol(protocol: u8) -> CloudEnvelope {
    CloudEnvelope {
        protocol,
        backup_profile: BACKUP_PROFILE,
    }
}

fn check_capture(manifest: &CloudManifest) -> Result<()> {
    if manifest.disk_format != HOME_FORMAT
        || manifest.protocol != CLOUD_PROTOCOL
        || manifest.profile_describes_disk()
        || manifest.deletes_local()
        || manifest.restorable
        || !manifest.selected_closure
    {
        return Err("manifest does not keep the disk and cloud versions apart".into());
    }
    let reported: Vec<&str> = manifest.parts.iter().map(|part| part.part_id).collect();
    if reported != export_part_ids() {
        return Err("part report dropped or reordered a disk export part".into());
    }
    for id in [
        "node-marker",
        "main-descriptor",
        "main-current",
        "cas-descriptor",
        "cas-current",
    ] {
        if report(manifest, id)?.artifact_count == 0 {
            return Err(format!("required part {id} has no frozen artifact").into());
        }
    }
    if report(manifest, "main-run")?.artifact_count == 0
        || report(manifest, "cas-run")?.artifact_count == 0
    {
        return Err("generated home did not export both a snapshot run and a blob run".into());
    }
    let lock = report(manifest, "run-lock")?;
    if lock.presence != ExportPresence::LocalOnly
        || lock.reason.is_empty()
        || lock.artifact_count != 0
        || lock.destination.is_some()
        || manifest
            .objects
            .iter()
            .any(|object| object.part_id == "run-lock")
        || manifest
            .excluded
            .iter()
            .all(|part| part.part_id != "run-lock")
    {
        return Err("run lock was not kept local".into());
    }
    let replay = report(manifest, "replay-segment")?;
    if replay.presence != ExportPresence::Gap
        || replay.artifact_count != 0
        || !replay
            .reason
            .contains("sealed replay segments are not in the frozen run capture")
    {
        return Err("replay gap was treated as an empty export".into());
    }
    if manifest
        .objects
        .iter()
        .any(|object| object.log_span.is_some())
    {
        return Err("frozen run capture invented a replay span".into());
    }
    Ok(())
}

fn check_placement(plan_r2: &CloudManifest, plan_b2: &CloudManifest) -> Result<()> {
    if role_destination(plan_r2, ExportRole::Snapshot)? != Destination::R2
        || role_destination(plan_b2, ExportRole::Snapshot)? != Destination::B2
        || role_destination(plan_r2, ExportRole::RecordOwnedBlob)? != Destination::B2
        || role_destination(plan_b2, ExportRole::RecordOwnedBlob)? != Destination::R2
        || report(plan_r2, "replay-segment")?.destination != Some(Destination::R2)
        || report(plan_b2, "replay-segment")?.destination != Some(Destination::B2)
    {
        return Err("placement did not follow the role policy".into());
    }
    if count_role(plan_r2, ExportRole::Snapshot) == 0
        || count_role(plan_r2, ExportRole::RecordOwnedBlob) == 0
    {
        return Err("plan did not include both exported roles".into());
    }
    Ok(())
}

fn check_bytes(
    pin: &PinnedDiskExport,
    plan_r2: &CloudManifest,
    plan_b2: &CloudManifest,
) -> Result<()> {
    let left = by_identity(plan_r2);
    let right = by_identity(plan_b2);
    if left.len() != right.len() {
        return Err("placement change added or dropped an artifact".into());
    }
    for (identity, signature) in &left {
        let &(sha, bytes, destination) = signature;
        let Some(&(other_sha, other_bytes, other_destination)) = right.get(identity) else {
            return Err("placement change renamed an artifact".into());
        };
        if sha != other_sha || bytes != other_bytes || destination == other_destination {
            return Err("source bytes changed or the destination did not".into());
        }
        let reread = reread_sha256(pin, identity, bytes)?;
        if reread != sha {
            return Err(format!("pin bytes for {identity} do not match the descriptor").into());
        }
    }
    Ok(())
}

fn check_refusals(descriptors: &[ArtifactDescriptor], format: &CloudFormat) -> Result<()> {
    let selected = [ExportRole::Snapshot, ExportRole::RecordOwnedBlob];
    let mut wrong_format = descriptors.to_vec();
    let Some(first) = wrong_format.first_mut() else {
        return Err("pin produced no artifact".into());
    };
    first.encoding.disk_format = 99;
    expect_err(
        plan(&wrong_format, format, &selected),
        "unsupported disk format",
    )?;
    let mut invented = descriptors.to_vec();
    let Some(first) = invented.first_mut() else {
        return Err("pin produced no artifact".into());
    };
    first.part_id = "invented-part";
    expect_err(plan(&invented, format, &selected), "unclassified part")?;
    expect_err(
        plan(descriptors, format, &[ExportRole::RecordOwnedBlob]),
        "incomplete closure",
    )?;
    let replay = plan(descriptors, format, &[ExportRole::ReplayLog]);
    match &replay {
        Err(error) => {
            let text = error.to_string();
            if !text.contains("replay gap")
                || !text.contains("sealed replay segments are not in the frozen run capture")
            {
                return Err(format!("replay error did not name the gap: {text}").into());
            }
        }
        Ok(_) => return Err("replay selection produced a manifest".into()),
    }
    Ok(())
}

fn check_snapshot_only(descriptors: &[ArtifactDescriptor], format: &CloudFormat) -> Result<()> {
    let manifest = plan(descriptors, format, &[ExportRole::Snapshot])?;
    if !manifest.selected_closure
        || manifest
            .objects
            .iter()
            .any(|object| object.role != ExportRole::Snapshot)
    {
        return Err("snapshot-only plan was not a closed snapshot set".into());
    }
    Ok(())
}

fn report<'a>(manifest: &'a CloudManifest, id: &str) -> Result<&'a PartReport> {
    manifest
        .parts
        .iter()
        .find(|part| part.part_id == id)
        .ok_or_else(|| format!("missing part {id}").into())
}

fn role_destination(manifest: &CloudManifest, role: ExportRole) -> Result<Destination> {
    let mut found = None;
    for object in &manifest.objects {
        if object.role != role {
            continue;
        }
        if let Some(previous) = found {
            if previous != object.destination {
                return Err("one role has two destinations".into());
            }
        }
        found = Some(object.destination);
    }
    found.ok_or_else(|| format!("no upload object for {role:?}").into())
}

fn count_role(manifest: &CloudManifest, role: ExportRole) -> usize {
    manifest
        .objects
        .iter()
        .filter(|object| object.role == role)
        .count()
}

fn by_identity(manifest: &CloudManifest) -> BTreeMap<&str, (&str, u64, Destination)> {
    manifest.objects.iter().map(object_signature).collect()
}

fn object_signature(object: &CloudObject) -> (&str, (&str, u64, Destination)) {
    (
        object.identity.as_str(),
        (object.sha256.as_str(), object.bytes, object.destination),
    )
}

fn reread_sha256(pin: &PinnedDiskExport, identity: &str, bytes: u64) -> Result<String> {
    let mut hasher = sha2::Sha256::new();
    let mut offset = 0_u64;
    let mut buf = [0_u8; 8192];
    while offset < bytes {
        let read = pin.read_at(identity, offset, &mut buf)?;
        if read == 0 {
            return Err(format!("short read of {identity}").into());
        }
        hasher.update(&buf[..read]);
        let read = u64::try_from(read)?;
        let Some(next) = offset.checked_add(read) else {
            return Err("artifact length overflow".into());
        };
        offset = next;
    }
    if pin.read_at(identity, offset, &mut buf)? != 0 {
        return Err(format!("{identity} is longer than its descriptor").into());
    }
    let result = hasher.finalize();
    let encoded: String = result.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(encoded)
}

fn expect_err<T>(
    result: std::result::Result<T, fold_db::storage::cloud_contract::ContractError>,
    phrase: &str,
) -> Result<()> {
    match result {
        Err(error) => {
            let text = error.to_string();
            if text.contains(phrase) {
                Ok(())
            } else {
                Err(format!("error `{text}` does not contain `{phrase}`").into())
            }
        }
        Ok(_) => Err(format!("expected an error containing `{phrase}`").into()),
    }
}

fn summary(plan_r2: &CloudManifest, plan_b2: &CloudManifest) -> String {
    let value = serde_json::json!({
        "disk_format": plan_r2.disk_format,
        "protocol": plan_r2.protocol,
        "backup_profile": plan_r2.backup_profile,
        "profile_describes_disk": plan_r2.profile_describes_disk(),
        "deletes_local": plan_r2.deletes_local(),
        "restorable": plan_r2.restorable,
        "selected_closure": plan_r2.selected_closure,
        "snapshot_objects": count_role(plan_r2, ExportRole::Snapshot),
        "blob_objects": count_role(plan_r2, ExportRole::RecordOwnedBlob),
        "snapshot_destination_records_on_r2": role_name(plan_r2, ExportRole::Snapshot),
        "snapshot_destination_records_on_b2": role_name(plan_b2, ExportRole::Snapshot),
        "blob_destination_records_on_r2": role_name(plan_r2, ExportRole::RecordOwnedBlob),
        "blob_destination_records_on_b2": role_name(plan_b2, ExportRole::RecordOwnedBlob),
        "part_ids": export_part_ids(),
    });
    format!("CONTRACT_JSON {value}")
}

fn role_name(manifest: &CloudManifest, role: ExportRole) -> &'static str {
    match role_destination(manifest, role) {
        Ok(Destination::R2) => "r2",
        Ok(Destination::B2) => "b2",
        Err(_) => "missing",
    }
}
