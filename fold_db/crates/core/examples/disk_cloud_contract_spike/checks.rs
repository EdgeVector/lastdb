//! Checks for the disk-to-cloud contract spike.
//!
//! These read pinned descriptors and the derived plan. They do not open a path.

use std::collections::BTreeMap;

use fold_db::storage::cloud_contract::{
    derive_cloud_format, export_part_ids, plan, ArtifactDescriptor, CloudEnvelope, CloudFormat,
    CloudManifest, CloudObject, ContractError, Destination, ExportPresence, ExportRole, PartReport,
    BACKUP_PROFILE, CLOUD_PROTOCOL,
};
use fold_db::storage::disk_pack::{PinnedDiskExport, HOME_FORMAT};
use sha2::Digest as _;

use super::Result;

pub(super) fn check_derived(format: &CloudFormat, profile_one: &CloudFormat) -> Result<()> {
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
        derive_cloud_format(envelope_protocol(1), super::records_on_r2()),
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

pub(super) fn check_capture(manifest: &CloudManifest) -> Result<()> {
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

pub(super) fn check_placement(plan_r2: &CloudManifest, plan_b2: &CloudManifest) -> Result<()> {
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

pub(super) fn check_bytes(
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
        let Some(&(other_sha, other_bytes, other_destination)) = right.get(*identity) else {
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

pub(super) fn check_refusals(
    descriptors: &[ArtifactDescriptor],
    format: &CloudFormat,
) -> Result<()> {
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

pub(super) fn check_snapshot_only(
    descriptors: &[ArtifactDescriptor],
    format: &CloudFormat,
) -> Result<()> {
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

fn expect_err<T>(result: std::result::Result<T, ContractError>, phrase: &str) -> Result<()> {
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

pub(super) fn summary(plan_r2: &CloudManifest, plan_b2: &CloudManifest) -> String {
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
