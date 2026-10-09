use super::*;

pub(super) async fn read_prior_backup_manifest(
    scoped: &fold_db::sync::auth::AuthClient,
    s3: &fold_db::sync::s3::S3Client,
    db_hash: &str,
) -> Result<(Option<String>, Option<BackupManifest>), String> {
    let latest = scoped
        .backup_latest_get_optional()
        .await
        .map_err(|error| format!("read prior cloud backup pointer: {error}"))?;
    let (prior_sha, prior) = if let Some(latest) = latest {
        let prior_sha = latest.latest.manifest_sha256;
        let download = scoped
            .presign_backup_manifest_download(&prior_sha)
            .await
            .map_err(|error| format!("read prior cloud backup manifest URL: {error}"))?;
        let bytes = s3
            .download_limited(&download, Some(MAX_MANIFEST_BYTES))
            .await
            .map_err(|error| format!("read prior cloud backup manifest: {error}"))?
            .ok_or_else(|| "prior cloud backup manifest is missing".to_string())?;
        let prior: BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode prior cloud backup manifest: {error}"))?;
        if manifest_sha256_hex(&prior).map_err(|error| error.to_string())? != prior_sha
            || cloud_db_hash_for_store_uuid(&prior.store_uuid) != db_hash
            || prior.counter != latest.latest.counter
        {
            return Err("prior cloud backup manifest does not match its pointer or source".into());
        }
        (Some(prior_sha), Some(prior))
    } else {
        (None, None)
    };
    Ok((prior_sha, prior))
}

pub(super) fn require_first_backup_writer_inventory(
    prior_manifest_present: bool,
    visible_log_object_count: usize,
    unparsed_log_objects: u64,
) -> Result<(), String> {
    if !prior_manifest_present && (visible_log_object_count != 0 || unparsed_log_objects != 0) {
        return Err("normal backup is absent, but peer or unknown cloud log objects remain".into());
    }
    Ok(())
}

struct InventoryFields {
    visible: usize,
    unparsed: u64,
    prior_present: bool,
    prior_sha: Option<String>,
    prior_counter: Option<u64>,
}

fn validate_inventory_fields(report: &serde_json::Value) -> Result<InventoryFields, String> {
    let visible = report["visible_log_object_count"]
        .as_u64()
        .ok_or("writer inventory omitted its visible count")?;
    let visible =
        usize::try_from(visible).map_err(|_| "writer inventory count exceeds this host")?;
    let unparsed = report["unparsed_log_objects"]
        .as_u64()
        .ok_or("writer inventory omitted its unparsed count")?;
    let prior_present = report["prior_manifest_present"]
        .as_bool()
        .ok_or("writer inventory omitted prior backup presence")?;
    if report["writer_list_complete"] != true {
        return Err("writer inventory did not complete the cloud log list".into());
    }
    let (prior_sha, prior_counter) = if prior_present {
        (
            Some(
                report["prior_manifest_sha256"]
                    .as_str()
                    .ok_or("writer inventory omitted the prior manifest")?
                    .to_string(),
            ),
            Some(
                report["prior_manifest_counter"]
                    .as_u64()
                    .ok_or("writer inventory omitted the prior counter")?,
            ),
        )
    } else if report
        .get("prior_manifest_sha256")
        .is_some_and(serde_json::Value::is_null)
        && report
            .get("prior_manifest_counter")
            .is_some_and(serde_json::Value::is_null)
        && report
            .get("prior_manifest_created_at_unix_secs")
            .is_some_and(serde_json::Value::is_null)
        && report["visible_writer_count"].as_u64() == Some(0)
        && report["writers"] == serde_json::json!([])
        && report["writer_list_pages"]
            .as_u64()
            .is_some_and(|pages| pages > 0)
        && report["local_writer_skipped"] == true
        && report
            .get("peer_logs_newer_than_prior_backup")
            .is_some_and(serde_json::Value::is_null)
        && report
            .get("unknown_logs_newer_than_prior_backup")
            .is_some_and(serde_json::Value::is_null)
    {
        (None, None)
    } else {
        return Err("writer inventory gives a prior manifest for an absent backup".into());
    };
    require_first_backup_writer_inventory(prior_present, visible, unparsed)?;
    Ok(InventoryFields {
        visible,
        unparsed,
        prior_present,
        prior_sha,
        prior_counter,
    })
}

pub(super) fn save_writer_inventory_proof(
    home: &Path,
    plan: &RescuePlan,
    report: serde_json::Value,
) -> Result<(), String> {
    let InventoryFields {
        visible,
        unparsed,
        prior_present,
        prior_sha,
        prior_counter,
    } = validate_inventory_fields(&report)?;
    let path = home.join(WRITER_PROOF_FILE);
    let existing = read_small_regular(&path, 65_536)?;
    if let Some(bytes) = &existing {
        let proof: WriterInventoryProof =
            serde_json::from_slice(bytes).map_err(|_| "invalid saved S0 writer proof")?;
        if proof.version != 1
            || proof.source_scope != "primary_only"
            || proof.rescue_manifest_sha256 != plan.manifest_sha256
            || proof.db_hash != plan.db_hash
            || proof.source != plan.source
            || proof
                .prior_manifest_present
                .unwrap_or(proof.prior_manifest_sha256.is_some())
                != prior_present
            || proof.prior_manifest_sha256 != prior_sha
            || proof.prior_manifest_counter != prior_counter
        {
            return Err("saved S0 writer proof belongs to a different rescue".into());
        }
    }
    let proof = WriterInventoryProof {
        version: 1,
        source_scope: "primary_only".into(),
        rescue_manifest_sha256: plan.manifest_sha256.clone(),
        db_hash: plan.db_hash.clone(),
        source: plan.source.clone(),
        prior_manifest_present: Some(prior_present),
        prior_manifest_sha256: prior_sha,
        prior_manifest_counter: prior_counter,
        visible_log_object_count: visible,
        unparsed_log_objects: unparsed,
        inspected_at_unix_secs: now_secs()?,
        inventory: report,
    };
    let bytes =
        serde_json::to_vec(&proof).map_err(|error| format!("encode S0 writer proof: {error}"))?;
    if existing.is_some() {
        save_replace(&path, &bytes)
    } else {
        save_once(&path, &bytes)
    }
}
