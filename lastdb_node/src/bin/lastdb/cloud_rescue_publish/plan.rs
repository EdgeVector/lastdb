//! Plan load/create, wait and report helpers for the S0 publisher. Moved verbatim from `cloud_rescue_publish.rs`.

use super::*;

pub(super) async fn load_or_create_plan(
    home: &Path,
    execute: bool,
) -> Result<LoadedRescuePlan, String> {
    let source = lastdb_node::cloud::validate_stopped_backup_source_copy(home)?;
    let key = source_key(home)?;
    let plan_path = home.join(PLAN_FILE);
    let existing = read_small_regular(&plan_path, MAX_PLAN_BYTES)?;
    if execute && existing.is_none() {
        return Err(
            "S0 rescue plan is missing; run backup-while-off without --execute first".into(),
        );
    }
    let store = open_store(home, key)?;
    store
        .verify_integrity()
        .map_err(|error| format!("stopped-copy LastStore integrity failed: {error}"))?;
    let plan = if let Some(bytes) = existing {
        serde_json::from_slice::<RescuePlan>(&bytes)
            .map_err(|error| format!("invalid saved S0 rescue plan: {error}"))?
    } else {
        prepare_offline_s0_restore_marker(&store).await?;
        let mut manifest = store
            .cut_backup_manifest_with_cloud_presence_strict(None, None)
            .map_err(|error| format!("cut complete S0 rescue manifest: {error}"))?;
        omit_root_cut_retirement_receipts(&mut manifest)?;
        let manifest_sha256 = manifest_sha256_hex(&manifest)
            .map_err(|error| format!("hash S0 rescue manifest: {error}"))?;
        let db_hash = fold_db::storage::laststore::read_cloud_db_hash(&home.join("data"))
            .ok_or_else(|| "stopped copy has no cloud database identity".to_string())?;
        let layout = laststore::describe_home(home.join("data"))
            .map_err(|error| format!("read stopped-copy layout: {error}"))?
            .ok_or_else(|| "stopped copy has no LastStore layout".to_string())?;
        let descriptor = RecoveryDescriptorV1::new(
            &manifest.store_uuid,
            &db_hash,
            layout,
            &manifest_sha256,
            manifest.counter,
            manifest.epoch,
        )?;
        let (descriptor_name, ciphertext) = descriptor.seal(&key)?;
        let descriptor_sha256 = sha256_hex(&ciphertext);
        let plan = RescuePlan {
            version: 2,
            source: source.clone(),
            manifest,
            manifest_sha256,
            db_hash,
            descriptor_name,
            descriptor_sha256,
            descriptor_base64: base64::engine::general_purpose::STANDARD.encode(ciphertext),
        };
        validate_manifest(&plan)?;
        plan.descriptor_bytes(&key)?;
        let candidates = validate_candidates(home, &store, &plan.manifest)?;
        page_groups(&candidates)?;
        let bytes =
            serde_json::to_vec(&plan).map_err(|error| format!("encode S0 rescue plan: {error}"))?;
        if bytes.len() as u64 > MAX_PLAN_BYTES {
            return Err("S0 rescue plan exceeds its size limit".into());
        }
        save_once(&plan_path, &bytes)?;
        plan
    };
    if plan.source != source {
        return Err("S0 rescue plan belongs to a different stopped copy".into());
    }
    validate_manifest(&plan)?;
    require_offline_s0_restore_marker(&store).await?;
    plan.descriptor_bytes(&key)?;
    let candidates = validate_candidates(home, &store, &plan.manifest)?;
    page_groups(&candidates)?;
    Ok((plan, candidates, key))
}

pub(super) fn now_secs() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| format!("system clock is before Unix time: {error}"))
}

pub(super) async fn wait_until(deadline: u64, wait: bool, stage: &str) -> Result<(), String> {
    let now = now_secs()?;
    if deadline <= now {
        return Ok(());
    }
    let delay = deadline - now;
    if !wait || delay > MAX_WAIT_SECS {
        return Err(format!(
            "S0 rescue {stage} must wait until Unix second {deadline}; rerun with --execute --wait"
        ));
    }
    tokio::time::sleep(Duration::from_secs(delay.saturating_add(1))).await;
    Ok(())
}

pub(super) fn print_report(plan: &RescuePlan, chunks: usize, published: bool, json: bool) {
    let report = serde_json::json!({
        "ok": true,
        "published": published,
        "source_scope": "primary_only",
        "cloud_sync": "off",
        "manifest_sha256": plan.manifest_sha256,
        "manifest_counter": plan.manifest.counter,
        "chunks": chunks,
        "source_pid": plan.source.source_pid,
        "flush_proof": plan.source.flush_proof.as_deref().unwrap_or("receipt"),
    });
    if json {
        println!("{report}");
    } else if published {
        println!(
            "Primary-only S0 rescue pointer committed: {}",
            plan.manifest_sha256
        );
    } else {
        println!(
            "Primary-only S0 rescue plan ready: {} ({} chunks)",
            plan.manifest_sha256, chunks
        );
    }
}
