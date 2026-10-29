use super::*;

pub(super) fn trusted_key(args: &TrustedKeyArgs) -> Result<(), String> {
    let signing_key = load_signing_key(&args.signing_key_locator)?;
    let verifying_key = signing_key.verifying_key();
    println!(
        "{}={}",
        key_id(&verifying_key),
        BASE64.encode(verifying_key.to_bytes())
    );
    Ok(())
}

pub(super) fn build(args: &BuildArgs) -> Result<(), String> {
    let snapshot_bytes = read_file("snapshot", &args.snapshot)?;
    let mut snapshot: SnapshotEnvelope = serde_json::from_slice(&snapshot_bytes)
        .map_err(|e| format!("snapshot parse failed: {e}"))?;
    // `build_artifacts_from_snapshot` applies the shared-only projection so
    // private legacy bootstrap / unknown rows never enter pack artifacts.
    if args.recompute_embeddings {
        snapshot = recompute_snapshot_embeddings(snapshot, args.max_schemas)?;
    } else if let Some(max) = args.max_schemas {
        snapshot = project_snapshot_shared_only(snapshot, offline_is_system_schema);
        snapshot.schemas.truncate(max);
    }
    let artifacts = build_artifacts_from_snapshot(&snapshot, &args.policy_version)?;

    fs::create_dir_all(&args.out_dir)
        .map_err(|e| format!("create out-dir {}: {e}", args.out_dir.display()))?;

    let resolver_config = match args.resolver_config.as_deref() {
        Some(path) => {
            let bytes = read_file("resolver_config", path)?;
            let config: ResolverConfig = parse_resolver_config(&bytes)
                .map_err(|e| format!("resolver_config parse failed: {e}"))?;
            config
                .validate()
                .map_err(|e| format!("resolver_config invalid: {e}"))?;
            serde_json::to_vec_pretty(&config)
                .map_err(|e| format!("resolver_config serialization failed: {e}"))?
        }
        None => serde_json::to_vec_pretty(&artifacts.resolver_config)
            .map_err(|e| format!("resolver_config serialization failed: {e}"))?,
    };

    write_artifact(&args.out_dir.join("resolver_config.json"), &resolver_config)?;
    write_artifact(
        &args.out_dir.join("schema_snapshot.json"),
        &artifacts.schema_snapshot,
    )?;
    write_artifact(
        &args.out_dir.join("embedding_artifact.json"),
        &artifacts.embedding_artifact,
    )?;

    let receipt = json!({
        "action": "build",
        "snapshot_format_version": snapshot.format_version,
        "source_snapshot_hash": artifact_sha256_hex(&snapshot_bytes),
        "resolver_config_hash": artifact_sha256_hex(&resolver_config),
        "schema_snapshot_hash": artifact_sha256_hex(&artifacts.schema_snapshot),
        "embedding_artifact_hash": artifact_sha256_hex(&artifacts.embedding_artifact),
        "algorithm_id": artifacts.resolver_config.algorithm.id,
        "algorithm_version": artifacts.resolver_config.algorithm.version,
        "resolver_contract_version": artifacts.resolver_config.resolver_contract_version,
        "policy_version": artifacts.resolver_config.policy_version,
        "out_dir": args.out_dir.display().to_string(),
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&receipt).map_err(|e| e.to_string())?
    );
    Ok(())
}

/// Build the signed manifest for `inputs` and verify it against the trusted keys.
pub(super) fn sign_and_verify_manifest(
    args: &PublishArgs,
    env: Env,
    inputs: PackInputs<'_>,
) -> Result<ResolverPackManifest, String> {
    let signing_key = load_signing_key(&args.signing_key_locator)?;
    let trusted_keys = parse_trusted_keys(&args.trusted_keys)?;
    let issued_at = Utc::now();
    let expires_at = args.expires_at.unwrap_or(issued_at + Duration::days(90));
    let manifest = build_signed_manifest(
        PackInputs {
            resolver_config: inputs.resolver_config,
            schema_snapshot: inputs.schema_snapshot,
            embedding_artifact: inputs.embedding_artifact,
        },
        env,
        &signing_key,
        issued_at,
        Some(expires_at),
    )?;

    let embedding: EmbeddingArtifact = parse_embedding_artifact(inputs.embedding_artifact)
        .map_err(|e| format!("embedding artifact parse failed: {e}"))?;
    verify_resolver_pack_manifest(
        &manifest,
        inputs.resolver_config,
        inputs.schema_snapshot,
        inputs.embedding_artifact,
        &trusted_keys,
        env,
        &embedding.embedder_id,
    )
    .map_err(|e| format!("signed manifest validation failed: {e}"))?;
    Ok(manifest)
}

/// Upload the pack to the bucket unless the run only wrote a local layout.
pub(super) async fn upload_published_pack(
    args: &PublishArgs,
    plan: &PublishPlan,
    manifest_bytes: &[u8],
    inputs: PackInputs<'_>,
) -> Result<(), String> {
    let bucket = args
        .r2_bucket
        .clone()
        .ok_or("--r2-bucket or SCHEMA_RESOLVER_PACK_BUCKET is required without --dry-run (or use --local-out-dir only)")?;
    let uploader =
        S3Uploader::new(args.aws_region.as_deref(), args.r2_endpoint_url.as_deref()).await;
    upload_pack(&uploader, &bucket, plan, manifest_bytes, inputs).await
}

pub(super) async fn publish(args: PublishArgs) -> Result<(), String> {
    let resolver_config = read_file("resolver_config", &args.resolver_config)?;
    let schema_snapshot = read_file("schema_snapshot", &args.schema_snapshot)?;
    let embedding_artifact = read_file("embedding_artifact", &args.embedding_artifact)?;
    let inputs = || PackInputs {
        resolver_config: &resolver_config,
        schema_snapshot: &schema_snapshot,
        embedding_artifact: &embedding_artifact,
    };

    let env = Env::from(args.env);
    let manifest = sign_and_verify_manifest(&args, env, inputs())?;

    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| format!("manifest serialization failed: {e}"))?;
    let plan = publish_plan(
        env,
        &manifest,
        &resolver_config,
        &schema_snapshot,
        &embedding_artifact,
    );
    let receipt = PublishReceipt {
        action: "publish",
        env,
        timestamp: Utc::now(),
        key_id: manifest.signing_key_id.clone(),
        manifest_hash: artifact_sha256_hex(&manifest_bytes),
        artifact_hashes: artifact_hashes(&manifest),
        validation: "passed",
        upload: if args.dry_run { "dry_run" } else { "uploaded" },
        dry_run: args.dry_run,
        plan,
    };

    if let Some(local_dir) = args.local_out_dir.as_ref() {
        write_pack_local_layout(local_dir, &receipt.plan, &manifest_bytes, inputs())?;
        eprintln!(
            "wrote local pack layout under {} (serve this directory as base_url)",
            local_dir.display()
        );
    }

    if args.dry_run {
        print_dry_run_manifest(&manifest, &receipt)?;
    } else if args.r2_bucket.is_some() || args.local_out_dir.is_none() {
        upload_published_pack(&args, &receipt.plan, &manifest_bytes, inputs()).await?;
    }
    write_receipt(args.audit_log.as_deref(), &receipt)?;
    Ok(())
}

pub(super) async fn rollback(args: RollbackArgs) -> Result<(), String> {
    let manifest_bytes = read_file("manifest", &args.manifest)?;
    let manifest: ResolverPackManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| format!("manifest parse failed: {e}"))?;
    let trusted_keys = parse_trusted_keys(&args.trusted_keys)?;
    let env = Env::from(args.env);

    if manifest.format_version != RESOLVER_PACK_FORMAT_VERSION {
        return Err(format!(
            "rollback requires format_version {}, got {}",
            RESOLVER_PACK_FORMAT_VERSION, manifest.format_version
        ));
    }

    let embedding_id = rollback_embedding_id(&args, &manifest, &trusted_keys, env)?;
    let plan = rollback_plan(env, &manifest, &manifest_bytes, &embedding_id);
    let receipt = PublishReceipt {
        action: "rollback",
        env,
        timestamp: Utc::now(),
        key_id: manifest.signing_key_id.clone(),
        manifest_hash: artifact_sha256_hex(&manifest_bytes),
        artifact_hashes: BTreeMap::from([
            ("resolver_config", manifest.resolver_config_hash.clone()),
            ("schema_snapshot", manifest.schema_snapshot_hash.clone()),
            (
                "embedding_artifact",
                manifest.embedding_artifact_hash.clone(),
            ),
        ]),
        validation: "passed",
        upload: if args.dry_run {
            "dry_run"
        } else {
            "promoted_latest_pointer"
        },
        dry_run: args.dry_run,
        plan,
    };
    if args.dry_run {
        println!(
            "{}",
            serde_json::to_string_pretty(&receipt).map_err(|e| e.to_string())?
        );
    } else {
        let bucket = args
            .r2_bucket
            .clone()
            .ok_or("--r2-bucket or SCHEMA_RESOLVER_PACK_BUCKET is required without --dry-run")?;
        let uploader =
            S3Uploader::new(args.aws_region.as_deref(), args.r2_endpoint_url.as_deref()).await;
        uploader
            .put(
                &bucket,
                &receipt.plan.latest_manifest_pointer_key,
                manifest_bytes,
                "application/json",
            )
            .await?;
    }
    write_receipt(args.audit_log.as_deref(), &receipt)?;
    Ok(())
}
