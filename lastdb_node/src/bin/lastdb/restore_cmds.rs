use super::*;

pub(super) fn restore_command(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
) -> Result<(), String> {
    match restore_command_inner(data_dir, into, env, api_url, json_only) {
        Ok(()) => Ok(()),
        Err(failure) => {
            if json_only {
                println!("{}", render_restore_failure_json(&failure));
            }
            Err(failure.detail)
        }
    }
}

pub(super) fn restore_command_inner(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
) -> Result<(), RestoreFailure> {
    restore_command_inner_with_progress(data_dir, into, env, api_url, json_only, None)
}

pub(super) fn restore_command_with_progress(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
) -> Result<(), String> {
    let reporter = restore_progress_reporter::Reporter::stderr();
    let result = restore_command_inner_with_progress(
        data_dir,
        into,
        env,
        api_url,
        json_only,
        Some(&reporter.progress),
    );
    reporter.finish(result.is_ok());
    result.map_err(|failure| {
        if json_only {
            println!("{}", render_restore_failure_json(&failure));
        }
        failure.detail
    })
}

pub(super) fn restore_command_with_chunk_cache(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
    progress_json: bool,
    cache_home: &Path,
) -> Result<(), String> {
    let reporter = progress_json.then(restore_progress_reporter::Reporter::stderr);
    let result = restore_command_inner_with_cache(
        data_dir,
        into,
        env,
        api_url,
        json_only,
        reporter.as_ref().map(|reporter| reporter.progress.as_ref()),
        RestoreSourceMode::Normal {
            cache_home: Some(cache_home),
        },
    );
    if let Some(reporter) = reporter {
        reporter.finish(result.is_ok());
    }
    result.map_err(|failure| {
        if json_only {
            println!("{}", render_restore_failure_json(&failure));
        }
        failure.detail
    })
}

pub(super) fn restore_command_inner_with_progress(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
    progress: Option<&fold_db::sync::engine::RestoreProgress>,
) -> Result<(), RestoreFailure> {
    restore_command_inner_with_cache(
        data_dir,
        into,
        env,
        api_url,
        json_only,
        progress,
        RestoreSourceMode::Normal { cache_home: None },
    )
}

pub(super) fn restore_command_inner_with_cache(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
    progress: Option<&fold_db::sync::engine::RestoreProgress>,
    mode: RestoreSourceMode<'_>,
) -> Result<(), RestoreFailure> {
    use RestoreFailureCode as Code;
    use RestoreFailureStage as Stage;

    let (cache_home, remote_s0_only, remote_latest, selection) = match mode {
        RestoreSourceMode::Normal { cache_home } => {
            (cache_home, false, false, RemoteRecoverySelector::default())
        }
        RestoreSourceMode::RemoteS0(selection) => (None, true, false, selection),
        RestoreSourceMode::RemoteLatest(selection) => (None, false, true, selection),
    };
    let remote_only = remote_s0_only || remote_latest;

    let source_home = lastdb_node::host::resolve_home(data_dir)
        .map_err(|detail| RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail))?;
    let target_home = expand_home_path(into)
        .map_err(|detail| RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail))?;
    refuse_same_home(&source_home, &target_home)
        .map_err(|detail| RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail))?;
    if remote_only {
        refuse_overlapping_restore_cache(&source_home, &target_home).map_err(|detail| {
            RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail)
        })?;
        refuse_recovery_home_with_store_data(&source_home).map_err(|detail| {
            RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail)
        })?;
    }
    let resume_mutation_log = dest_has_committed_s0(&target_home);
    if cache_home.is_some() || remote_only {
        refuse_non_fresh_migration_home(&target_home).map_err(|detail| {
            RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail)
        })?;
    }
    if !resume_mutation_log || cache_home.is_some() || remote_only {
        refuse_non_fresh_restore_home(&target_home).map_err(|detail| {
            RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail)
        })?;
    }
    let (target_active_cloud, target_paused_cloud) =
        lastdb_node::cloud::cloud_sync_paths(&target_home);
    if target_active_cloud.exists()
        || lastdb_node::cloud::cloud_resume_requested_path(&target_home).exists()
        || lastdb_node::cloud::cloud_resume_ready_path(&target_home).exists()
    {
        return Err(RestoreFailure::new(
            Stage::Preflight,
            Code::OperationFailed,
            "restore destination has active or stale cloud state; use a fresh destination",
        ));
    }

    let cache = cache_home
        .map(|home| {
            let home = expand_home_path(home)?;
            refuse_overlapping_restore_cache(&home, &target_home)?;
            fold_db::sync::engine::RestoreChunkCache::new(&home)
        })
        .transpose()
        .map_err(|detail| RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail))?;

    // The source store identity selects the only cloud namespace this command
    // may read. Missing or malformed local proof must fail before auth or S3.
    let local_source_db_hash = if remote_only {
        None
    } else {
        Some(
            fold_db::storage::laststore::read_cloud_db_hash(&source_home.join("data"))
                .ok_or_else(|| {
                    RestoreFailure::new(
                        Stage::SourceScope,
                        Code::InvalidSourceScope,
                        "source LastStore cloud identity is missing or invalid; refusing an unscoped restore",
                    )
                })?,
        )
    };

    let (active_cloud, paused_cloud) = lastdb_node::cloud::cloud_sync_paths(&source_home);
    if active_cloud.exists() && paused_cloud.exists() {
        return Err(RestoreFailure::new(
            Stage::Preflight,
            Code::OperationFailed,
            "source has both active and paused cloud configuration",
        ));
    }
    let paused_source = remote_only || (!active_cloud.exists() && paused_cloud.exists());
    let source_cloud = if paused_source {
        if remote_only && active_cloud.exists() {
            &active_cloud
        } else {
            &paused_cloud
        }
    } else {
        &active_cloud
    };
    let paused_receipt = if paused_source && !remote_only {
        if !lastdb_node::cloud::cloud_resume_required_path(&source_home).is_file()
            || lastdb_node::cloud::cloud_resume_requested_path(&source_home).exists()
        {
            return Err(RestoreFailure::new(
                Stage::Preflight,
                Code::OperationFailed,
                "paused source needs a completed backup and a durable resume barrier",
            ));
        }
        Some(
            lastdb_node::cloud::read_paused_home_backup_receipt(&source_home).map_err(
                |detail| RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail),
            )?,
        )
    } else {
        None
    };
    let resume_report = if resume_mutation_log && !remote_only {
        Some(
            restore_checkpoint::load(
                &target_home,
                local_source_db_hash.as_deref().expect("local source hash"),
                &source_home,
            )
            .map_err(|detail| {
                RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail)
            })?,
        )
    } else {
        None
    };

    let (url, api_key) =
        load_cloud_creds_from_path(source_cloud, api_url, env).map_err(|detail| {
            RestoreFailure::new(Stage::CloudCredentials, Code::OperationFailed, detail)
        })?;
    let seed_path = source_home.join(lastdb_node::host::IDENTITY_KEY_FILE);
    let seed_bytes = std::fs::read(&seed_path).map_err(|e| {
        RestoreFailure::new(
            Stage::IdentityKey,
            Code::IoError,
            format!("read {}: {e}", seed_path.display()),
        )
    })?;
    if seed_bytes.len() != 32 {
        return Err(RestoreFailure::new(
            Stage::IdentityKey,
            Code::InvalidIdentityKey,
            format!(
                "{} must be 32 bytes, got {}",
                seed_path.display(),
                seed_bytes.len()
            ),
        ));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_bytes);
    let e2e = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed).map_err(|e| {
        RestoreFailure::new(
            Stage::IdentityKey,
            Code::CryptoError,
            format!("E2E derive: {e}"),
        )
    })?;

    // A lost source home has no local store UUID, layout, or receipt. The
    // encrypted account descriptor supplies them only after cloud latest and
    // its exact manifest identity agree. Do this before writing the target.
    let remote_descriptor = if remote_only {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                RestoreFailure::new(
                    Stage::RuntimeInit,
                    Code::OperationFailed,
                    format!("tokio runtime construction failed: {error}"),
                )
            })?;
        let http = std::sync::Arc::new(fold_db::sync::build_shared_http_client());
        let s3 = fold_db::sync::s3::S3Client::new(std::sync::Arc::clone(&http));
        let unscoped = fold_db::sync::auth::AuthClient::new(
            http,
            url.clone(),
            fold_db::sync::auth::SyncAuth::ApiKey(api_key.clone()),
        )
        .without_db_auto_claim();
        let discovered = if remote_s0_only {
            runtime.block_on(discover_remote_recovery_descriptor(
                &unscoped,
                &s3,
                &e2e.encryption_key(),
                selection,
            ))
        } else {
            runtime.block_on(discover_remote_latest_descriptor(
                &unscoped,
                &s3,
                &e2e.encryption_key(),
                selection,
            ))
        };
        Some(discovered.map_err(|detail| {
            RestoreFailure::new(Stage::SourceScope, Code::InvalidSourceScope, detail)
        })?)
    } else {
        None
    };
    let source_db_hash = remote_descriptor
        .as_ref()
        .map(|recovery| recovery.descriptor.db_hash.clone())
        .or(local_source_db_hash)
        .expect("source scope was checked");

    std::fs::create_dir_all(&target_home).map_err(|e| {
        RestoreFailure::new(
            Stage::PrepareDestination,
            Code::IoError,
            format!("create {}: {e}", target_home.display()),
        )
    })?;
    write_owner_only_local(
        &target_home.join(lastdb_node::host::IDENTITY_KEY_FILE),
        &seed_bytes,
    )
    .map_err(|detail| RestoreFailure::new(Stage::PrepareDestination, Code::IoError, detail))?;
    let cloud_bytes = std::fs::read(source_cloud).map_err(|e| {
        RestoreFailure::new(
            Stage::PrepareDestination,
            Code::IoError,
            format!("read {}: {e}", source_cloud.display()),
        )
    })?;
    // Every incomplete restore stays Off, including a failed normal tail replay.
    write_owner_only_local(&target_paused_cloud, &cloud_bytes)
        .map_err(|detail| RestoreFailure::new(Stage::PrepareDestination, Code::IoError, detail))?;
    lastdb_node::cloud::mark_cloud_resume_required(&target_home)
        .map_err(|detail| RestoreFailure::new(Stage::PrepareDestination, Code::IoError, detail))?;

    let data_path = target_home.join("data");
    std::fs::create_dir_all(&data_path).map_err(|e| {
        RestoreFailure::new(
            Stage::PrepareDestination,
            Code::IoError,
            format!("create {}: {e}", data_path.display()),
        )
    })?;
    // Backup chunks keep the source shard/group addresses. The destination
    // descriptor must therefore match the source descriptor before any chunk
    // is installed. Reading the descriptor does not open or lock the live
    // source store. Existing destination descriptors still win through
    // open_existing_or_with, which keeps mutation-log resume stable.
    let opts = if let Some(recovery) = &remote_descriptor {
        recovery
            .descriptor
            .to_options(
                &e2e.encryption_key(),
                env_flag::var_truthy("LASTDB_RESTORE_FRAME_AEAD"),
            )
            .map_err(|detail| {
                let code = if detail == "frame AEAD recovery layout requires opt-in" {
                    Code::FrameAeadOptInRequired
                } else {
                    Code::LayoutMismatch
                };
                RestoreFailure::new(Stage::SourceLayout, code, detail)
            })?
    } else {
        restore_options_from_source_layout(
            &source_home.join("data"),
            e2e.encryption_key(),
            env_flag::var_truthy("LASTDB_RESTORE_FRAME_AEAD"),
        )?
    };
    let store = if opts.packaging == laststore::PackagingMode::FrameAead {
        fold_db::storage::LastStoreNamespacedStore::open_with_options_and_high_water_data_key(
            &data_path,
            opts,
            target_home.join("laststore_high_water.json"),
        )
        .map_err(|e| {
            RestoreFailure::new(
                Stage::OpenDestination,
                Code::OperationFailed,
                format!("open target LastStore (frame_aead): {e}"),
            )
        })?
    } else {
        fold_db::storage::LastStoreNamespacedStore::open_with_options_and_high_water(
            &data_path,
            opts,
            target_home.join("laststore_high_water.json"),
        )
        .map_err(|e| {
            RestoreFailure::new(
                Stage::OpenDestination,
                Code::OperationFailed,
                format!("open target LastStore: {e}"),
            )
        })?
    };
    let store = std::sync::Arc::new(store);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            RestoreFailure::new(
                Stage::RuntimeInit,
                Code::OperationFailed,
                format!("tokio runtime construction failed: {e}"),
            )
        })?;
    let http = std::sync::Arc::new(fold_db::sync::build_shared_http_client());
    let s3 = fold_db::sync::s3::S3Client::new(std::sync::Arc::clone(&http));
    // Publish writes `{cloud_db_hash(source store_uuid)}/backup/latest`. A
    // restore AuthClient with no db_hash looks under the authenticated
    // user_hash instead and reports "backup latest pointer missing".
    let auth = fold_db::sync::auth::AuthClient::new(
        http,
        url,
        fold_db::sync::auth::SyncAuth::ApiKey(api_key),
    )
    .with_db_hash(Some(source_db_hash.clone()))
    .without_db_auto_claim();
    // Cloud outer seal uses the account E2E content key (same key that sealed
    // continuous mutation-log segments). After S0 chunks land, apply the
    // continuous plane so post-S0 history is not write-only.
    let sync_crypto: std::sync::Arc<dyn fold_db::crypto::CryptoProvider> = std::sync::Arc::new(
        fold_db::crypto::LocalCryptoProvider::from_key(e2e.encryption_key()),
    );
    // Mutation-log replay must write through the local at-rest seam, exactly as
    // the daemon wires it (`fold_db_core::factory::local::store_stack`: the
    // engine receives `enc_store`, the cursor store stays raw). Handing the
    // engine the bare LastStore made `stored_replay_value` fall through to its
    // no-key branch and write logical bytes verbatim, so every replayed
    // `main`/`metadata` row landed unsealed on disk while S0 chunks stayed
    // sealed. Catalog namespaces stay plaintext by policy through the same
    // allowlist the factory uses.
    let at_rest_crypto: std::sync::Arc<dyn fold_db::crypto::CryptoProvider> = std::sync::Arc::new(
        fold_db::crypto::LocalCryptoProvider::from_key(e2e.encryption_key()),
    );
    let namespaced: std::sync::Arc<dyn fold_db::storage::NamespacedStore> = std::sync::Arc::new(
        fold_db::storage::EncryptingNamespacedStore::with_plaintext_namespaces(
            std::sync::Arc::clone(&store) as _,
            at_rest_crypto,
            fold_db::storage::LASTSTORE_PLAINTEXT_NAMESPACES
                .iter()
                .copied()
                .map(str::to_string)
                .collect(),
        ),
    );
    let signer = std::sync::Arc::new(
        fold_db::security::Ed25519KeyPair::from_secret_key(&seed).map_err(|e| {
            RestoreFailure::new(
                Stage::RuntimeInit,
                Code::OperationFailed,
                format!("restore signer from identity seed: {e}"),
            )
        })?,
    );
    let mut engine = fold_db::sync::SyncEngine::new_with_laststore_backup_source(
        "lastdb-restore".to_string(),
        sync_crypto,
        s3.clone(),
        auth.clone(),
        namespaced,
        fold_db::sync::SyncConfig {
            capture_mode: fold_db::sync::engine::CaptureMode::MutationLog,
            legacy_personal_cloud_sync: false,
            ..fold_db::sync::SyncConfig::default()
        },
        std::sync::Arc::clone(&signer),
        Some(std::sync::Arc::clone(&store)),
    );
    // The photograph and download-cursor planes carry on-disk `ENC:` envelopes
    // already, so they stay on the raw store and are not sealed twice. Without
    // this the engine store above would become the cursor store by default.
    engine.set_cursor_store(std::sync::Arc::clone(&store) as _);
    // Cursor bookkeeping is sealed under the portable content key before it
    // reaches the local seam, and legacy `ENC:` replay values are unwrapped
    // with it. Same key the daemon passes.
    engine.set_at_rest_key(e2e.encryption_key());
    let engine = std::sync::Arc::new(engine);
    let data_path_str = data_path
        .to_str()
        .ok_or_else(|| {
            RestoreFailure::new(
                Stage::RuntimeInit,
                Code::OperationFailed,
                format!("restore data path is not UTF-8: {}", data_path.display()),
            )
        })?
        .to_string();
    let namespaced_for_db: std::sync::Arc<dyn fold_db::storage::NamespacedStore> =
        std::sync::Arc::clone(&store) as _;
    let (report, restore_mode) = runtime.block_on(async {
        // Restore calls explicit cloud read methods. Arm the upload interlock
        // before the first one. The later FoldDB shutdown still runs its normal
        // final sync, but that cycle exits before lock, register, PUT, CAS, or
        // DELETE. Normal daemon engines keep their default Cloud On state.
        engine.set_cloud_sync_disabled(true).await;
        if let Some(recovery) = &remote_descriptor {
            if let Some(expected) = &recovery.latest {
                let current = auth.backup_latest_get().await.map_err(|error| {
                    RestoreFailure::from_sync(
                        Stage::RestoreS0LatestPointer,
                        "read normal latest before restore",
                        &error,
                    )
                })?;
                if current.latest != expected.latest || current.key != expected.key {
                    return Err(RestoreFailure::new(
                        Stage::SourceScope,
                        Code::InvalidSourceScope,
                        "normal backup latest changed before restore",
                    ));
                }
            }
        }
        if let Some(recovery) = &remote_descriptor {
            if let Some(expected_rescue) = &recovery.rescue {
                let unscoped = auth.clone().with_db_hash(None).without_db_auto_claim();
                let rescue = unscoped
                    .rescue_s0_get(&expected_rescue.manifest_sha256)
                    .await
                    .map_err(|error| {
                        RestoreFailure::from_sync(
                            Stage::RestoreS0LatestPointer,
                            "read S0 rescue cut before restore",
                            &error,
                        )
                    })?;
                if rescue != *expected_rescue {
                    return Err(RestoreFailure::new(
                        Stage::SourceScope,
                        Code::InvalidSourceScope,
                        "S0 rescue cut changed before restore",
                    ));
                }
            }
        }
        if let Some(receipt) = &paused_receipt {
            let latest = auth.backup_latest_get().await.map_err(|error| {
                RestoreFailure::from_sync(
                    Stage::SourceScope,
                    "read latest backup for paused source",
                    &error,
                )
            })?;
            if latest.latest.manifest_sha256 != receipt.manifest_sha256
                || latest.latest.counter != receipt.manifest_counter
            {
                return Err(RestoreFailure::new(
                    Stage::SourceScope,
                    Code::InvalidSourceScope,
                    "paused source receipt does not match the latest cloud backup",
                ));
            }
        }
        let mut report = if let Some(report) = resume_report {
            store.verify_integrity().map_err(|error| {
                RestoreFailure::new(
                    Stage::Preflight,
                    Code::OperationFailed,
                    format!("restore checkpoint integrity check failed: {error}"),
                )
            })?;
            report
        } else {
            let report = if let Some(recovery) = &remote_descriptor {
                if let Some(rescue) = &recovery.rescue {
                    fold_db::sync::engine::restore_laststore_cloud_backup_from_rescue_with_cache(
                        &auth,
                        &s3,
                        store.as_ref(),
                        rescue,
                        progress,
                    )
                    .await
                } else {
                    fold_db::sync::engine::restore_laststore_cloud_backup_from_latest_pointer(
                        &auth,
                        &s3,
                        store.as_ref(),
                        recovery.latest.as_ref().expect("normal latest was checked"),
                        progress,
                    )
                    .await
                }
            } else {
                fold_db::sync::engine::restore_laststore_cloud_backup_with_cache(
                    &auth,
                    &s3,
                    store.as_ref(),
                    progress,
                    cache.as_ref(),
                )
                .await
            }
            .map_err(|e| RestoreFailure::from_s0("restore LastStore S0 backup", &e))?;
            if !remote_only {
                restore_checkpoint::save(
                    &target_home,
                    &source_db_hash,
                    &report,
                    restore_checkpoint::capture_files(
                        &target_home,
                        store.as_ref(),
                        report.chunks_installed,
                    )
                    .map_err(|detail| {
                        RestoreFailure::new(Stage::CompletionMarker, Code::IoError, detail)
                    })?,
                )
                .map_err(|detail| {
                    RestoreFailure::new(Stage::CompletionMarker, Code::IoError, detail)
                })?;
            }
            report
        };
        if let Some(recovery) = &remote_descriptor {
            if report.manifest_sha256 != recovery.descriptor.manifest_sha256
                || report.counter != recovery.descriptor.counter
            {
                return Err(RestoreFailure::new(
                    Stage::SourceScope,
                    Code::InvalidSourceScope,
                    "installed backup does not match the recovery descriptor",
                ));
            }
        }
        if let Some(receipt) = &paused_receipt {
            if report.manifest_sha256 != receipt.manifest_sha256
                || report.counter != receipt.manifest_counter
            {
                return Err(RestoreFailure::new(
                    Stage::SourceScope,
                    Code::InvalidSourceScope,
                    "installed backup does not match the paused source receipt",
                ));
            }
        }
        let (frontier, stored_mode) =
            engine
                .restored_backup_marker_and_frontier()
                .await
                .map_err(|e| {
                    RestoreFailure::from_sync(
                        Stage::RestoreTail,
                        "read restored snapshot writer frontier and restore mode",
                        &e,
                    )
                })?;
        report.mutation_log_snapshot_frontier = Some(frontier.clone());
        if remote_latest
            && stored_mode != Some(fold_db::sync::engine::BackupRestoreMode::ReplayTail)
        {
            return Err(RestoreFailure::new(
                Stage::RestoreTail,
                Code::OperationFailed,
                "normal remote backup needs the authenticated replay-tail restore marker",
            ));
        }
        // An offline rescue published before the in-store marker existed still
        // has an authenticated S0-only descriptor and an exact immutable root
        // pointer. The remote restore has already verified all chunks and
        // committed the destination. Only an absent marker may use that proof.
        let mode = match stored_mode {
            Some(mode) => mode,
            None if remote_s0_only
                && remote_descriptor.is_some()
                && report.source_scope_verified
                && report.manifests_walked == 1 =>
            {
                fold_db::sync::engine::BackupRestoreMode::S0Only
            }
            None => fold_db::sync::engine::BackupRestoreMode::ReplayTail,
        };
        if mode == fold_db::sync::engine::BackupRestoreMode::S0Only {
            report.remote_read_only = true;
            return Ok::<_, RestoreFailure>((report, mode));
        }
        if remote_s0_only {
            return Err(RestoreFailure::new(
                Stage::RestoreTail,
                Code::OperationFailed,
                "remote recovery requires the authenticated v2 S0-only restore marker",
            ));
        }
        if paused_source && !remote_latest {
            return Err(RestoreFailure::new(
                Stage::RestoreTail,
                Code::OperationFailed,
                "paused source backup lacks the S0-only restore marker",
            ));
        }
        // Restore is a one-shot CLI. Plist LASTDB_RESIDENT_MODE=write parks
        // replayed molecules in the resident graph; without a long-lived
        // persist worker they never land in atoms/tips (2026-08-21: 715
        // records_applied, dest HashRangeRange empty, only sync_pin_log
        // files were new). Force LastStore puts + per-batch flush.
        std::env::set_var("LASTDB_RESIDENT_MODE", "off");
        std::env::set_var("LASTDB_MUTATION_SYNC_FLUSH", "1");
        if let Some(progress) = progress {
            progress.phase(fold_db::sync::engine::RestorePhase::OpenDatabase);
        }
        let fold_db = fold_db::FoldDB::for_restore(namespaced_for_db, &data_path_str, signer, &e2e)
            .await
            .map_err(|e| {
                RestoreFailure::new(
                    Stage::OpenRestoreDatabase,
                    Code::OperationFailed,
                    format!("FoldDB for restore apply: {e}"),
                )
            })?;
        fold_db
            .set_sync_engine(std::sync::Arc::clone(&engine))
            .await;
        let replay = engine
            .restore_mutation_log_after_s0_with_progress(&frontier, progress)
            .await
            .map_err(|e| {
                RestoreFailure::from_sync(Stage::RestoreTail, "restore mutation log after S0", &e)
            })?;
        report.mutation_log_replay = Some(replay);
        // Memory-first mutations + resident persist sit in RAM until shutdown.
        if let Some(progress) = progress {
            progress.phase(fold_db::sync::engine::RestorePhase::Flush);
        }
        fold_db.shutdown().await.map_err(|e| {
            RestoreFailure::new(
                Stage::FlushDestination,
                Code::OperationFailed,
                format!("shutdown dest after mutation-log apply: {e}"),
            )
        })?;
        report.remote_read_only = true;
        Ok::<_, RestoreFailure>((report, mode))
    })?;
    write_owner_only_local(
        &target_home.join(lastdb_node::cloud::BOOTSTRAP_DONE_FILE),
        b"ok\n",
    )
    .map_err(|detail| {
        RestoreFailure::new(
            Stage::CompletionMarker,
            Code::IoError,
            format!(
                "write {} after LastStore restore: {detail}",
                target_home
                    .join(lastdb_node::cloud::BOOTSTRAP_DONE_FILE)
                    .display()
            ),
        )
    })?;
    if restore_mode == fold_db::sync::engine::BackupRestoreMode::ReplayTail && !remote_latest {
        lastdb_node::cloud::clear_cloud_resume_required(&target_home).map_err(|detail| {
            RestoreFailure::new(Stage::CompletionMarker, Code::IoError, detail)
        })?;
        lastdb_node::cloud::resume_cloud_sync_file(&target_home).map_err(|detail| {
            RestoreFailure::new(Stage::CompletionMarker, Code::IoError, detail)
        })?;
    }

    let remote_ready = if remote_only {
        let expected_mode = if remote_s0_only {
            fold_db::sync::engine::BackupRestoreMode::S0Only
        } else {
            fold_db::sync::engine::BackupRestoreMode::ReplayTail
        };
        if restore_mode != expected_mode
            || !report.remote_read_only
            || !report.source_scope_verified
            || !target_paused_cloud.is_file()
            || !lastdb_node::cloud::cloud_resume_required_path(&target_home).is_file()
            || target_active_cloud.exists()
            || !target_home
                .join(lastdb_node::cloud::BOOTSTRAP_DONE_FILE)
                .is_file()
        {
            return Err(RestoreFailure::new(
                Stage::CompletionMarker,
                Code::OperationFailed,
                "remote restore did not leave a complete Cloud Off target",
            ));
        }
        let recovery = remote_descriptor
            .as_ref()
            .expect("remote recovery was checked");
        let (ready_file, mode_name) = if remote_s0_only {
            (RESCUE_S0_RESTORE_READY_FILE, "s0_only")
        } else {
            (NORMAL_LATEST_RESTORE_READY_FILE, "replay_tail")
        };
        let ready = serde_json::json!({
            "version": 1,
            "ok": true,
            "db_hash": source_db_hash,
            "store_uuid": recovery.descriptor.store_uuid,
            "manifest_sha256": report.manifest_sha256,
            "counter": report.counter,
            "restore_mode": mode_name,
            "cloud_sync_off": true,
        });
        let bytes = serde_json::to_vec_pretty(&ready).map_err(|error| {
            RestoreFailure::new(
                Stage::CompletionMarker,
                Code::SerializationError,
                format!("encode remote restore record: {error}"),
            )
        })?;
        write_owner_only_local(&target_home.join(ready_file), &bytes).map_err(|detail| {
            RestoreFailure::new(Stage::CompletionMarker, Code::IoError, detail)
        })?;
        std::fs::File::open(&target_home)
            .and_then(|dir| dir.sync_all())
            .map_err(|error| {
                RestoreFailure::new(
                    Stage::CompletionMarker,
                    Code::IoError,
                    format!("sync remote restore home: {error}"),
                )
            })?;
        Some(ready)
    } else {
        None
    };

    if json_only {
        let mut report_json = serde_json::to_value(&report).map_err(|error| {
            RestoreFailure::new(
                Stage::RenderReport,
                Code::SerializationError,
                format!("encode report: {error}"),
            )
        })?;
        if let Some(serde_json::Value::Object(ready)) = remote_ready {
            report_json
                .as_object_mut()
                .expect("restore report is an object")
                .extend(ready);
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&report_json).map_err(|e| {
                RestoreFailure::new(
                    Stage::RenderReport,
                    Code::SerializationError,
                    format!("encode report: {e}"),
                )
            })?
        );
    } else {
        println!("Restored LastStore backup into {}", target_home.display());
        println!("  manifest: {}", report.manifest_sha256);
        println!("  counter:  {}", report.counter);
        println!("  cut_csn:  {}", report.cut_csn);
        println!("  chunks:   {}", report.chunks_installed);
        println!("  bytes:    {}", format_bytes(report.bytes_installed));
        println!("  epoch:    {}", report.restored_epoch);
        println!("  source scope verified: {}", report.source_scope_verified);
        println!("  remote read-only:      {}", report.remote_read_only);
        if restore_mode == fold_db::sync::engine::BackupRestoreMode::S0Only {
            println!("  cloud mutation tail:  skipped");
            println!("  Cloud Sync:           Off");
        } else if remote_latest {
            println!("  Cloud Sync:           Off");
        }
        if let Some(ml) = &report.mutation_log_replay {
            println!(
                "  mutation-log: considered={} applied={} records={}",
                ml.segments_considered, ml.segments_applied, ml.records_applied
            );
        }
    }
    Ok(())
}

#[path = "restore_guards.rs"]
mod restore_guards;
pub(crate) use restore_guards::*;
#[path = "restore_remote_discovery.rs"]
mod restore_remote_discovery;
pub(crate) use restore_remote_discovery::*;
