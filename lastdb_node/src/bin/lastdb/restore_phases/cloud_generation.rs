//! Source-free restore from the permanent, authenticated cloud generation.

use super::*;
use fold_db::sync::cloud_purge::protocol::{
    CloudAccount, CloudAccountKind, CloudControlGet, CloudPurgeControl, CloudPurgePhase,
    CloudReplacementManifest, CloudReplacementRoot,
};
use fold_db::sync::cloud_purge_peer::CloudPurgePeer;
use std::collections::BTreeMap;
use std::sync::Arc;

#[path = "cloud_generation/legacy.rs"]
mod legacy;
pub(crate) use legacy::{
    bind_generation_reader, reset_continuation_seed, verify_legacy_continuation,
};

struct Selection {
    current: CloudControlGet,
    ready: CloudPurgeControl,
    replacement: CloudReplacementManifest,
    root: CloudReplacementRoot,
}

struct Reader {
    engine: Engine,
    store: Arc<fold_db::storage::LastStoreNamespacedStore>,
    cleanup: String,
}

pub(crate) fn try_restore_cloud_generation(
    homes: &Homes,
    source: &mut Source,
    identity: &Identity,
    flavor: &Flavor<'_>,
    json_only: bool,
    progress: Option<&fold_db::sync::engine::RestoreProgress>,
) -> Phase<bool> {
    if !flavor.remote_latest {
        return Ok(false);
    }
    let clients = build_cloud_clients(identity, "")?;
    let signer = fold_db::security::Ed25519KeyPair::from_secret_key(&identity.seed)
        .map_err(|_| scope_failure("the recovery identity is invalid"))?;
    let account = CloudAccount {
        kind: CloudAccountKind::Personal,
        id: lastdb_node::host::user_hash_from_pubkey(&signer.public_key_base64())
            .map_err(scope_failure)?,
    };
    clients.runtime.block_on(async {
        let Some(selected) = select(&clients.auth, &account, flavor, source).await? else {
            return Ok(false);
        };
        let report = restore(homes, source, identity, &clients, selected, progress).await?;
        if json_only {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .map_err(|_| runtime_failure("encode cloud generation restore report"))?
            );
        } else {
            println!(
                "Restored cloud generation {} into {}",
                report["data_generation"],
                homes.target.display()
            );
            println!("  Cloud Sync: Off");
        }
        Ok(true)
    })
}

async fn select(
    auth: &fold_db::sync::auth::AuthClient,
    account: &CloudAccount,
    flavor: &Flavor<'_>,
    source: &mut Source,
) -> Phase<Option<Selection>> {
    let current = auth.cloud_control_get(account).await.map_err(|error| {
        RestoreFailure::from_sync(
            Stage::SourceScope,
            "read permanent cloud recovery control",
            &error,
        )
    })?;
    if current.notification_only || current.account.as_ref() != Some(account) {
        return Err(scope_failure(
            "cloud recovery control does not authorize this account",
        ));
    }
    if !current.protocol_required {
        if current.generation != 0 || current.control.is_some() {
            return Err(scope_failure("invalid legacy cloud recovery control"));
        }
        return Ok(None);
    }
    let control = current
        .control
        .as_ref()
        .ok_or_else(|| scope_failure("cloud recovery control is absent"))?;
    control.validate_identity().map_err(scope_failure)?;
    if control.account != *account || control.generation != current.generation {
        return Err(scope_failure(
            "cloud recovery control identity differs from its account",
        ));
    }
    let ready = if control.phase == CloudPurgePhase::Ready && control.deletion_is_complete() {
        control.clone()
    } else if control.unchanged_continuation().is_some() && control.data_generation == Some(0) {
        source.recovery_control = Some(current);
        return Ok(None);
    } else if control.unchanged_continuation().is_some() {
        serde_json::from_value(control.previous_ready_control.clone().ok_or_else(|| {
            scope_failure("the cancelled generation has no verified prior Ready seed")
        })?)
        .map_err(|_| scope_failure("the prior Ready seed is invalid"))?
    } else {
        return Err(scope_failure(
            "the cloud generation is held; recovery cannot publish a completed home",
        ));
    };
    let replacement: CloudReplacementManifest = serde_json::from_value(
        ready
            .replacement_manifest
            .clone()
            .ok_or_else(|| scope_failure("the Ready replacement inventory is absent"))?,
    )
    .map_err(|_| scope_failure("the Ready replacement inventory is invalid"))?;
    let roots: Vec<_> = replacement
        .roots
        .iter()
        .filter(|root| {
            root.format == "laststore_v1"
                && flavor
                    .selection
                    .db_hash
                    .is_none_or(|hash| hash == root.logical_root)
                && flavor
                    .selection
                    .manifest_sha256
                    .is_none_or(|sha| sha == root.manifest_sha256)
        })
        .collect();
    if roots.len() != 1 {
        return Err(scope_failure(
            "select one exact Ready LastStore root with --db-hash and --manifest-sha256",
        ));
    }
    Ok(Some(Selection {
        root: roots[0].clone(),
        current,
        ready,
        replacement,
    }))
}

async fn restore(
    homes: &Homes,
    source: &Source,
    identity: &Identity,
    clients: &CloudClients,
    selected: Selection,
    progress: Option<&fold_db::sync::engine::RestoreProgress>,
) -> Phase<serde_json::Value> {
    let empty_data = prepare_destination(homes, source, identity)?;
    std::fs::remove_dir(&empty_data).map_err(|_| {
        io_failure(
            Stage::PrepareDestination,
            "the new recovery data directory is not empty",
        )
    })?;
    let crypto = fold_db::crypto::LocalCryptoProvider::from_key(identity.e2e.encryption_key());
    let installed = fold_db::sync::cloud_purge::restore_cloud_purge_root_with_report(
        &clients.auth,
        &selected.ready,
        &selected.root,
        &homes.target,
        &crypto,
        &identity.e2e.encryption_key(),
    )
    .await
    .map_err(|error| {
        RestoreFailure::from_sync(
            Stage::RestoreS0LatestPointer,
            "restore exact cloud generation seed",
            &error,
        )
    })?;
    let reader = prepare_reader(homes, identity, clients, &selected, &installed).await?;
    let replay = replay(
        identity,
        &reader.engine,
        &reader.store,
        &selected.root,
        progress,
        &installed.data,
    )
    .await?;
    let after = clients
        .auth
        .cloud_control_get(&selected.ready.account)
        .await
        .map_err(|error| {
            RestoreFailure::from_sync(Stage::SourceScope, "recheck recovery generation", &error)
        })?;
    if after.notification_only
        || after.account != selected.current.account
        || !after.protocol_required
        || after.generation != selected.current.generation
        || after.control != selected.current.control
    {
        return Err(scope_failure(
            "the cloud recovery generation changed during restore",
        ));
    }
    finish(homes, &selected, &installed, &reader.cleanup, &replay)
}

async fn prepare_reader(
    homes: &Homes,
    identity: &Identity,
    clients: &CloudClients,
    selected: &Selection,
    installed: &fold_db::sync::cloud_purge::CloudPurgeRootRestore,
) -> Phase<Reader> {
    let source_id = fold_db::sync::get_or_create_device_id(&installed.data.to_string_lossy());
    let peer = CloudPurgePeer::open(
        CloudPurgePeer::directory_for_home(&homes.target)
            .map_err(|error| scope_failure(error.to_string()))?,
        source_id,
    )
    .map_err(|error| scope_failure(error.to_string()))?;
    peer.observe(&selected.current)
        .map_err(|error| scope_failure(error.to_string()))?;
    let plan = peer
        .prepare_adoption(&selected.ready)
        .map_err(|error| scope_failure(error.to_string()))?;
    let auth = clients
        .auth
        .for_cloud_adoption_reads(&plan)
        .with_db_hash(Some(selected.root.logical_root.clone()));
    let store = Arc::new(
        fold_db::storage::LastStoreNamespacedStore::open_with_data_key_and_high_water(
            &installed.data,
            identity.e2e.encryption_key(),
            homes.target.join("laststore_high_water.json"),
        )
        .map_err(|_| runtime_failure("open the exact restored LastStore layout"))?,
    );
    let engine = build_engine_with_auth(identity, clients, &store, &auth)?;
    engine.engine.arm_stopped_cloud_purge_interlock().await;
    let frontiers = frontiers(&selected.replacement, &selected.root)?;
    let cleanup = engine
        .engine
        .reset_fresh_cloud_replacement_seed(&frontiers)
        .await
        .map_err(|error| {
            RestoreFailure::from_sync(
                Stage::RestoreTail,
                "retire inherited selected-device queues",
                &error,
            )
        })?;
    engine
        .engine
        .begin_readonly_cloud_replacement_replay()
        .await
        .map_err(|error| {
            RestoreFailure::from_sync(
                Stage::RestoreTail,
                "arm read-only generation replay",
                &error,
            )
        })?;
    Ok(Reader {
        engine,
        store,
        cleanup,
    })
}

fn frontiers(
    replacement: &CloudReplacementManifest,
    main: &CloudReplacementRoot,
) -> Phase<BTreeMap<String, BTreeMap<String, u64>>> {
    let mut frontiers = BTreeMap::from([(String::new(), main.writer_frontier.clone())]);
    for root in replacement
        .roots
        .iter()
        .filter(|root| root.format == "logical_snapshot_v1")
    {
        if frontiers
            .insert(root.logical_root.clone(), root.writer_frontier.clone())
            .is_some()
        {
            return Err(scope_failure(
                "the Ready snapshot has duplicate target frontiers",
            ));
        }
    }
    Ok(frontiers)
}

async fn replay(
    identity: &Identity,
    engine: &Engine,
    store: &Arc<fold_db::storage::LastStoreNamespacedStore>,
    root: &CloudReplacementRoot,
    progress: Option<&fold_db::sync::engine::RestoreProgress>,
    data_path: &Path,
) -> Phase<fold_db::sync::engine::MutationLogReplayReport> {
    std::env::set_var("LASTDB_MUTATION_SYNC_FLUSH", "1");
    let data = data_path.to_string_lossy().to_string();
    let database = fold_db::FoldDB::for_restore(
        store.clone() as _,
        &data,
        Arc::clone(&engine.signer),
        &identity.e2e,
    )
    .await
    .map_err(|_| runtime_failure("open database for read-only generation replay"))?;
    database
        .set_sync_engine(Arc::clone(&engine.engine))
        .await
        .map_err(|error| runtime_failure(format!("attach generation replay engine: {error}")))?;
    let frontier =
        fold_db::sync::snapshot_log::Frontier::from_writer_hwm(root.writer_frontier.clone());
    let replay = engine
        .engine
        .restore_mutation_log_after_s0_with_progress(&frontier, progress)
        .await;
    let flush = database.shutdown_local_only().await;
    let replay = replay.map_err(|error| {
        RestoreFailure::from_sync(
            Stage::RestoreTail,
            "restore current generation tail",
            &error,
        )
    })?;
    flush.map_err(|_| io_failure(Stage::FlushDestination, "flush current generation restore"))?;
    store.verify_integrity().map_err(|error| {
        io_failure(
            Stage::FlushDestination,
            format!("integrity check after generation flush: {error}"),
        )
    })?;
    Ok(replay)
}

fn finish(
    homes: &Homes,
    selected: &Selection,
    installed: &fold_db::sync::cloud_purge::CloudPurgeRootRestore,
    cleanup: &str,
    replay: &fold_db::sync::engine::MutationLogReplayReport,
) -> Phase<serde_json::Value> {
    write_owner_only_local(
        &homes.target.join(lastdb_node::cloud::BOOTSTRAP_DONE_FILE),
        b"ok\n",
    )
    .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
    let report = serde_json::json!({"version":2,"ok":true,"account":selected.ready.account,
        "cloud_generation":selected.current.generation,"data_generation":selected.ready.generation,
        "operation_id":selected.ready.operation_id,"db_hash":selected.root.logical_root,
        "store_uuid":installed.manifest.store_uuid,"manifest_sha256":selected.root.manifest_sha256,
        "counter":installed.manifest.counter,"cut_csn":installed.manifest.cut_csn,
        "restored_epoch":installed.restored_epoch,"restore_mode":"cloud_generation_and_tail",
        "cloud_sync_off":true,"remote_read_only":true,"source_scope_verified":true,
        "queue_reset_sha256":cleanup,"mutation_log_snapshot_frontier":selected.root.writer_frontier,
        "mutation_log_replay":replay});
    let bytes = serde_json::to_vec_pretty(&report)
        .map_err(|_| runtime_failure("encode generation restore receipt"))?;
    write_owner_only_local(
        &homes.target.join(".cloud_generation_restore_ready"),
        &bytes,
    )
    .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
    std::fs::File::open(&homes.target)
        .and_then(|dir| dir.sync_all())
        .map_err(|_| io_failure(Stage::CompletionMarker, "flush generation restore receipt"))?;
    Ok(report)
}
