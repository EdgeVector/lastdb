use super::*;

impl Host {
    /// Record, at boot, what the home knows about peer data and reverse edges.
    ///
    /// `brand_new` is the one boot that created the home: it minted the identity
    /// over a store that did not exist. A restored identity or an existing store
    /// never stamps. The stamps are best effort and cannot fail boot. A home a
    /// restore finished (the `.peer_derived` marker) holds data it did not
    /// author: that is recorded durably, and a failure to record it fails boot.
    pub(super) async fn record_boot_peer_state(
        db: &fold_db::FoldDB,
        home: &Path,
        brand_new: bool,
    ) -> Result<(), String> {
        if brand_new {
            // First: the fresh-home stamp writes `aref:` rows, which the
            // emptiness proof of the peer-apply stamp would then see. A home
            // this boot created has applied no peer mutation yet; every other
            // home stays `unknown`.
            db.stamp_peer_apply_never_on_new_home().await;
            db.stamp_fresh_home_atom_ref_markers().await;
        }
        if crate::cloud::peer_derived_marker_path(home).exists() {
            db.mark_peer_apply_seen()
                .await
                .map_err(|error| format!("record the restored home's peer-apply state: {error}"))?;
        }
        Ok(())
    }

    /// Create the node home and data dir, refuse a stopped backup copy, and
    /// clear the prior shutdown proof. Returns the data dir.
    pub(super) fn prepare_boot_home(home: &Path) -> Result<PathBuf, String> {
        ensure_node_home(home)
            .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
        if crate::cloud::cloud_backup_source_copy_path(home).exists() {
            return Err("a stopped backup copy cannot boot as a daemon".into());
        }
        crate::session_ledger::clear_shutdown_flush_receipt(home)
            .map_err(|error| format!("could not clear prior shutdown proof: {error}"))?;
        if crate::cloud::cloud_sync_file_state(home) == "off" {
            crate::cloud::mark_cloud_resume_required(home)?;
        }
        let data_path = home.join("data");
        std::fs::create_dir_all(&data_path)
            .map_err(|e| format!("failed to create data dir {}: {e}", data_path.display()))?;
        Ok(data_path)
    }

    /// Load (or generate) the identity seed and derive the node and E2E keys.
    /// The flag is true when this boot minted the identity.
    pub(super) fn load_boot_identity(
        home: &Path,
    ) -> Result<(Arc<Ed25519KeyPair>, E2eKeys, bool), String> {
        let (seed, freshly_generated) = load_or_generate_seed_with_meta(home)?;
        if freshly_generated {
            // Same BIP39 path as `lastdbd connect` invite: phrase ↔ identity.key
            // ↔ E2E content/index/ope keys. Print once so cloud enroll later is
            // restore-from-phrase, not a second local encode dance.
            match bip39::Mnemonic::from_entropy(&seed) {
                Ok(mnemonic) => {
                    emit_fresh_recovery_phrase(&mnemonic.to_string());
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "fresh identity written but BIP39 phrase could not be encoded"
                    );
                }
            }
        }
        let keypair = Arc::new(
            Ed25519KeyPair::from_secret_key(&seed)
                .map_err(|e| format!("identity keyfile is not a valid Ed25519 seed: {e}"))?,
        );
        let e2e_keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|e| format!("E2E key derivation failed: {e}"))?;
        Ok((keypair, e2e_keys, freshly_generated))
    }

    /// Open the FoldDB, with cloud sync wired when `cloud_sync.json` exists.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn open_boot_db(
        home: &Path,
        data_path: &Path,
        existing_store: bool,
        keypair: &Arc<Ed25519KeyPair>,
        e2e_keys: &E2eKeys,
        cloud_sync: Option<CloudSyncConfig>,
        search_outbox_inbox: PathBuf,
        resume_required: bool,
    ) -> Result<Arc<FoldDB>, String> {
        // The disabled local proof route refuses before database construction.
        if cloud_sync.is_none() && crate::proof_replay::boot_engine_requested(home) {
            return Self::open_proof_boot_db(
                home,
                data_path,
                existing_store,
                keypair,
                e2e_keys,
                search_outbox_inbox,
            );
        }
        // Last Store only. Legacy sled personal bootstrap is gone — restore
        // uses `lastdb restore --into`. Auth-refresh still wires 401 self-heal
        // when cloud_sync.json is present.
        let storage_engine = StorageEngine::from_env()
            .map_err(|e| map_and_record_boot_error(home, existing_store, e))?
            .unwrap_or(StorageEngine::Laststore);

        Ok(if let Some(cloud) = cloud_sync {
            tracing::info!(
                target: "lastdbd::cloud",
                "LastStore boot: skipping legacy personal bootstrap; restore uses `lastdb restore --into`"
            );

            let refresh = crate::cloud::auth_refresh_callback(
                home.to_path_buf(),
                cloud.api_url.clone(),
                Arc::clone(keypair),
            );
            let cloud = load_cloud_sync(home)?.unwrap_or(cloud);
            let config = DatabaseConfig {
                path: data_path.to_path_buf(),
                engine: storage_engine,
                cloud_sync: Some(cloud),
            };
            // Product rule: Mini personal at-rest == account E2E content key
            // (portable every device). Never pass a store keyring DEK here.
            fold_db::fold_db_core::factory::create_fold_db_with_pool_auth_refresh_and_search_outbox(
                &config,
                e2e_keys,
                Arc::clone(keypair),
                Some(refresh),
                None, // at_rest_keyring: Mini = e2e_content_key only
                Some(search_outbox_inbox),
                resume_required,
            )
            .await
            .map_err(|e| map_and_record_boot_error(home, existing_store, e))?
        } else {
            let config = DatabaseConfig {
                path: data_path.to_path_buf(),
                engine: storage_engine,
                cloud_sync: None,
            };
            // Local-only Mini path also uses content-key at-rest (no keyring).
            fold_db::fold_db_core::factory::create_fold_db_with_pool_auth_refresh_and_search_outbox(
                &config,
                e2e_keys,
                Arc::clone(keypair),
                None,
                None,
                Some(search_outbox_inbox),
                false,
            )
            .await
            .map_err(|e| map_and_record_boot_error(home, existing_store, e))?
        })
    }

    /// Refuse the disabled local proof engine after the proof guard.
    pub(super) fn open_proof_boot_db(
        home: &Path,
        data_path: &Path,
        existing_store: bool,
        keypair: &Arc<Ed25519KeyPair>,
        e2e_keys: &E2eKeys,
        search_outbox_inbox: PathBuf,
    ) -> Result<Arc<FoldDB>, String> {
        let storage_engine = StorageEngine::from_env()
            .map_err(|e| map_and_record_boot_error(home, existing_store, e))?
            .unwrap_or(StorageEngine::Laststore);
        let config = DatabaseConfig {
            path: data_path.to_path_buf(),
            engine: storage_engine,
            cloud_sync: None,
        };
        fold_db::fold_db_core::factory::create_fold_db_with_local_proof_engine(
            &config,
            e2e_keys,
            Arc::clone(keypair),
            Some(search_outbox_inbox),
        )
        .map_err(|e| map_and_record_boot_error(home, existing_store, e))
    }
}
