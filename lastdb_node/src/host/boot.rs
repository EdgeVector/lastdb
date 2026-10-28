use super::*;

impl Host {
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
        if crate::cloud::cloud_credentials_path(home).exists() {
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
            // ↔ E2E content/index keys. Print once so cloud enroll later is
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

    /// Open native local storage without a legacy cloud or replay engine.
    pub(super) async fn open_boot_db(
        home: &Path,
        data_path: &Path,
        existing_store: bool,
        keypair: &Arc<Ed25519KeyPair>,
        e2e_keys: &E2eKeys,
        search_outbox_inbox: PathBuf,
    ) -> Result<Arc<FoldDB>, String> {
        let config = DatabaseConfig {
            path: data_path.to_path_buf(),
            engine: StorageEngine::Laststore,
        };
        fold_db::fold_db_core::factory::create_fold_db_with_options(
            &config,
            e2e_keys,
            Arc::clone(keypair),
            fold_db::fold_db_core::factory::FoldDbOptions {
                offline_maintenance: false,
                at_rest_keyring: None,
                search_outbox_inbox: Some(search_outbox_inbox),
            },
        )
        .await
        .map_err(|e| map_and_record_boot_error(home, existing_store, e))
    }
}
