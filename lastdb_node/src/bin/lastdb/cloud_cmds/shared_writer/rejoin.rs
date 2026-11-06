use super::Owner;
use fold_db::crypto::E2eKeys;
use fold_db::FoldDB;
use lastdb_node::shared_restore::{prepare_owner_rejoin, OwnerRejoin, PreparedRejoin};
use std::path::Path;
use std::sync::Arc;
use zeroize::Zeroizing;

impl Owner {
    pub(super) async fn rejoin(
        &self,
        db: Arc<FoldDB>,
        store: &Path,
        e2e: &E2eKeys,
        batches: u16,
    ) -> Result<(), String> {
        let candidate = match self.prepare_rejoin(&db, store, e2e, batches).await {
            Ok(candidate) => candidate,
            Err(error) => {
                db.shutdown().await.map_err(|shutdown| {
                    format!("{error}; cannot close shared writer home: {shutdown}")
                })?;
                return Err(error);
            }
        };
        let previous = candidate
            .activate(db)
            .await
            .map_err(|error| error.to_string())?;
        println!(
            "Shared writer rejoined. Previous data is at {}",
            previous.display()
        );
        Ok(())
    }

    async fn prepare_rejoin(
        &self,
        db: &FoldDB,
        store: &Path,
        e2e: &E2eKeys,
        batches: u16,
    ) -> Result<PreparedRejoin, String> {
        db.db_ops()
            .native_runtime()
            .require_direct_shared_home()
            .map_err(|_| "returning writer requires a dedicated shared home")?;
        let native = db.mutation_manager();
        let (stream, _) = native
            .portable_writer_progress()
            .await
            .map_err(string_error)?;
        self.check_writer_enrollment(&stream, false).await?;
        let content_key = Zeroizing::new(e2e.encryption_key());
        self.push(db, store, &content_key, batches).await?;

        let page = native.portable_outgoing().await.map_err(string_error)?;
        if page.stream != stream || !page.entries.is_empty() {
            return Err("shared writer still has pending mutations; push them and retry".into());
        }
        prepare_owner_rejoin(OwnerRejoin {
            auth: self.auth.clone(),
            device: &self.device,
            signer: self.device_signer.clone(),
            stream: &stream,
            confirmed_head: page.uploaded_head.as_ref(),
            seed: Zeroizing::new(self.account_signer.secret_key_bytes()),
            source_data: store,
            source: db,
        })
        .await
        .map_err(string_error)
    }
}

fn string_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

pub(super) fn require_source_data(store: &Path) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;

    lastdb_node::offline_home::refuse_primary(store)?;
    let info = std::fs::symlink_metadata(store).map_err(string_error)?;
    if !info.is_dir() || info.uid() != unsafe { libc::geteuid() } || info.mode() & 0o022 != 0 {
        return Err("returning writer requires an owned data directory".into());
    }
    Ok(())
}
