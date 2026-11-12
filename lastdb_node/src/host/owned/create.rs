//! Create one owned store and register it after a successful open.

use super::*;

impl Tables {
    pub(crate) async fn create(
        &self,
        home: &Path,
        signer: Arc<Ed25519KeyPair>,
        name: &str,
        kind: Kind,
        spec: Option<SpecBinding>,
    ) -> Result<Entry, OwnedError> {
        if kind == Kind::Personal && spec.is_some() {
            return Err(OwnedError::InvalidSpec);
        }
        validate_create(name, spec.as_ref())?;
        let id = self.allocate(name)?;
        self.create_registered(
            home,
            signer,
            Entry {
                id,
                name: Some(name.to_owned()),
                kind,
                spec,
            },
        )
        .await
    }

    /// Register the cloud id in an isolated fresh recovery home.
    pub(crate) async fn register_recovered(
        &self,
        home: &Path,
        signer: Arc<Ed25519KeyPair>,
        name: &str,
        id: &cloud_backup_protocol::Id,
    ) -> Result<Entry, OwnedError> {
        validate_create(name, None)?;
        if !matches!(
            self.registered_store(id.as_str())?,
            (Resolved::Absent, None)
        ) {
            return Err(OwnedError::AlreadyRegistered);
        }
        self.create_registered(
            home,
            signer,
            Entry {
                id: id.as_str().to_owned(),
                name: Some(name.to_owned()),
                kind: Kind::Personal,
                spec: None,
            },
        )
        .await
    }

    async fn create_registered(
        &self,
        home: &Path,
        signer: Arc<Ed25519KeyPair>,
        entry: Entry,
    ) -> Result<Entry, OwnedError> {
        let id = &entry.id;
        let store = store::open(home, id, signer, true)
            .await
            .map_err(|_| OwnedError::Unavailable)?;
        if let Err(error) = self.commit_create(home, entry.clone(), Arc::clone(&store)) {
            if !error.renamed {
                let _ = store.shutdown().await;
                let _ = store::remove(home, id);
            }
            return Err(error.kind);
        }
        self.changed.send_modify(|generation| {
            *generation = generation.saturating_add(1);
        });
        Ok(entry)
    }
}
