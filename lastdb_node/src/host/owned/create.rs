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
        requested_id: Option<uuid::Uuid>,
    ) -> Result<Entry, OwnedError> {
        if kind == Kind::Personal && spec.is_some() {
            return Err(OwnedError::InvalidSpec);
        }
        validate_create(name, spec.as_ref())?;
        let _creating = self.creating.lock().await;
        let id = match requested_id {
            Some(id) => id.to_string(),
            None => self.allocate(name)?,
        };
        let entry = Entry {
            id,
            name: Some(name.to_owned()),
            kind,
            spec,
        };
        if requested_id.is_some() {
            let state = self.read()?;
            if let Some(existing) = state.owned.iter().find(|row| row.id == entry.id) {
                return if existing == &entry {
                    Ok(entry)
                } else {
                    Err(OwnedError::AlreadyRegistered)
                };
            }
            ensure_free(&state, name)?;
            if state.registry_owner.id == entry.id {
                return Err(OwnedError::AlreadyRegistered);
            }
        }
        self.create_registered(home, signer, entry).await
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
        let store = store::open(home, &entry, signer, true)
            .await
            .map_err(|_| OwnedError::Unavailable)?;
        if let Err(error) = self.commit_create(home, entry.clone(), Arc::clone(&store)) {
            if !error.renamed {
                let _ = store.shutdown().await;
                let _ = store::remove(home, id);
            }
            return Err(error.kind);
        }
        Ok(entry)
    }

    /// Point the personal row at a restored cloud id. The store stays `<home>/data`.
    pub(crate) fn adopt_owner_id(&self, home: &Path, database_id: &str) -> Result<(), OwnedError> {
        if cloud_backup_protocol::Id::try_from(database_id.to_owned()).is_err() {
            return Err(OwnedError::InvalidName);
        }
        let mut state = self.write()?;
        if state.registry_owner.kind != Kind::Personal
            || state.registry_owner.name.as_deref() != Some("personal")
            || state.registry_owner.spec.is_some()
        {
            return Err(OwnedError::Unavailable);
        }
        if state.registry_owner.id == database_id {
            return Ok(());
        }
        if state.stores.contains_key(database_id)
            || state.owned.iter().any(|entry| entry.id == database_id)
        {
            return Err(OwnedError::AlreadyRegistered);
        }
        let previous = state.registry_owner.id.clone();
        state.registry_owner.id = database_id.to_owned();
        if let Err(error) = registry::write(home, &snapshot(&state)) {
            if !error.renamed {
                state.registry_owner.id = previous;
            }
            return Err(OwnedError::Unavailable);
        }
        Ok(())
    }
}
