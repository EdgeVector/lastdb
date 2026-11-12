//! Rebind the personal owner id to the cloud database that sealed with that key.

use std::path::Path;

use super::{registry, snapshot, Kind, OwnedError, Tables};

impl Tables {
    /// Point the personal row at a restored cloud id. The store stays `<home>/data`.
    pub(crate) fn adopt_owner_id(&self, home: &Path, database_id: &str) -> Result<(), OwnedError> {
        if cloud_backup_protocol::Id::try_from(database_id.to_owned()).is_err() {
            return Err(OwnedError::InvalidName);
        }
        let mut state = self.write()?;
        if !personal_owner(&state.registry_owner) {
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

fn personal_owner(owner: &super::Entry) -> bool {
    owner.kind == Kind::Personal
        && owner.name.as_deref() == Some("personal")
        && owner.spec.is_none()
}
