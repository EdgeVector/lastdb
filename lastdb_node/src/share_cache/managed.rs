//! Node-owned Share bindings on top of the registration cache.
use super::{
    files, publish, Lock, Registration, Result, ShareCache, ShareCacheError,
    MANAGED_SHARE_DIRECTORY,
};
use crate::registered_share::RegisteredShare;
use cloud_backup_protocol::{Digest, Id};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

const REGISTRY_LOCK: &str = "registry.lock";
const MAX_REGISTERED: usize = 32;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManagedRegistration {
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    spec: Option<crate::share_spec::PublicationSpec>,
    expires_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    grant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    grant_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    grant_expires_ms: Option<u64>,
    /// Named-database signed log. Publication shares omit this.
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) signed_log: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl ShareCache {
    pub(crate) fn open_managed(home: &Path, create: bool) -> Result<Option<Self>> {
        let root = home.join(MANAGED_SHARE_DIRECTORY);
        if !create
            && matches!(fs::symlink_metadata(root), Err(e) if e.kind() == ErrorKind::NotFound)
        {
            return Ok(None);
        }
        let home = home.canonicalize()?;
        let root = home.join(MANAGED_SHARE_DIRECTORY);
        let recipient = crate::share_invitation::recipient(&home)
            .map_err(|_| ShareCacheError::Invalid("recipient identity"))?;
        files::private_directory(&root, create)?;
        Ok(Some(Self { root, recipient }))
    }

    pub(crate) fn managed_ids(&self) -> Result<Vec<Id>> {
        let directory = files::private_directory(&self.root, false)?;
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let name = entry?
                .file_name()
                .into_string()
                .map_err(|_| ShareCacheError::Invalid("registry entry"))?;
            if name == REGISTRY_LOCK {
                continue;
            }
            if ids.len() == MAX_REGISTERED {
                return Err(ShareCacheError::Invalid("registry size"));
            }
            ids.push(Id::try_from(name).map_err(|_| ShareCacheError::Invalid("registry entry"))?);
        }
        directory
            .check()
            .map_err(|_| ShareCacheError::Invalid("registry directory"))?;
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(ids)
    }

    pub(crate) fn managed_record(&self, id: &Id) -> Result<Option<RegisteredShare>> {
        let entry = match self.entry(id, false) {
            Ok(entry) => entry,
            Err(ShareCacheError::Unavailable) => return Ok(None),
            Err(error) => return Err(error),
        };
        let _pointer = entry.lock(Lock::Pointer, false)?;
        let (registration, _) = entry.load()?;
        registration
            .as_ref()
            .map(|registration| describe(id, registration))
            .transpose()
    }

    /// Sealed registration marked as a signed log.
    ///
    /// `expected` is the stable database id. It must equal the invitation stream.
    /// A mismatch writes nothing.
    pub(crate) fn register_signed_log(
        &self,
        home: &Path,
        invitation: &[u8],
        publisher: &Digest,
        name: Option<&str>,
        expected: Option<&Id>,
    ) -> Result<RegisteredShare> {
        let opened = self.recipient.open(invitation, publisher)?;
        let id = &opened.permission.stream().stream_id;
        if expected.is_some_and(|expected| expected.as_str() != id.as_str()) {
            return Err(ShareCacheError::Invalid("database id"));
        }
        let directory = files::private_directory(&self.root, false)?;
        let lock = directory
            .open_lock(REGISTRY_LOCK)
            .map_err(|_| ShareCacheError::Invalid("registry lock"))?;
        lock.try_lock().map_err(|_| ShareCacheError::Busy)?;
        if crate::host::owned::registry_claims(home, id.as_str(), name)
            .map_err(|()| ShareCacheError::Unavailable)?
        {
            return Err(ShareCacheError::Invalid("database selector already in use"));
        }
        self.admit_name(id, name)?;
        let entry = self.entry(id, true)?;
        let _writer = entry.lock(Lock::Writer, true)?;
        let (registration, previous) = entry.load()?;
        if let Some(registration) = &registration {
            self.require_management(registration)?;
        }
        let mut registration =
            Registration::with_invitation(registration, &opened, publisher, invitation)?;
        let state = registration.managed.get_or_insert(ManagedRegistration {
            name: None,
            spec: None,
            expires_ms: 0,
            grant: None,
            grant_version: None,
            grant_expires_ms: None,
            signed_log: false,
        });
        if let Some(name) = name {
            state.name = Some(name.to_owned());
        }
        if let Some(spec) = opened.spec {
            if state.spec.as_ref().is_some_and(|current| current != &spec) {
                return Err(ShareCacheError::Invalid(
                    "invitation spec differs from this share handle",
                ));
            }
            state.spec = Some(spec);
        }
        state.expires_ms = opened.permission.expires_ms();
        state.signed_log = true;
        state.grant = None;
        state.grant_version = None;
        state.grant_expires_ms = None;
        publish(
            &entry,
            previous.as_deref().map(Vec::as_slice),
            &registration,
        )?;
        describe(id, &registration)
    }

    fn admit_name(&self, id: &Id, name: Option<&str>) -> Result<()> {
        let ids = self.managed_ids()?;
        if ids.len() == MAX_REGISTERED && !ids.contains(id) {
            return Err(ShareCacheError::Invalid("registry size"));
        }
        if let Some(name) = name {
            for other in ids.iter().filter(|other| *other != id) {
                if self
                    .managed_record(other)?
                    .is_some_and(|registered| registered.name.as_deref() == Some(name))
                {
                    return Err(ShareCacheError::Invalid("name already registered"));
                }
            }
        }
        Ok(())
    }
}

fn describe(id: &Id, registration: &Registration) -> Result<RegisteredShare> {
    let state = registration
        .managed
        .as_ref()
        .filter(|_| &registration.stream.stream_id == id)
        .ok_or(ShareCacheError::Invalid("managed registration"))?;
    Ok(RegisteredShare {
        id: id.clone(),
        publisher: registration.publisher.clone(),
        name: state.name.clone(),
        spec: state.spec.clone(),
        snapshot: None,
        expires_ms: state.expires_ms,
        stream: registration.stream.clone(),
        environment: registration.environment,
        signed_log: state.signed_log,
    })
}
