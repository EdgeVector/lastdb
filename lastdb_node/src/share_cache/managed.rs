//! Node-owned Share bindings on top of the verified cache.
use super::{
    files, publish, Lock, Registration, Result, ShareCache, ShareCacheError, MANAGED_DIRECTORY,
};
use crate::registered_share::RegisteredShare;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
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
    expires_ms: u64,
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
        let root = home.join(MANAGED_DIRECTORY);
        if !create
            && matches!(fs::symlink_metadata(root), Err(e) if e.kind() == ErrorKind::NotFound)
        {
            return Ok(None);
        }
        let home = home.canonicalize()?;
        let root = home.join(MANAGED_DIRECTORY);
        let recipient = crate::share_publication::recipient(&home)
            .map_err(|_| ShareCacheError::Invalid("recipient identity"))?;
        files::private_directory(&root, create)?;
        Ok(Some(Self {
            root,
            recipient,
            managed: true,
        }))
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

    pub(crate) fn managed_record(&self, home: &Path, id: &Id) -> Result<Option<RegisteredShare>> {
        let entry = match self.entry(id, false) {
            Ok(entry) => entry,
            Err(ShareCacheError::Unavailable) => return Ok(None),
            Err(error) => return Err(error),
        };
        let _pointer = entry.lock(Lock::Pointer, false)?;
        let (registration, _) = entry.load()?;
        registration
            .as_ref()
            .map(|registration| describe(home, id, registration))
            .transpose()
    }

    pub(crate) fn register_managed(
        &self,
        home: &Path,
        invitation: &[u8],
        publisher: &Digest,
        name: Option<&str>,
    ) -> Result<RegisteredShare> {
        self.register(home, invitation, publisher, name, None)
    }

    /// Same sealed registration as [`Self::register_managed`], marked as a signed log.
    ///
    /// `expected` is the stable database id. It must equal the invitation stream.
    /// A mismatch writes nothing.
    pub(crate) fn register_signed_log(
        &self,
        home: &Path,
        invitation: &[u8],
        publisher: &Digest,
        name: Option<&str>,
        expected: &Id,
    ) -> Result<RegisteredShare> {
        self.register(home, invitation, publisher, name, Some(expected))
    }

    fn register(
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
        if crate::registered_share::local_publication_id(home)?.as_ref() == Some(id) {
            return Err(ShareCacheError::Invalid(
                "invitation names the local publication",
            ));
        }
        let directory = files::private_directory(&self.root, false)?;
        let lock = directory
            .open_lock(REGISTRY_LOCK)
            .map_err(|_| ShareCacheError::Invalid("registry lock"))?;
        lock.try_lock().map_err(|_| ShareCacheError::Busy)?;
        self.admit_name(home, id, name)?;
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
            expires_ms: 0,
            grant: None,
            grant_version: None,
            grant_expires_ms: None,
            signed_log: false,
        });
        if let Some(name) = name {
            state.name = Some(name.to_owned());
        }
        state.expires_ms = opened.permission.expires_ms();
        state.signed_log |= expected.is_some();
        if state.signed_log {
            state.grant = None;
            state.grant_version = None;
            state.grant_expires_ms = None;
        }
        publish(
            &entry,
            previous.as_deref().map(Vec::as_slice),
            &registration,
        )?;
        describe(home, id, &registration)
    }

    fn admit_name(&self, home: &Path, id: &Id, name: Option<&str>) -> Result<()> {
        let ids = self.managed_ids()?;
        if ids.len() == MAX_REGISTERED && !ids.contains(id) {
            return Err(ShareCacheError::Invalid("registry size"));
        }
        if let Some(name) = name {
            for other in ids.iter().filter(|other| *other != id) {
                if self
                    .managed_record(home, other)?
                    .is_some_and(|registered| registered.name.as_deref() == Some(name))
                {
                    return Err(ShareCacheError::Invalid("name already registered"));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn attach_managed_grant(
        &self,
        home: &Path,
        id: &Id,
        sealed: &[u8],
    ) -> Result<RegisteredShare> {
        let entry = self.entry(id, false)?;
        let _writer = entry.lock(Lock::Writer, true)?;
        let (registration, previous) = entry.load()?;
        let mut registration = registration.ok_or(ShareCacheError::Unavailable)?;
        self.require_management(&registration)?;
        let binding = crate::share_contribution::validate_grant(
            home,
            sealed,
            &registration.publisher,
            &registration.stream,
            registration.environment,
        )
        .map_err(|_| ShareCacheError::Invalid("contributor grant"))?;
        let state = registration
            .managed
            .as_mut()
            .ok_or(ShareCacheError::Invalid("managed registration"))?;
        if state.signed_log {
            return Err(ShareCacheError::Invalid("contributor grant"));
        }
        if state
            .grant_version
            .is_some_and(|version| version > binding.version)
        {
            return Err(ShareCacheError::Invalid(
                "contributor grant version regressed",
            ));
        }
        state.grant = Some(URL_SAFE_NO_PAD.encode(sealed));
        state.grant_version = Some(binding.version);
        state.grant_expires_ms = Some(binding.expires_ms);
        publish(
            &entry,
            previous.as_deref().map(Vec::as_slice),
            &registration,
        )?;
        describe(home, id, &registration)
    }
}

fn describe(home: &Path, id: &Id, registration: &Registration) -> Result<RegisteredShare> {
    let state = registration
        .managed
        .as_ref()
        .filter(|_| &registration.stream.stream_id == id)
        .ok_or(ShareCacheError::Invalid("managed registration"))?;
    let contributor_grant = state
        .grant
        .as_ref()
        .map(|grant| URL_SAFE_NO_PAD.decode(grant))
        .transpose()
        .map_err(|_| ShareCacheError::Invalid("contributor grant"))?;
    Ok(RegisteredShare {
        id: id.clone(),
        publisher: registration.publisher.clone(),
        name: state.name.clone(),
        snapshot: registration
            .active
            .as_ref()
            .map(|active| active.snapshot.clone()),
        expires_ms: state.expires_ms,
        home: home.canonicalize()?,
        stream: registration.stream.clone(),
        environment: registration.environment,
        contributor_grant,
        contributor_expires_ms: state.grant_expires_ms,
        signed_log: state.signed_log,
    })
}
