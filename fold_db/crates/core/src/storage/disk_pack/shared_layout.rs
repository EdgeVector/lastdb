//! Physical placement for a dedicated shared database.

use super::*;
use crate::storage::format::{
    records::{applied_deletes, inline_files},
    CAS_COLLECTION, CONTENT_KEY_PROOF_KEY, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY,
    FIELD_KEYS_NAMESPACE, METADATA_NAMESPACE, RECORDS_COLLECTION, RECORD_FILE_PREFIX,
    RETENTION_POLICY_KEY_PREFIX, SCHEMAS_NAMESPACE, SCHEMA_STATES_NAMESPACE,
};
use lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX;
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use laststore::disk_run::home::{RunHome, RunRead};
use std::collections::{BTreeMap, BTreeSet};

pub(super) mod journal;
pub(super) mod range;

use crate::storage::format::metadata::NATIVE_SHARED_SCOPE_KEY as SCOPE_KEY;
const CREATE_ENV: &str = "LASTDB_NATIVE_SHARED_SCHEMAS";
const CONFIG_BYTES: usize = 16 * 1024;

pub(super) struct SharedHome {
    pub(super) private: Arc<RunHome>,
    pub(super) scope: Arc<RwLock<Arc<Scope>>>,
    pub(super) journal_key: zeroize::Zeroizing<[u8; 32]>,
}

#[derive(Clone)]
pub(super) struct Scope {
    aliases: BTreeSet<String>,
    spaces: BTreeSet<String>,
    alternates: BTreeSet<String>,
    schemas: BTreeMap<String, Vec<u8>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Lane {
    Records,
    Files,
    Private,
}

impl Scope {
    pub(super) fn requested() -> StorageResult<Option<Self>> {
        let bytes = match std::env::var(CREATE_ENV) {
            Ok(value) => value.into_bytes(),
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(_) => return Err(invalid("shared schema scope is not UTF-8")),
        };
        Self::decode(&bytes).map(Some)
    }

    fn decode(bytes: &[u8]) -> StorageResult<Self> {
        if bytes.len() > CONFIG_BYTES {
            return Err(invalid("shared schema scope exceeds its size bound"));
        }
        let names: Vec<String> = serde_json::from_slice(bytes)
            .map_err(|_| invalid("shared schema scope requires a JSON array of aliases"))?;
        let count = names.len();
        let aliases: BTreeSet<_> = names.into_iter().collect();
        if aliases.len() != count {
            return Err(invalid("duplicate shared schema alias"));
        }
        validate_aliases(&aliases)?;
        let spaces = aliases.iter().map(|name| record_space_uuid(name)).collect();
        Ok(Self {
            aliases,
            spaces,
            alternates: BTreeSet::new(),
            schemas: BTreeMap::new(),
        })
    }

    pub(super) fn encode(&self) -> StorageResult<Vec<u8>> {
        serde_json::to_vec(&self.aliases).map_err(|_| invalid("shared schema scope encoding"))
    }

    pub(super) fn load(read: &mut RunRead) -> StorageResult<Self> {
        let bytes = read
            .get_bounded(METADATA_NAMESPACE, SCOPE_KEY, CONFIG_BYTES as u64)
            .map_err(map)?
            .ok_or_else(|| invalid("shared schema scope is missing"))?;
        let mut scope = Self::decode(&bytes)?;
        let coordinates = scope
            .aliases
            .iter()
            .map(|name| {
                (
                    SCHEMAS_NAMESPACE.to_owned(),
                    native_routing::encode(name.as_bytes()),
                )
            })
            .collect::<Vec<_>>();
        let rows = read.get_many(&coordinates).map_err(map)?;
        let names = scope.aliases.iter().cloned().collect::<Vec<_>>();
        for (name, row) in names.into_iter().zip(rows) {
            if let Some(bytes) = row.map_err(map)? {
                scope.add_schema(name, &bytes)?;
            }
        }
        Ok(scope)
    }

    pub(super) fn with_writes<'a>(
        current: &Arc<Self>,
        operations: impl IntoIterator<Item = &'a HomeMutation>,
    ) -> StorageResult<Arc<Self>> {
        let mut scope = None;
        let mut schemas = Vec::new();
        for op in operations {
            match op.collection.as_str() {
                METADATA_NAMESPACE if op.key == SCOPE_KEY => {
                    let bytes = op
                        .body
                        .as_deref()
                        .ok_or_else(|| invalid("shared disk scope cannot be removed"))?;
                    let requested = Self::decode(bytes)?;
                    let active = scope.get_or_insert_with(|| current.as_ref().clone());
                    if !active.aliases.is_subset(&requested.aliases) {
                        return Err(invalid("shared disk scope cannot lose an alias"));
                    }
                    active.aliases = requested.aliases;
                    active.spaces = requested.spaces;
                }
                SCHEMAS_NAMESPACE | SCHEMA_STATES_NAMESPACE => schemas.push(op),
                _ => {}
            }
        }
        for op in schemas {
            let key = native_routing::decode(&op.key)?;
            let name =
                std::str::from_utf8(&key).map_err(|_| invalid("schema alias is not UTF-8"))?;
            let active = scope.as_ref().unwrap_or(current.as_ref());
            if !active.aliases.contains(name) {
                continue;
            }
            if op.collection == SCHEMA_STATES_NAMESPACE {
                if op.body.as_deref() != Some(b"\"Available\"") {
                    return Err(invalid("a shared schema must stay available"));
                }
                continue;
            }
            let bytes = op
                .body
                .as_deref()
                .ok_or_else(|| invalid("a shared schema cannot be removed"))?;
            match active.schemas.get(name) {
                Some(previous) if previous == bytes => continue,
                Some(_) => return Err(invalid("a shared schema definition cannot be replaced")),
                None => {
                    scope
                        .get_or_insert_with(|| current.as_ref().clone())
                        .add_schema(name.to_owned(), bytes)?;
                }
            }
        }
        Ok(scope.map_or_else(|| Arc::clone(current), Arc::new))
    }

    fn add_schema(&mut self, name: String, bytes: &[u8]) -> StorageResult<()> {
        let schema: crate::schema::Schema = serde_json::from_slice(bytes)
            .map_err(|_| invalid("shared schema definition encoding"))?;
        if schema.name != name {
            return Err(invalid("shared schema alias differs from its definition"));
        }
        let keys = crate::storage::field_key_store::schema_field_key_profiles(&schema)?;
        self.alternates.extend(keys.into_iter().map(|(key, _)| key));
        self.schemas.insert(name, bytes.to_vec());
        Ok(())
    }

    pub(super) fn lane(&self, collection: &str, key: &str) -> Lane {
        let decoded;
        let key = if key.starts_with("b64:") {
            decoded = native_routing::decode(key).ok();
            decoded
                .as_deref()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .unwrap_or(key)
        } else {
            key
        };
        match collection {
            CAS_COLLECTION
                if part(key, RECORD_FILE_PREFIX)
                    .is_some_and(|space| self.spaces.contains(space)) =>
            {
                Lane::Files
            }
            RECORDS_COLLECTION if self.public_record_key(key) => Lane::Records,
            SCHEMAS_NAMESPACE if self.aliases.contains(key) => Lane::Records,
            SCHEMA_STATES_NAMESPACE
                if self.aliases.contains(key)
                    || key
                        .strip_prefix(RETENTION_POLICY_KEY_PREFIX)
                        .is_some_and(|name| self.aliases.contains(name)) =>
            {
                Lane::Records
            }
            FIELD_KEYS_NAMESPACE
                if crate::storage::format::field_keys::storage_key_identity(key)
                    .is_some_and(|id| self.alternates.contains(id) || self.spaces.contains(id)) =>
            {
                Lane::Records
            }
            METADATA_NAMESPACE
                if key == SCOPE_KEY
                    || key.as_bytes() == CONTENT_KEY_PROOF_KEY
                    || crate::storage::format::metadata::shared_metadata_key(key) =>
            {
                Lane::Records
            }
            _ => Lane::Private,
        }
    }

    fn public_record_key(&self, key: &str) -> bool {
        let record = inline_files::owning_record(key);
        key == CURRENT_KEY_FORMAT_KEY
            || key == DELETE_REPLAY_CUTOFF_KEY
            || applied_deletes::is_age_key(key)
            || part(record, RECORD_PREFIX).is_some_and(|space| self.spaces.contains(space))
            || part(key, ALTERNATE_ENTRY_PREFIX).is_some_and(|id| self.alternates.contains(id))
    }

    pub(super) fn bounded_range_lane(
        &self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Option<Lane> {
        let key = std::str::from_utf8(start).ok()?;
        if !matches!(
            collection,
            RECORDS_COLLECTION
                | CAS_COLLECTION
                | SCHEMAS_NAMESPACE
                | SCHEMA_STATES_NAMESPACE
                | FIELD_KEYS_NAMESPACE
                | METADATA_NAMESPACE
        ) {
            return Some(Lane::Private);
        }
        let prefix = match collection {
            RECORDS_COLLECTION => {
                let body = inline_files::owning_record(key);
                let prefix = if body.starts_with(RECORD_PREFIX) {
                    RECORD_PREFIX
                } else {
                    ALTERNATE_ENTRY_PREFIX
                };
                part(body, prefix).map(|id| {
                    let offset = key.len() - body.len();
                    &key[..offset + prefix.len() + id.len()]
                })
            }
            CAS_COLLECTION => part(key, RECORD_FILE_PREFIX)
                .map(|space| &key[..RECORD_FILE_PREFIX.len() + space.len()]),
            _ => None,
        }?;
        end.starts_with(prefix.as_bytes())
            .then(|| self.lane(collection, key))
    }

    pub(super) fn owns_record(&self, key: &str) -> bool {
        part(key, RECORD_PREFIX).is_some_and(|space| self.spaces.contains(space))
    }

    fn aliases_match<'a>(&self, names: impl IntoIterator<Item = &'a str>) -> bool {
        let mut names: Vec<_> = names.into_iter().collect();
        names.sort_unstable();
        self.aliases.iter().map(String::as_str).eq(names)
    }
}

impl PackRuntime {
    pub(crate) fn is_shared_schema(&self, name: &str) -> bool {
        self.physical.shared.as_ref().is_some_and(|shared| {
            shared
                .scope
                .read()
                .expect("shared disk scope")
                .aliases
                .contains(name)
        })
    }

    pub fn require_shared_schema_set<'a>(
        &self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> StorageResult<()> {
        if let Some(shared) = &self.physical.shared {
            if !shared
                .scope
                .read()
                .expect("shared disk scope")
                .aliases_match(names)
            {
                return Err(invalid(
                    "writer schema aliases differ from its dedicated shared disk scope",
                ));
            }
        }
        Ok(())
    }

    /// A writer binds a subset of the shared aliases. Peer definitions add the rest.
    pub fn require_shared_schema_subset<'a>(
        &self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> StorageResult<()> {
        if let Some(shared) = &self.physical.shared {
            let scope = shared.scope.read().expect("shared disk scope");
            if names.into_iter().any(|name| !scope.aliases.contains(name)) {
                return Err(invalid(
                    "writer schema alias is outside its dedicated shared disk scope",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn shared_schema_aliases(&self) -> Vec<String> {
        let Some(shared) = &self.physical.shared else {
            return Vec::new();
        };
        let scope = shared.scope.read().expect("shared disk scope");
        scope.aliases.iter().cloned().collect()
    }

    /// The caller grows the scope only by aliases that no schema in this home
    /// uses yet, so no existing private row changes its lane.
    pub(in crate::storage) fn grown_shared_aliases<'a>(
        &self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> StorageResult<Option<Vec<u8>>> {
        let Some(shared) = &self.physical.shared else {
            return Err(invalid(
                "shared schema definitions require a dedicated shared disk home",
            ));
        };
        let active = shared.scope.read().expect("shared disk scope");
        let mut names: BTreeSet<_> = names.into_iter().map(str::to_owned).collect();
        if names.is_subset(&active.aliases) {
            return Ok(None);
        }
        names.extend(active.aliases.iter().cloned());
        validate_aliases(&names)?;
        let encoded =
            serde_json::to_vec(&names).map_err(|_| invalid("shared schema scope encoding"))?;
        if encoded.len() > CONFIG_BYTES {
            return Err(invalid("shared schema scope exceeds its size bound"));
        }
        Ok(Some(encoded))
    }

    pub(crate) fn require_shared_record_route(
        &self,
        key: &str,
        shared_record: bool,
    ) -> StorageResult<()> {
        if let Some(shared) = &self.physical.shared {
            if shared
                .scope
                .read()
                .expect("shared disk scope")
                .owns_record(key)
                != shared_record
            {
                return Err(invalid(
                    "record operation differs from its dedicated shared disk scope",
                ));
            }
        }
        Ok(())
    }
}

fn validate_aliases(aliases: &BTreeSet<String>) -> StorageResult<()> {
    if aliases.is_empty()
        || aliases.len() > 64
        || aliases
            .iter()
            .any(|name| name.trim() != name || name.is_empty() || name.contains('\0'))
    {
        return Err(invalid("invalid shared schema alias"));
    }
    Ok(())
}

pub(super) fn invalid(reason: &str) -> StorageError {
    StorageError::InvalidOperation(reason.into())
}

fn part<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?;
    Some(rest.split_once(':').map_or(rest, |(first, _)| first))
}
