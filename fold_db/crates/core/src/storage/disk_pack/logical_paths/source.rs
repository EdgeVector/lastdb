//! Full decoded atoms and source coordinates belong to one read or write call.
use super::*;
use crate::atom::Atom;
use std::collections::BTreeMap;

const MAX_SOURCE_ENTRIES: usize = 10_000;
const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
mod certified;
pub(crate) use certified::{certified_atoms, CertifiedAtom};
static SOURCE_CACHE: u8 = 0;

pub(in crate::storage::disk_pack) struct BodyLocation {
    pub(in crate::storage::disk_pack) partition: Option<crate::atom::AtomPartition>,
}

pub(in crate::storage::disk_pack) fn body_location(
    core: &crate::resident::CoreWarmSet,
    path: &crate::resident::CorePath,
    atom: &str,
) -> StorageResult<Option<BodyLocation>> {
    body_location_from_coordinates(&core.source_coordinates(path), atom)
}

pub(in crate::storage::disk_pack) fn body_location_from_coordinates(
    coordinates: &[(String, String)],
    atom: &str,
) -> StorageResult<Option<BodyLocation>> {
    for (_, key) in coordinates {
        let raw = crate::storage::laststore::native_routing::decode(key)?;
        let Ok(raw) = std::str::from_utf8(&raw) else {
            continue;
        };
        if crate::atom::atom_key_codec::uuid_of(raw) == Some(atom) {
            return Ok(Some(BodyLocation {
                partition: crate::atom::atom_key_codec::partition_of(raw),
            }));
        }
    }
    Ok(None)
}

pub(super) fn body_matches(
    core: &crate::resident::CoreWarmSet,
    path: &crate::resident::CorePath,
    requested: &str,
) -> bool {
    body_matches_coordinates(&core.source_coordinates(path), requested)
}

pub(super) fn body_matches_coordinates(coordinates: &[(String, String)], requested: &str) -> bool {
    let Some(first) = crate::kind_partition::read_forms(requested)
        .into_iter()
        .next()
    else {
        return false;
    };
    let Some(collection) =
        crate::storage::laststore::native_routing::read_candidates(first.as_bytes())
            .into_iter()
            .next()
    else {
        return false;
    };
    let encoded = crate::storage::laststore::native_routing::encode(first.as_bytes());
    coordinates
        .iter()
        .any(|(actual_collection, key)| actual_collection == collection && key == &encoded)
}

#[derive(Clone)]
pub(super) struct AtomFacts {
    source: SourceKey,
    pub(super) atom: Arc<Atom>,
    bytes: usize,
}

impl AtomFacts {
    pub(super) fn source(&self) -> (&str, &str) {
        (&self.source.collection, &self.source.key)
    }

    fn primary(&self, requested: &str) -> bool {
        let Some(first) = crate::kind_partition::read_forms(requested)
            .into_iter()
            .next()
        else {
            return false;
        };
        crate::storage::laststore::native_routing::read_candidates(first.as_bytes())
            .into_iter()
            .next()
            == Some(self.source.collection.as_str())
            && crate::storage::laststore::native_routing::encode(first.as_bytes())
                == self.source.key
    }
}

#[derive(Default)]
pub(in crate::storage::disk_pack) struct Sources {
    reads: BTreeMap<String, SourceKey>,
    atoms: BTreeMap<String, AtomFacts>,
    scoped: BTreeMap<String, String>,
    bytes: usize,
}

#[derive(Clone)]
struct SourceKey {
    collection: String,
    key: String,
}

impl Sources {
    fn room(&mut self, bytes: usize) -> bool {
        let entries = self.reads.len() + self.atoms.len() + self.scoped.len();
        let Some(total) = self
            .bytes
            .checked_add(bytes)
            .filter(|n| *n <= MAX_SOURCE_BYTES)
        else {
            return false;
        };
        if entries >= MAX_SOURCE_ENTRIES {
            return false;
        }
        self.bytes = total;
        true
    }
}

fn sources(runtime: Option<Arc<PackRuntime>>) -> StorageResult<Option<Arc<Mutex<Sources>>>> {
    if let Some(pending) = runtime
        .as_ref()
        .and_then(|runtime| transaction::PendingBatch::current(runtime))
    {
        return Ok(Some(pending.source_receipts()));
    }
    if !read_snapshot_active(runtime.clone()) {
        return Ok(None);
    }
    let cache = std::ptr::from_ref(&SOURCE_CACHE) as usize;
    let name = "native_typed_source_receipts_v2";
    if let Some(sources) = read_snapshot_get(runtime.clone(), cache, name)? {
        return Ok(Some(sources));
    }
    match read_snapshot_insert(runtime, cache, name.into(), Mutex::new(Sources::default())) {
        Ok(sources) => Ok(Some(sources)),
        Err(StorageError::ReadResourceLimit { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn note_read_source(
    runtime: Arc<PackRuntime>,
    requested: &[u8],
    source: Option<(&str, &str)>,
) -> StorageResult<()> {
    let Ok(requested) = std::str::from_utf8(requested) else {
        return Ok(());
    };
    let Some(sources) = sources(Some(runtime))? else {
        return Ok(());
    };
    let mut sources = sources.lock().expect("native read source receipts");
    if let Some(previous) = sources.reads.remove(requested) {
        sources.bytes -= 192 + requested.len() + previous.collection.len() + previous.key.len();
    }
    if let Some(previous) = sources.atoms.remove(requested) {
        sources.bytes -= previous.bytes;
    }
    let Some((collection, actual)) = source else {
        return Ok(());
    };
    if sources.room(192 + requested.len() + collection.len() + actual.len()) {
        sources.reads.insert(
            requested.to_owned(),
            SourceKey {
                collection: collection.to_owned(),
                key: actual.to_owned(),
            },
        );
    }
    Ok(())
}

pub(crate) fn note_atom(
    runtime: Option<Arc<PackRuntime>>,
    requested: &str,
    atom: &Atom,
) -> StorageResult<()> {
    let Some(sources) = sources(runtime)? else {
        return Ok(());
    };
    let mut sources = sources.lock().expect("native typed atom sources");
    if let Some(previous) = sources.atoms.remove(requested) {
        sources.bytes -= previous.bytes;
    }
    let Some(source) = sources.reads.get(requested).cloned() else {
        return Ok(());
    };
    let decoded = crate::storage::laststore::native_routing::decode(&source.key)?;
    if std::str::from_utf8(&decoded)
        .ok()
        .and_then(crate::atom::atom_key_codec::uuid_of)
        != Some(atom.uuid())
    {
        return Ok(());
    }
    let bytes = 192
        + requested.len()
        + source.collection.len()
        + source.key.len()
        + super::body::encoded_len(atom)?;
    let facts = AtomFacts {
        source,
        atom: Arc::new(atom.clone()),
        bytes,
    };
    if facts.primary(requested) && sources.room(bytes) {
        sources.atoms.insert(requested.to_owned(), facts);
    }
    Ok(())
}

pub(crate) fn bind_atoms(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    slots: &[(&str, Option<crate::atom::AtomPartition>)],
    keys: &[String],
    atoms: &[Option<Atom>],
    primary: &[String],
) -> StorageResult<()> {
    let Some(sources) = sources(runtime)? else {
        return Ok(());
    };
    let mut sources = sources.lock().expect("native scoped atom sources");
    for (((key, atom), (_, hint)), first) in keys.iter().zip(atoms).zip(slots).zip(primary) {
        let Some(atom) = atom else { continue };
        if key == first {
            bind(&mut sources, scope, key, atom, hint.as_ref());
        }
    }
    Ok(())
}

pub(crate) fn bind_atom(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    key: &str,
    atom: &Atom,
    hint: Option<&crate::atom::AtomPartition>,
    primary: &str,
) -> StorageResult<()> {
    if key != primary {
        return Ok(());
    }
    if let Some(sources) = sources(runtime)? {
        bind(
            &mut sources.lock().expect("native scoped atom source"),
            scope,
            key,
            atom,
            hint,
        );
    }
    Ok(())
}

fn bind(
    sources: &mut Sources,
    scope: Option<&str>,
    key: &str,
    atom: &Atom,
    hint: Option<&crate::atom::AtomPartition>,
) {
    let identity = scoped(scope, atom.uuid(), hint);
    if let Some(previous) = sources.scoped.remove(&identity) {
        sources.bytes -= 192 + identity.len() + previous.len();
    }
    if !sources.atoms.contains_key(key) {
        return;
    }
    if sources.room(192 + identity.len() + key.len()) {
        sources.scoped.insert(identity, key.to_owned());
    }
}

pub(super) fn atom_facts(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    atom: &str,
    hint: Option<&crate::atom::AtomPartition>,
) -> StorageResult<Option<AtomFacts>> {
    let Some(sources) = sources(runtime)? else {
        return Ok(None);
    };
    let sources = sources.lock().expect("native selected full atom");
    Ok(sources
        .scoped
        .get(&scoped(scope, atom, hint))
        .and_then(|key| sources.atoms.get(key))
        .cloned())
}

fn scoped(scope: Option<&str>, atom: &str, hint: Option<&crate::atom::AtomPartition>) -> String {
    serde_json::to_string(&(scope, atom, hint.map(crate::atom::AtomPartition::as_str)))
        .expect("atom scope tuple")
}

pub(super) fn source_for_request(
    runtime: Option<Arc<PackRuntime>>,
    key: &str,
) -> StorageResult<Option<(String, String)>> {
    let Some(sources) = sources(runtime)? else {
        return Ok(None);
    };
    let sources = sources.lock().expect("native typed source coordinate");
    Ok(sources
        .reads
        .get(key)
        .map(|source| (source.collection.clone(), source.key.clone())))
}
