//! Compact source certificates belong to one read or write call, never bodies.
use super::*;
use crate::atom::Atom;
use std::collections::BTreeMap;

// One read can need a source, atom certificate, and scope binding for each key.
// All three maps share this byte cap. Each entry charges a fixed overhead, so
// the bytes also bound the count.
const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
mod certified;
mod proof;
pub(crate) use certified::{certified_atoms, CertifiedAtom};
static SOURCE_CACHE: u8 = 0;

/// True when one of the path's source coordinates is the body of `atom`.
pub(in crate::storage::disk_pack) fn body_present(
    core: &crate::resident::CoreWarmSet,
    path: &crate::resident::CorePath,
    atom: &str,
) -> StorageResult<bool> {
    body_present_in_coordinates(&core.source_coordinates(path), atom)
}

pub(in crate::storage::disk_pack) fn body_present_in_coordinates(
    coordinates: &[(String, String)],
    atom: &str,
) -> StorageResult<bool> {
    for (_, key) in coordinates {
        let raw = crate::storage::disk_pack::native_routing::decode(key)?;
        let Ok(raw) = std::str::from_utf8(&raw) else {
            continue;
        };
        if crate::atom::atom_key_codec::body_uuid_of(raw) == Some(atom) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn body_matches(
    core: &crate::resident::CoreWarmSet,
    path: &crate::resident::CorePath,
    requested: &str,
) -> bool {
    body_matches_coordinates(&core.source_coordinates(path), requested)
}

pub(super) fn body_matches_coordinates(coordinates: &[(String, String)], requested: &str) -> bool {
    let (collection, encoded) = primary_coordinate(requested);
    coordinates
        .iter()
        .any(|(actual_collection, key)| actual_collection == collection && key == &encoded)
}

fn primary_coordinate(requested: &str) -> (&'static str, String) {
    (
        crate::storage::disk_pack::native_routing::collection_for_key(requested.as_bytes()),
        crate::storage::disk_pack::native_routing::encode(requested.as_bytes()),
    )
}

pub(super) struct AtomFacts {
    source: SourceKey,
    pub(super) atom: Arc<Atom>,
}

impl AtomFacts {
    pub(super) fn source(&self) -> (&str, &str) {
        (&self.source.collection, &self.source.key)
    }
}

#[derive(Clone)]
struct SourceKey {
    collection: String,
    key: String,
}

struct AtomProof {
    source: SourceKey,
    fingerprint: [u8; 32],
    bytes: usize,
}

impl SourceKey {
    fn primary(&self, requested: &str) -> bool {
        let (collection, key) = primary_coordinate(requested);
        collection == self.collection && key == self.key
    }
}

#[derive(Default)]
pub(in crate::storage::disk_pack) struct Sources {
    reads: BTreeMap<String, SourceKey>,
    atoms: BTreeMap<String, AtomProof>,
    scoped: BTreeMap<String, String>,
    bytes: usize,
}

impl Sources {
    /// Optional receipts retain only a complete fingerprint and source keys.
    fn retain_atom(
        &mut self,
        requested: &str,
        source: SourceKey,
        bytes: usize,
        fingerprint: [u8; 32],
    ) {
        if source.primary(requested) && self.room(bytes) {
            self.atoms.insert(
                requested.to_owned(),
                AtomProof {
                    source,
                    fingerprint,
                    bytes,
                },
            );
        }
    }

    fn room(&mut self, bytes: usize) -> bool {
        let Some(total) = self
            .bytes
            .checked_add(bytes)
            .filter(|n| *n <= MAX_SOURCE_BYTES)
        else {
            return false;
        };
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
    let name = "native_typed_source_receipts_v3";
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
    let decoded = crate::storage::disk_pack::native_routing::decode(&source.key)?;
    let Ok(decoded) = std::str::from_utf8(&decoded) else {
        return Ok(());
    };
    if crate::atom::atom_key_codec::body_uuid_of(decoded) != Some(atom.uuid()) {
        return Ok(());
    }
    let bytes = 192 + requested.len() + source.collection.len() + source.key.len();
    sources.retain_atom(requested, source, bytes, proof::fingerprint(atom)?);
    Ok(())
}

pub(crate) fn bind_atoms(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    keys: &[String],
    atoms: &[Option<Atom>],
) -> StorageResult<()> {
    let Some(sources) = sources(runtime)? else {
        return Ok(());
    };
    let mut sources = sources.lock().expect("native scoped atom sources");
    for (key, atom) in keys.iter().zip(atoms) {
        if let Some(atom) = atom {
            bind(&mut sources, scope, key, atom);
        }
    }
    Ok(())
}

fn bind(sources: &mut Sources, scope: Option<&str>, key: &str, atom: &Atom) {
    let identity = scoped(scope, atom.uuid());
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
    atom: Atom,
) -> StorageResult<Option<AtomFacts>> {
    let Some(sources) = sources(runtime)? else {
        return Ok(None);
    };
    let sources = sources.lock().expect("native selected atom certificate");
    let Some((source, _)) = certified::certified_source(&sources, scope, &atom)? else {
        return Ok(None);
    };
    Ok(Some(AtomFacts {
        source,
        atom: Arc::new(atom),
    }))
}

fn scoped(scope: Option<&str>, atom: &str) -> String {
    serde_json::to_string(&(scope, atom)).expect("atom scope tuple")
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
