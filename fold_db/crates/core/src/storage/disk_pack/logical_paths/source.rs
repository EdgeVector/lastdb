//! Compact source certificates belong to one read or write call, never bodies.
use super::*;
use crate::atom::Atom;
use std::collections::BTreeMap;

// One read can need a source, atom certificate, and scope binding for each key.
// All three maps share this byte cap. Each entry charges a fixed overhead, so
// the bytes also bound the count.
const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
static SOURCE_CACHE: u8 = 0;

fn primary_coordinate(requested: &str) -> (&'static str, String) {
    (
        crate::storage::disk_pack::native_routing::collection_for_key(requested.as_bytes()),
        crate::storage::disk_pack::native_routing::encode(requested.as_bytes()),
    )
}

#[derive(Clone)]
struct SourceKey {
    collection: String,
    key: String,
}

struct AtomProof {
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
    /// Optional receipts retain only the charge of a primary source.
    fn retain_atom(&mut self, requested: &str, source: &SourceKey, bytes: usize) {
        if source.primary(requested) && self.room(bytes) {
            self.atoms.insert(requested.to_owned(), AtomProof { bytes });
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
    sources.retain_atom(requested, &source, bytes);
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

fn scoped(scope: Option<&str>, atom: &str) -> String {
    serde_json::to_string(&(scope, atom)).expect("atom scope tuple")
}
