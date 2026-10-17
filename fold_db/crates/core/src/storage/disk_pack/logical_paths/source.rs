//! Atom facts share the canonical body; source receipts expire with one read call.

use super::*;
use crate::atom::{Atom, KeyMetadata};
use crate::schema::types::field::FieldValue;
use crate::storage::disk_pack::read::ReadContext;
use std::collections::HashMap;

const MAX_SOURCE_ENTRIES: usize = 10_000;
const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
static SOURCE_CACHE: u8 = 0;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct AtomFacts {
    source: SourceKey,
    version: super::body::ReadVersion,
    witness: u64,
    source_file_name: Option<String>,
    metadata: Option<HashMap<String, String>>,
    written_at: Option<u64>,
}

impl AtomFacts {
    pub(super) fn valid(&self, context: &ReadContext) -> bool {
        context.provenance_valid(self.version, Some(self.witness))
    }

    pub(super) fn inherit(&self, context: &ReadContext) -> bool {
        self.valid(context) && context.inherit_witness(Some(self.witness))
    }

    pub(super) fn primary(&self, requested: &str) -> bool {
        let Some(first) = crate::kind_partition::read_forms(requested)
            .into_iter()
            .next()
        else {
            return false;
        };
        let collection =
            crate::storage::laststore::native_routing::read_candidates(first.as_bytes())
                .into_iter()
                .next();
        collection == Some(self.source.collection.as_str())
            && crate::storage::laststore::native_routing::encode(first.as_bytes())
                == self.source.key
    }

    pub(super) fn renew(&mut self, context: &ReadContext) {
        if self.version.revision == context.version.revision || !self.inherit(context) {
            return;
        }
        if let Some(witness) = context.source_witness(&self.source.collection, &self.source.key) {
            self.version = context.version;
            self.witness = witness;
        }
    }

    pub(super) fn field(
        &self,
        content: serde_json::Value,
        atom: &str,
        metadata: Option<&KeyMetadata>,
        writer: Option<&str>,
    ) -> FieldValue {
        FieldValue {
            value: content,
            atom_uuid: atom.to_string(),
            source_file_name: metadata
                .and_then(|value| value.source_file_name.clone())
                .or_else(|| self.source_file_name.clone()),
            metadata: metadata
                .and_then(|value| value.metadata.clone())
                .or_else(|| self.metadata.clone()),
            molecule_uuid: None,
            molecule_version: None,
            writer_pubkey: writer.filter(|value| !value.is_empty()).map(str::to_string),
            written_at: self.written_at,
        }
    }
}

#[derive(Default)]
struct Sources {
    reads: BTreeMap<String, SourceKey>,
    atoms: BTreeMap<String, AtomFacts>,
    scoped: BTreeMap<String, String>,
    bytes: usize,
}

#[derive(Clone, Serialize, Deserialize)]
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
    if !read_snapshot_active(runtime.clone()) {
        return Ok(None);
    }
    let cache = std::ptr::from_ref(&SOURCE_CACHE) as usize;
    let name = "native_atom_source_receipts_v1";
    if let Some(sources) = read_snapshot_get(runtime.clone(), cache, name)? {
        return Ok(Some(sources));
    }
    match read_snapshot_insert(runtime, cache, name.into(), Mutex::new(Sources::default())) {
        Ok(sources) => Ok(Some(sources)),
        Err(StorageError::ReadResourceLimit { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// The native KV selector supplies the exact encoded key whose bytes won.
pub(crate) fn note_read_source(
    runtime: Arc<PackRuntime>,
    requested: &[u8],
    collection: &str,
    actual: &str,
) -> StorageResult<()> {
    let Ok(requested) = std::str::from_utf8(requested) else {
        return Ok(());
    };
    if crate::atom::atom_key_codec::uuid_of(requested).is_none() {
        return Ok(());
    }
    let Some(sources) = sources(Some(runtime))? else {
        return Ok(());
    };
    let mut sources = sources.lock().expect("native atom source receipts");
    if !sources.reads.contains_key(requested)
        && sources.room(192 + requested.len() + collection.len() + actual.len())
    {
        sources.reads.insert(
            requested.to_string(),
            SourceKey {
                collection: collection.to_string(),
                key: actual.to_string(),
            },
        );
    }
    Ok(())
}

/// A typed decode associates immutable atom facts with that read's real source.
pub(crate) fn note_atom(
    runtime: Option<Arc<PackRuntime>>,
    requested: &str,
    atom: &Atom,
) -> StorageResult<()> {
    let Some(context) = runtime
        .as_ref()
        .and_then(|runtime| ReadContext::captured(runtime))
    else {
        return Ok(());
    };
    let Some(sources) = sources(runtime)? else {
        return Ok(());
    };
    let mut sources = sources.lock().expect("native typed atom sources");
    if sources.atoms.contains_key(requested) {
        return Ok(());
    }
    let Some(source) = sources.reads.get(requested).cloned() else {
        return Ok(());
    };
    let decoded = crate::storage::laststore::native_routing::decode(&source.key)?;
    let uuid = std::str::from_utf8(&decoded)
        .ok()
        .and_then(crate::atom::atom_key_codec::uuid_of);
    if uuid != Some(atom.uuid()) {
        return Ok(());
    }
    let Some(witness) = context.source_witness(&source.collection, &source.key) else {
        return Ok(());
    };
    let facts = AtomFacts {
        source,
        version: context.version,
        witness,
        source_file_name: atom.source_file_name().cloned(),
        metadata: atom.metadata().cloned(),
        written_at: atom
            .created_at()
            .timestamp_nanos_opt()
            .and_then(|value| u64::try_from(value).ok()),
    };
    if !facts.primary(requested) {
        return Ok(());
    }
    let bytes = super::body::encoded_len(&facts)?;
    if sources.room(192 + requested.len() + bytes) {
        sources.atoms.insert(requested.to_string(), facts);
    }
    Ok(())
}

/// Located reads retain the explicit namespace and whichever fallback actually won.
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
        if key != first || !sources.atoms.contains_key(key) {
            continue;
        }
        bind(&mut sources, scope, key, atom, hint.as_ref());
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
    if !sources.atoms.contains_key(key) {
        return;
    }
    let identity = scoped(scope, atom.uuid(), hint);
    if !sources.scoped.contains_key(&identity) && sources.room(192 + identity.len() + key.len()) {
        sources.scoped.insert(identity, key.to_string());
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
    let sources = sources.lock().expect("native selected atom facts");
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
