//! Interned invalidation source keys.
//!
//! Each source key is stored once, packed, and both sides of a source link
//! hold only ids. Source keys are long: a delete barrier spells its molecule
//! key in hex (`rdel:v2:6d6b3a…`), and molecule keys carry hex ids, so the
//! packed form stores each long lowercase hex run as raw bytes.

use super::slab::Slab;
use super::tables::SlotId;
use super::*;
use hashbrown::HashTable;
use std::hash::{BuildHasher, RandomState};

pub(super) type SourceId = u32;

/// Above this many members a source's slot set switches to a hash set.
const FEW_SLOTS: usize = 16;
/// Pack lowercase hex runs of at least this many characters.
const HEX_RUN: usize = 16;
/// Marks a packed hex chunk. It never occurs in UTF-8 text.
const HEX_MARK: u8 = 0xFF;

/// Slots of one source. Most sources belong to one slot; a molecule control
/// source belongs to every warm slot of its molecule, so a large set hashes.
pub(super) enum IdSet {
    One(SlotId),
    Few(Vec<SlotId>),
    Many(Box<ManySlots>),
}

/// Boxed so the common `One` entry stays two words; only shared control
/// sources pay for the hash set.
pub(super) struct ManySlots(HashSet<SlotId>);

impl IdSet {
    fn insert(&mut self, id: SlotId) {
        match self {
            Self::One(existing) if *existing == id => {}
            Self::One(existing) => *self = Self::Few(vec![*existing, id]),
            Self::Few(ids) if ids.contains(&id) => {}
            Self::Few(ids) if ids.len() < FEW_SLOTS => ids.push(id),
            Self::Few(ids) => {
                let mut many: HashSet<_> = ids.iter().copied().collect();
                many.insert(id);
                *self = Self::Many(Box::new(ManySlots(many)));
            }
            Self::Many(ids) => {
                ids.0.insert(id);
            }
        }
    }

    /// Returns true when the set is empty after the removal.
    fn remove(&mut self, id: SlotId) -> bool {
        match self {
            Self::One(existing) => *existing == id,
            Self::Few(ids) => {
                ids.retain(|existing| *existing != id);
                ids.is_empty()
            }
            Self::Many(ids) => {
                ids.0.remove(&id);
                ids.0.is_empty()
            }
        }
    }

    pub(super) fn iter(&self) -> Box<dyn Iterator<Item = SlotId> + '_> {
        match self {
            Self::One(id) => Box::new(std::iter::once(*id)),
            Self::Few(ids) => Box::new(ids.iter().copied()),
            Self::Many(ids) => Box::new(ids.0.iter().copied()),
        }
    }
}

/// How one source key is stored.
enum StoredKey {
    /// The `pack_source` form of the coordinates.
    Packed(Arc<[u8]>),
    /// A delete barrier (`rdel:v2:`) spells its `mk:` key in hex two times, so
    /// it is stored as the `pack_source` form of that `mk:` key. The `Arc` is
    /// shared with the `mk:` source when both are interned.
    Barrier(Arc<[u8]>),
}

pub(super) struct SourceEntry {
    key: StoredKey,
    /// The hash of the full coordinates. The index stores only ids.
    hash: u64,
    pub(super) slots: IdSet,
}

#[derive(Default)]
pub(super) struct SourceTable {
    index: HashTable<SourceId>,
    hasher: RandomState,
    entries: Slab<SourceEntry>,
}

/// Lazily packed forms of one lookup, so a probe packs at most once.
struct Probe<'a> {
    collection: &'a str,
    key: &'a str,
    packed: Option<Vec<u8>>,
    barrier: Option<Option<Vec<u8>>>,
}

impl<'a> Probe<'a> {
    fn new(collection: &'a str, key: &'a str) -> Self {
        Self {
            collection,
            key,
            packed: None,
            barrier: None,
        }
    }

    fn matches(&mut self, stored: &StoredKey) -> bool {
        match stored {
            StoredKey::Packed(packed) => {
                let probe = self
                    .packed
                    .get_or_insert_with(|| pack_source(self.collection, self.key));
                packed.as_ref() == probe.as_slice()
            }
            StoredKey::Barrier(base) => {
                let probe = self.barrier.get_or_insert_with(|| {
                    barrier_base(self.key).map(|mk| pack_source(self.collection, &mk))
                });
                probe.as_deref() == Some(base.as_ref())
            }
        }
    }
}

impl SourceTable {
    fn hash(&self, collection: &str, key: &str) -> u64 {
        self.hasher.hash_one((collection, key))
    }

    pub(super) fn find(&self, collection: &str, key: &str) -> Option<SourceId> {
        let hash = self.hash(collection, key);
        let mut probe = Probe::new(collection, key);
        let entries = &self.entries;
        self.index
            .find(hash, |id| {
                entries
                    .get(*id)
                    .is_some_and(|entry| entry.hash == hash && probe.matches(&entry.key))
            })
            .copied()
    }

    pub(super) fn get(&self, id: SourceId) -> Option<&SourceEntry> {
        self.entries.get(id)
    }

    pub(super) fn coordinates(&self, id: SourceId) -> (String, String) {
        coordinates(&self.get(id).expect("linked core source").key)
    }

    /// The key bytes charged to this source. A barrier is charged nothing for
    /// its key: its bytes are those of the `mk:` source that it shares.
    pub(super) fn charged_len(&self, id: SourceId) -> usize {
        self.get(id).map_or(0, |entry| charged_len(&entry.key))
    }

    /// Return the id of the source and whether this call created it. A new
    /// source has no slots; the caller links it at once.
    pub(super) fn intern(&mut self, collection: &str, key: &str, slot: SlotId) -> (SourceId, bool) {
        if let Some(id) = self.find(collection, key) {
            return (id, false);
        }
        let stored = match barrier_base(key) {
            Some(mk) => StoredKey::Barrier(self.shared_mk(collection, &mk, None)),
            None => StoredKey::Packed(self.shared_mk(collection, key, Some(key))),
        };
        debug_assert!(
            coordinates(&stored) == (collection.to_owned(), key.to_owned()),
            "stored core source round trip"
        );
        let hash = self.hash(collection, key);
        let entry = SourceEntry {
            key: stored,
            hash,
            slots: IdSet::One(slot),
        };
        let id = self.entries.insert(entry);
        let entries = &self.entries;
        self.index
            .insert_unique(hash, id, |id| entry_hash(entries, *id));
        (id, true)
    }

    /// The packed `(collection, mk)` form, shared with an interned source
    /// that already stores it: the `mk:` source itself (`plain` is None) or
    /// the barrier of a new `mk:` source (`plain` is the new key).
    fn shared_mk(&self, collection: &str, mk: &str, plain: Option<&str>) -> Arc<[u8]> {
        let other = match plain {
            None => self.find(collection, mk),
            Some(key) if key.contains("mk:") => {
                let barrier = crate::atom::delete_barrier::delete_barrier_key(key.as_bytes());
                self.find(collection, &barrier)
            }
            Some(_) => None,
        };
        let shared = other.and_then(|id| match (&self.get(id)?.key, plain) {
            (StoredKey::Packed(packed), None) | (StoredKey::Barrier(packed), Some(_)) => {
                Some(Arc::clone(packed))
            }
            _ => None,
        });
        shared.unwrap_or_else(|| Arc::from(pack_source(collection, mk)))
    }

    pub(super) fn link(&mut self, source: SourceId, slot: SlotId) {
        if let Some(entry) = self.entries.get_mut(source) {
            entry.slots.insert(slot);
        }
    }

    /// Remove one link. Returns the charged key length of the source when
    /// this was its last slot and the source left the table.
    pub(super) fn unlink(&mut self, source: SourceId, slot: SlotId) -> Option<usize> {
        let entry = self.entries.get_mut(source)?;
        if !entry.slots.remove(slot) {
            return None;
        }
        let entry = self.entries.take(source)?;
        if let Ok(found) = self.index.find_entry(entry.hash, |id| *id == source) {
            found.remove();
        }
        Some(charged_len(&entry.key))
    }

    pub(super) fn try_reserve(&mut self, additional: usize) -> Result<(), CoreCapacity> {
        let entries = &self.entries;
        self.index
            .try_reserve(additional, |id| entry_hash(entries, *id))
            .map_err(|_| super::model::capacity())?;
        self.entries
            .try_reserve(additional)
            .map_err(|_| super::model::capacity())
    }
}

fn entry_hash(entries: &Slab<SourceEntry>, id: SourceId) -> u64 {
    entries.get(id).expect("indexed core source").hash
}

fn coordinates(stored: &StoredKey) -> (String, String) {
    match stored {
        StoredKey::Packed(packed) => unpack_source(packed),
        StoredKey::Barrier(base) => {
            let (collection, mk) = unpack_source(base);
            let key = crate::atom::delete_barrier::delete_barrier_key(mk.as_bytes());
            (collection, key)
        }
    }
}

fn charged_len(stored: &StoredKey) -> usize {
    match stored {
        StoredKey::Packed(packed) => packed.len(),
        StoredKey::Barrier(_) => 0,
    }
}

/// The `mk:` key of a delete barrier key, only when the barrier of that `mk:`
/// key is exactly `key`. Any other key is stored packed.
fn barrier_base(key: &str) -> Option<String> {
    let marker = key.find("rdel:v2:")?;
    let (_, tail) = key[marker..].split_once('\0')?;
    let (len, hex) = tail.split_once(':')?;
    let bytes = lastdb_model::hex::hex_decode(hex)?;
    if len.parse::<usize>().ok()? != bytes.len() {
        return None;
    }
    let mk = String::from_utf8(bytes).ok()?;
    (crate::atom::delete_barrier::delete_barrier_key(mk.as_bytes()) == key).then_some(mk)
}

/// The collection length, the collection, then the packed key. Packing is
/// deterministic, so equal coordinates always have equal packed forms.
fn pack_source(collection: &str, key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(collection.len() + key.len() + 2);
    push_len(&mut out, collection.len());
    out.extend_from_slice(collection.as_bytes());
    let bytes = key.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        let run = bytes[at..]
            .iter()
            .take_while(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            .count()
            & !1;
        if run < HEX_RUN {
            // Copy up to the next hex run in one step.
            let literal = bytes[at..]
                .iter()
                .skip(1)
                .position(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                .map_or(bytes.len() - at, |skip| skip + 1);
            out.extend_from_slice(&bytes[at..at + literal]);
            at += literal;
            continue;
        }
        let pairs = &bytes[at..at + run];
        out.push(HEX_MARK);
        push_len(&mut out, run / 2);
        out.extend(
            pairs
                .chunks(2)
                .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1])),
        );
        at += run;
    }
    out
}

fn unpack_source(packed: &[u8]) -> (String, String) {
    let (collection_len, mut at) = read_len(packed, 0);
    let collection = &packed[at..at + collection_len];
    at += collection_len;
    let mut key = Vec::with_capacity(2 * (packed.len() - at));
    while at < packed.len() {
        if packed[at] != HEX_MARK {
            key.push(packed[at]);
            at += 1;
            continue;
        }
        let (bytes, start) = read_len(packed, at + 1);
        for byte in &packed[start..start + bytes] {
            key.push(HEX_DIGITS[usize::from(byte >> 4)]);
            key.push(HEX_DIGITS[usize::from(byte & 0xF)]);
        }
        at = start + bytes;
    }
    (
        String::from_utf8(collection.to_vec()).expect("packed core source collection"),
        String::from_utf8(key).expect("packed core source key"),
    )
}

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

fn nibble(digit: u8) -> u8 {
    match digit {
        b'0'..=b'9' => digit - b'0',
        _ => digit - b'a' + 10,
    }
}

/// LEB128 length. Its bytes stay below 0x80 or carry the high bit with a
/// continuation; it is only read at positions where a length is expected.
fn push_len(out: &mut Vec<u8>, mut len: usize) {
    loop {
        let low = (len & 0x7F) as u8;
        len >>= 7;
        if len == 0 {
            out.push(low);
            return;
        }
        out.push(low | 0x80);
    }
}

fn read_len(bytes: &[u8], mut at: usize) -> (usize, usize) {
    let mut len = 0;
    let mut shift = 0;
    loop {
        let byte = bytes[at];
        at += 1;
        len |= usize::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return (len, at);
        }
        shift += 7;
    }
}
