//! Interned invalidation source keys.
//!
//! Each source key is stored once, packed, and both sides of a source link
//! hold only ids. Source keys are long: a delete barrier spells its molecule
//! key in hex (`rdel:v2:6d6b3a…`), and molecule keys carry hex ids, so the
//! packed form stores each long lowercase hex run as raw bytes.

use super::tables::{slab_insert, SlotId};
use super::*;

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

pub(super) struct SourceEntry {
    /// `pack_source` form of the coordinates; the same allocation keys the index.
    packed: Arc<[u8]>,
    pub(super) slots: IdSet,
}

#[derive(Default)]
pub(super) struct SourceTable {
    index: HashMap<Arc<[u8]>, SourceId>,
    entries: Vec<Option<SourceEntry>>,
    free: Vec<SourceId>,
}

impl SourceTable {
    pub(super) fn find(&self, collection: &str, key: &str) -> Option<SourceId> {
        self.index
            .get(pack_source(collection, key).as_slice())
            .copied()
    }

    pub(super) fn get(&self, id: SourceId) -> Option<&SourceEntry> {
        self.entries.get(id as usize)?.as_ref()
    }

    pub(super) fn coordinates(&self, id: SourceId) -> (String, String) {
        unpack_source(&self.get(id).expect("linked core source").packed)
    }

    pub(super) fn packed_len(&self, id: SourceId) -> usize {
        self.get(id).map_or(0, |entry| entry.packed.len())
    }

    /// Return the id of the source and whether this call created it. A new
    /// source has no slots; the caller links it at once.
    pub(super) fn intern(&mut self, collection: &str, key: &str, slot: SlotId) -> (SourceId, bool) {
        let packed = pack_source(collection, key);
        if let Some(id) = self.index.get(packed.as_slice()) {
            return (*id, false);
        }
        debug_assert!(
            unpack_source(&packed) == (collection.to_owned(), key.to_owned()),
            "packed core source round trip"
        );
        let packed: Arc<[u8]> = Arc::from(packed);
        let entry = SourceEntry {
            packed: Arc::clone(&packed),
            slots: IdSet::One(slot),
        };
        let id = slab_insert(&mut self.entries, &mut self.free, entry);
        self.index.insert(packed, id);
        (id, true)
    }

    pub(super) fn link(&mut self, source: SourceId, slot: SlotId) {
        if let Some(entry) = self
            .entries
            .get_mut(source as usize)
            .and_then(Option::as_mut)
        {
            entry.slots.insert(slot);
        }
    }

    /// Remove one link. Returns the packed length of the source when this was
    /// its last slot and the source left the table.
    pub(super) fn unlink(&mut self, source: SourceId, slot: SlotId) -> Option<usize> {
        let entry = self.entries.get_mut(source as usize)?.as_mut()?;
        if !entry.slots.remove(slot) {
            return None;
        }
        let entry = self.entries[source as usize].take()?;
        self.index.remove(&entry.packed);
        self.free.push(source);
        Some(entry.packed.len())
    }

    pub(super) fn try_reserve(&mut self, additional: usize) -> Result<(), CoreCapacity> {
        self.index
            .try_reserve(additional)
            .and_then(|_| self.free.try_reserve(additional))
            .and_then(|_| self.entries.try_reserve(additional))
            .map_err(|_| super::model::capacity())
    }
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
