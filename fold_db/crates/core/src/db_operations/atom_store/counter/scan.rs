//! Reads shared by the verifier and the offline recount: the key ranges that
//! hold references, the holders of a page of rows, and the pairing of the body
//! and the counter of an atom. Nothing here writes.

use crate::atom::{atom_key_codec, MutationEvent};
use crate::db_operations::atom_store::conflict_record::is_counted_record_key;
use crate::db_operations::atom_store::counter::{event_atoms, AtomCounter};
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use std::collections::BTreeMap;

/// The most keys of unreadable rows a page keeps for the sample list.
const UNREADABLE_KEYS: usize = 4;

/// The key families that hold the references of atoms, and the atoms themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Family {
    /// Tip records `mk:`.
    Tips,
    /// History rows, of which the conflict records hold references.
    Records,
    /// Body records and counter records, `atom:`.
    Atoms,
    /// Molecule headers `mh:`.
    Headers,
    /// `bindings` counters of molecules, `mref:v1:n:`.
    BindingCounters,
    /// Binding records of molecules, `mref:v1:e:`.
    BindingRecords,
}

/// The start and the exclusive end of the range of a family in a scope.
pub(super) fn family_range(family: Family, scope: Option<&str>) -> (Vec<u8>, Vec<u8>) {
    let base = match family {
        Family::Tips => "mk:",
        Family::Records => "history\0",
        Family::Atoms => "atom:",
        Family::Headers => "mh:",
        Family::BindingCounters => "mref:v1:n:",
        Family::BindingRecords => "mref:v1:e:",
    };
    let start = build_storage_key(scope, base);
    let end =
        FilterUtils::create_prefix_end(&start).expect("a family prefix has a finite upper bound");
    (start.into_bytes(), end.into_bytes())
}

/// What a read found for the counter record of an atom.
pub(super) enum CounterRead {
    Absent,
    /// The record exists and does not decode.
    Unreadable,
    Present(AtomCounter),
}

impl CounterRead {
    pub(super) fn parse(bytes: Option<&[u8]>) -> Self {
        match bytes {
            None => Self::Absent,
            Some(bytes) => serde_json::from_slice(bytes).map_or(Self::Unreadable, Self::Present),
        }
    }
}

/// The body and counter of one atom, read together.
pub(super) struct PairRead {
    pub atom: String,
    pub body: bool,
    pub counter: CounterRead,
}

/// The pairs of one page of the `atom:` range.
pub(super) struct Paired {
    pub pairs: Vec<PairRead>,
    /// Keys under `atom:` that are neither a body nor a counter.
    pub foreign: u64,
    /// Partner reads that dropped a row with no at-rest envelope. Such a row
    /// reads as absent, so the pair shows a missing body or counter.
    pub unsealed: u64,
}

/// Rows that did not decode.
#[derive(Default)]
pub(super) struct Unreadable {
    pub total: u64,
    pub keys: Vec<String>,
}

impl Unreadable {
    fn note(&mut self, key: &[u8]) {
        self.total += 1;
        if self.keys.len() < UNREADABLE_KEYS {
            self.keys
                .push(String::from_utf8_lossy(key).replace('\0', "\\0"));
        }
    }
}

/// The tips of a page, counted by the atom they name.
pub(super) fn tip_holders(rows: &[(Vec<u8>, Vec<u8>)]) -> (BTreeMap<String, u32>, Unreadable) {
    let mut holders = BTreeMap::new();
    let mut bad = Unreadable::default();
    for (key, value) in rows {
        match serde_json::from_slice::<PerKeyRecord>(value) {
            Ok(tip) if tip.entry.atom_uuid.is_empty() => {}
            Ok(tip) => *holders.entry(tip.entry.atom_uuid).or_insert(0u32) += 1,
            Err(_) => bad.note(key),
        }
    }
    (holders, bad)
}

/// The tips of a page, counted by their molecule.
pub(super) fn tip_molecules(rows: &[(Vec<u8>, Vec<u8>)]) -> BTreeMap<String, u32> {
    let mut molecules = BTreeMap::new();
    for (key, _) in rows {
        if let Some(molecule) = crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(
            &String::from_utf8_lossy(key),
        ) {
            *molecules.entry(molecule.to_string()).or_insert(0u32) += 1;
        }
    }
    molecules
}

/// The counted conflict records of a page, counted by their molecule. The key
/// `history\0{molecule}:{ts}:{tag}:{digest}` names the molecule.
pub(super) fn record_molecules(rows: &[(Vec<u8>, Vec<u8>)]) -> BTreeMap<String, u32> {
    let mut molecules = BTreeMap::new();
    for (key, _) in rows {
        let key = String::from_utf8_lossy(key);
        if !is_counted_record_key(&key) {
            continue;
        }
        let molecule =
            crate::kind_partition::rest_of(&key, "history").and_then(|rest| rest.split(':').next());
        if let Some(molecule) = molecule.filter(|molecule| !molecule.is_empty()) {
            *molecules.entry(molecule.to_string()).or_insert(0u32) += 1;
        }
    }
    molecules
}

/// The conflict records of a page, counted by the atoms they hold. A history
/// row that no recorder wrote as a counted record holds no reference.
pub(super) fn record_holders(rows: &[(Vec<u8>, Vec<u8>)]) -> (BTreeMap<String, u32>, Unreadable) {
    let mut holders = BTreeMap::new();
    let mut bad = Unreadable::default();
    for (key, value) in rows {
        if !is_counted_record_key(&String::from_utf8_lossy(key)) {
            continue;
        }
        match serde_json::from_slice::<MutationEvent>(value) {
            Ok(event) => {
                for atom in event_atoms(&event)
                    .into_iter()
                    .filter(|atom| !atom.is_empty())
                {
                    *holders.entry(atom).or_insert(0u32) += 1;
                }
            }
            Err(_) => bad.note(key),
        }
    }
    (holders, bad)
}

/// The rows of one atom in a page.
struct Group {
    atom: String,
    body: bool,
    counter: Option<Vec<u8>>,
}

/// Group the rows of the `atom:` range by atom. The body key sorts before the
/// counter key, so the rows of one atom are adjacent. Returns the groups and
/// the number of keys that are neither a body nor a counter.
fn group_rows(rows: &[(Vec<u8>, Vec<u8>)]) -> (Vec<Group>, u64) {
    let mut groups: Vec<Group> = Vec::new();
    let mut foreign = 0;
    for (key, value) in rows {
        let text = String::from_utf8_lossy(key);
        let Some(atom) = atom_key_codec::uuid_of(&text) else {
            foreign += 1;
            continue;
        };
        if groups.last().is_none_or(|group| group.atom != atom) {
            groups.push(Group {
                atom: atom.to_string(),
                body: false,
                counter: None,
            });
        }
        let group = groups.last_mut().expect("a group was just pushed");
        if atom_key_codec::is_counter_key(&text) {
            group.counter = Some(value.clone());
        } else {
            group.body = true;
        }
    }
    (groups, foreign)
}

impl AtomStore {
    pub(super) async fn read_counter_bytes(
        &self,
        atoms: &[String],
        scope: Option<&str>,
    ) -> Result<Vec<Option<Vec<u8>>>, SchemaError> {
        let keys = atoms
            .iter()
            .map(|atom| build_storage_key(scope, &atom_key_codec::counter_key(atom)).into_bytes())
            .collect();
        self.raw()
            .inner()
            .get_many(keys)
            .await
            .map_err(SchemaError::from)
    }

    pub(super) async fn read_body_flags(
        &self,
        atoms: &[String],
        scope: Option<&str>,
    ) -> Result<Vec<bool>, SchemaError> {
        let keys = atoms
            .iter()
            .map(|atom| build_storage_key(scope, &atom_key_codec::body_key(atom)).into_bytes())
            .collect();
        self.raw()
            .inner()
            .exists_many(keys)
            .await
            .map_err(SchemaError::from)
    }

    /// Pair the body and the counter of each atom in a page of the `atom:`
    /// range. A partner that lies outside the page is read by key, in the same
    /// read scope as the page when the caller holds one.
    ///
    /// An atom belongs to the page that holds the first key it could have, its
    /// body key. A page that starts after the body key skips the atom, because
    /// the page before it paired the atom whole.
    pub(super) async fn pair_rows(
        &self,
        rows: &[(Vec<u8>, Vec<u8>)],
        start: &[u8],
        scope: Option<&str>,
    ) -> Result<Paired, SchemaError> {
        let (mut groups, foreign) = group_rows(rows);
        groups.retain(|group| {
            build_storage_key(scope, &atom_key_codec::body_key(&group.atom)).as_bytes() >= start
        });
        let no_body: Vec<usize> = (0..groups.len()).filter(|at| !groups[*at].body).collect();
        let no_counter: Vec<usize> = (0..groups.len())
            .filter(|at| groups[*at].counter.is_none())
            .collect();
        let named = |at: &[usize]| -> Vec<String> {
            at.iter().map(|slot| groups[*slot].atom.clone()).collect()
        };
        let (body_atoms, counter_atoms) = (named(&no_body), named(&no_counter));
        let (partners, unsealed) = crate::crypto::count_unsealed_discards(async {
            let bodies = self.read_body_flags(&body_atoms, scope).await?;
            let counters = self.read_counter_bytes(&counter_atoms, scope).await?;
            Ok::<_, SchemaError>((bodies, counters))
        })
        .await;
        let (bodies, counters) = partners?;
        for (slot, found) in no_body.into_iter().zip(bodies) {
            groups[slot].body = found;
        }
        for (slot, bytes) in no_counter.into_iter().zip(counters) {
            groups[slot].counter = bytes;
        }
        let pairs = groups
            .into_iter()
            .map(|group| PairRead {
                counter: CounterRead::parse(group.counter.as_deref()),
                body: group.body,
                atom: group.atom,
            })
            .collect();
        Ok(Paired {
            pairs,
            foreign,
            unsealed,
        })
    }
}
