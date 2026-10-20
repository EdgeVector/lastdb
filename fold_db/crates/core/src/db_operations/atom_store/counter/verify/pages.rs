//! The reads of the verifier. One page is one captured read scope: the rows of
//! the range, the counters and the bodies of the atoms they name are all read
//! at one committed root. Nothing here takes the write gate and nothing here
//! writes.

use super::check::CounterRead;
use super::report::Phase;
use crate::atom::{atom_key_codec, MutationEvent};
use crate::db_operations::atom_store::conflict_record::is_counted_record_key;
use crate::db_operations::atom_store::counter::event_atoms;
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use std::collections::BTreeMap;

/// The most keys of unreadable rows a page keeps for the sample list.
const UNREADABLE_KEYS: usize = 4;

/// A read the walk asked for: start, end and the number of rows.
pub(super) type Request = (Vec<u8>, Vec<u8>, usize);

/// One atom that tips or records of a page name, with what the root holds for it.
pub(super) struct AtomRead {
    pub atom: String,
    /// Tips or records of this page that name the atom.
    pub holders: u32,
    pub counter: CounterRead,
    pub body: bool,
}

/// A page of the tips or records phase.
pub(super) struct ReferencedPage {
    pub rows: usize,
    pub last_key: Option<Vec<u8>>,
    /// Rows that did not decode.
    pub unreadable: u64,
    pub unreadable_keys: Vec<String>,
    pub atoms: Vec<AtomRead>,
}

/// The body and counter of one atom, read at one root.
pub(super) struct PairRead {
    pub atom: String,
    pub body: bool,
    pub counter: CounterRead,
}

/// A page of the atoms phase.
pub(super) struct AtomsPage {
    pub rows: usize,
    pub last_key: Option<Vec<u8>>,
    /// Keys under `atom:` that are neither a body nor a counter record.
    pub foreign: u64,
    pub pairs: Vec<PairRead>,
}

/// Where each phase reads: the start and the exclusive end of its range.
pub(super) fn phase_range(phase: Phase, scope: Option<&str>) -> Option<(Vec<u8>, Vec<u8>)> {
    let base = match phase {
        Phase::Tips => "mk:",
        Phase::Records => "history\0",
        Phase::Atoms => "atom:",
        Phase::Done => return None,
    };
    let start = build_storage_key(scope, base);
    let end = crate::schema::types::field::FilterUtils::create_prefix_end(&start).ok()?;
    Some((start.into_bytes(), end.into_bytes()))
}

fn key_text(key: &[u8]) -> String {
    String::from_utf8_lossy(key).replace('\0', "\\0")
}

impl AtomStore {
    /// Read one page of tips or of conflict records, and the counters and the
    /// bodies of the atoms they name, at one root.
    pub(super) async fn read_referenced_page(
        &self,
        phase: Phase,
        scope: Option<&str>,
        (start, end, asked): Request,
    ) -> Result<ReferencedPage, SchemaError> {
        self.atom_read_scope(async {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(&start, &end, asked)
                .await
                .map_err(SchemaError::from)?;
            let last_key = rows.last().map(|(key, _)| key.clone());
            let (holders, bad) = match phase {
                Phase::Records => record_holders(&rows),
                _ => tip_holders(&rows),
            };
            let atoms = self.read_atoms_of(holders, scope).await?;
            Ok(ReferencedPage {
                rows: rows.len(),
                last_key,
                unreadable: bad.total,
                unreadable_keys: bad.keys,
                atoms,
            })
        })
        .await
    }

    async fn read_atoms_of(
        &self,
        holders: BTreeMap<String, u32>,
        scope: Option<&str>,
    ) -> Result<Vec<AtomRead>, SchemaError> {
        let atoms: Vec<String> = holders.keys().cloned().collect();
        let counters = self.read_counter_bytes(&atoms, scope).await?;
        let bodies = self.read_body_flags(&atoms, scope).await?;
        Ok(atoms
            .into_iter()
            .zip(counters)
            .zip(bodies)
            .map(|((atom, counter), body)| AtomRead {
                holders: holders[&atom],
                counter: CounterRead::parse(counter.as_deref()),
                body,
                atom,
            })
            .collect())
    }

    async fn read_counter_bytes(
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

    async fn read_body_flags(
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

    /// Read one page of the `atom:` range and pair each atom's body and counter
    /// at one root. A partner that lies outside the page is read by key.
    ///
    /// An atom belongs to the page that holds the first key it could have, its
    /// body key. A page that starts after the body key skips the atom, because
    /// the page before it read the atom whole.
    pub(super) async fn read_atoms_page(
        &self,
        scope: Option<&str>,
        (start, end, asked): Request,
    ) -> Result<AtomsPage, SchemaError> {
        self.atom_read_scope(async {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(&start, &end, asked)
                .await
                .map_err(SchemaError::from)?;
            let last_key = rows.last().map(|(key, _)| key.clone());
            let (mut groups, foreign) = group_rows(&rows);
            groups.retain(|group| {
                build_storage_key(scope, &atom_key_codec::body_key(&group.atom)).as_bytes()
                    >= start.as_slice()
            });
            self.complete_groups(&mut groups, scope).await?;
            Ok(AtomsPage {
                rows: rows.len(),
                last_key,
                foreign,
                pairs: groups
                    .into_iter()
                    .map(|group| PairRead {
                        counter: CounterRead::parse(group.counter.as_deref()),
                        body: group.body,
                        atom: group.atom,
                    })
                    .collect(),
            })
        })
        .await
    }

    /// Read the body or the counter that the range did not show.
    async fn complete_groups(
        &self,
        groups: &mut [Group],
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        let no_body: Vec<usize> = (0..groups.len()).filter(|at| !groups[*at].body).collect();
        let no_counter: Vec<usize> = (0..groups.len())
            .filter(|at| groups[*at].counter.is_none())
            .collect();
        let named = |at: &[usize]| -> Vec<String> {
            at.iter().map(|slot| groups[*slot].atom.clone()).collect()
        };
        let (body_atoms, counter_atoms) = (named(&no_body), named(&no_counter));
        let bodies = self.read_body_flags(&body_atoms, scope).await?;
        let counters = self.read_counter_bytes(&counter_atoms, scope).await?;
        for (slot, found) in no_body.into_iter().zip(bodies) {
            groups[slot].body = found;
        }
        for (slot, bytes) in no_counter.into_iter().zip(counters) {
            groups[slot].counter = bytes;
        }
        Ok(())
    }
}

/// The rows of one atom in a page.
struct Group {
    atom: String,
    body: bool,
    counter: Option<Vec<u8>>,
}

/// Group the rows of the `atom:` range by atom. The body key sorts before the
/// counter key, so the rows of one atom are adjacent.
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

/// Rows that did not decode.
#[derive(Default)]
struct Unreadable {
    total: u64,
    keys: Vec<String>,
}

impl Unreadable {
    fn note(&mut self, key: &[u8]) {
        self.total += 1;
        if self.keys.len() < UNREADABLE_KEYS {
            self.keys.push(key_text(key));
        }
    }
}

/// The tips of a page, counted by the atom they name.
fn tip_holders(rows: &[(Vec<u8>, Vec<u8>)]) -> (BTreeMap<String, u32>, Unreadable) {
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

/// The conflict records of a page, counted by the atoms they hold. A history
/// row that no recorder wrote as a counted record holds no reference.
fn record_holders(rows: &[(Vec<u8>, Vec<u8>)]) -> (BTreeMap<String, u32>, Unreadable) {
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
