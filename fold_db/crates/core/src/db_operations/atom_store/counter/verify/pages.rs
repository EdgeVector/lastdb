//! The reads of the verifier. One page is one captured read scope: the rows of
//! the range, the counters and the bodies of the atoms they name are all read
//! at one committed root. Nothing here takes the write gate and nothing here
//! writes.

use super::blobs::{blobs_of_pairs, blobs_of_tips, BlobListed};
use super::report::Phase;
use crate::db_operations::atom_store::blob_counter::{counter_range, BlobQueue};
use crate::db_operations::atom_store::counter::scan::{
    family_range, record_holders, record_ids, tip_holders, CounterRead, Family, PairRead,
};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use crate::storage::traits::YieldedScan;
use std::collections::BTreeMap;

/// The keys that a page read keeps for the reads it makes for each row it gets:
/// the counter and the body of an atom, and a blob counter.
pub(super) const VERIFY_KEYS_PER_ROW: usize = 4;

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
    /// The last key the read examined, or `None` when the range ended.
    pub resume: Option<Vec<u8>>,
    /// Rows that did not decode.
    pub unreadable: u64,
    pub unreadable_keys: Vec<String>,
    pub atoms: Vec<AtomRead>,
    /// The blobs that the tips of the page name in their metadata (tips phase).
    pub blobs: Vec<BlobListed>,
    /// The ids of the counted records of the page (the records phase only): the
    /// first 128 bits of the id that the queue keys name.
    pub record_ids: Vec<u128>,
}

/// A page of the suspects phase: the keys of the counted records whose id no
/// queue entry names.
pub(super) struct SuspectsPage {
    pub rows: usize,
    /// The last key the read examined, or `None` when the range ended.
    pub resume: Option<Vec<u8>>,
    pub keys: Vec<String>,
}

/// A page of the atoms phase.
pub(super) struct AtomsPage {
    pub rows: usize,
    /// The last key the read examined, or `None` when the range ended.
    pub resume: Option<Vec<u8>>,
    /// Keys under `atom:` that are neither a body nor a counter record.
    pub foreign: u64,
    pub pairs: Vec<PairRead>,
    /// The blobs that the atom counters of the page list.
    pub blobs: Vec<BlobListed>,
}

/// Where each phase reads: the start and the exclusive end of its range.
pub(super) fn phase_range(phase: Phase, scope: Option<&str>) -> Option<(Vec<u8>, Vec<u8>)> {
    let family = match phase {
        Phase::Tips => Family::Tips,
        Phase::Records | Phase::Suspects => Family::Records,
        Phase::Atoms => Family::Atoms,
        Phase::Blobs => return Some(counter_range()),
        Phase::BlobQueue => return Some(BlobQueue::entries_range()),
        Phase::Queue | Phase::Done => return None,
    };
    Some(family_range(family, scope))
}

impl AtomStore {
    /// Read one verify page. The key budget stays [`VERIFY_KEYS_PER_ROW`].
    /// The caller keeps its own read scope.
    pub(super) async fn read_verify_page(
        &self,
        start: &[u8],
        end: &[u8],
        asked: usize,
    ) -> Result<YieldedScan, SchemaError> {
        self.raw()
            .inner()
            .scan_range_yielding(start, end, asked, VERIFY_KEYS_PER_ROW)
            .await
            .map_err(SchemaError::from)
    }

    /// Read one page of tips or of conflict records, and the counters and the
    /// bodies of the atoms they name, at one root.
    pub(super) async fn read_referenced_page(
        &self,
        phase: Phase,
        scope: Option<&str>,
        (start, end, asked): Request,
    ) -> Result<ReferencedPage, SchemaError> {
        self.atom_read_scope(async {
            let scan = self.read_verify_page(&start, &end, asked).await?;
            let (rows, resume) = (scan.rows, scan.resume);
            let (holders, bad) = match phase {
                Phase::Records => record_holders(&rows),
                _ => tip_holders(&rows),
            };
            let blob_lists = blobs_of_tips(&rows);
            let record_ids = match phase {
                Phase::Records => record_ids(&rows),
                _ => Vec::new(),
            };
            let atoms = self.read_atoms_of(holders, scope).await?;
            let blobs = self.read_blob_listed(blob_lists).await?;
            Ok(ReferencedPage {
                rows: rows.len(),
                resume,
                unreadable: bad.total,
                unreadable_keys: bad.keys,
                atoms,
                blobs,
                record_ids,
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

    /// Read one page of the conflict records, and keep the keys of the counted
    /// records whose id is in `suspects`.
    pub(super) async fn read_suspects_page(
        &self,
        suspects: &std::collections::HashSet<u128>,
        (start, end, asked): Request,
    ) -> Result<SuspectsPage, SchemaError> {
        let scan = self.read_verify_page(&start, &end, asked).await?;
        let (rows, resume) = (scan.rows, scan.resume);
        let keys = rows
            .iter()
            .map(|(key, _)| String::from_utf8_lossy(key).into_owned())
            .filter(|key| {
                crate::db_operations::atom_store::conflict_record::is_counted_record_key(key)
                    && !crate::db_operations::atom_store::atom_history::is_archive(key)
            })
            .filter(|key| {
                crate::db_operations::atom_store::record_digest(key)
                    .is_some_and(|digest| suspects.contains(&digest))
            })
            .collect();
        Ok(SuspectsPage {
            rows: rows.len(),
            resume,
            keys,
        })
    }

    /// Read one page of the `atom:` range and pair each atom's body and counter
    /// at one root.
    pub(super) async fn read_atoms_page(
        &self,
        scope: Option<&str>,
        (start, end, asked): Request,
    ) -> Result<AtomsPage, SchemaError> {
        self.atom_read_scope(async {
            let scan = self.read_verify_page(&start, &end, asked).await?;
            let (rows, resume) = (scan.rows, scan.resume);
            let paired = self.pair_rows(&rows, &start, scope).await?;
            let blobs = self.read_blob_listed(blobs_of_pairs(&paired.pairs)).await?;
            Ok(AtomsPage {
                rows: rows.len(),
                resume,
                foreign: paired.foreign,
                pairs: paired.pairs,
                blobs,
            })
        })
        .await
    }
}
