//! The reads of the verifier. One page is one captured read scope: the rows of
//! the range, the counters and the bodies of the atoms they name are all read
//! at one committed root. Nothing here takes the write gate and nothing here
//! writes.

use super::blobs::{blobs_of_pairs, blobs_of_tips, BlobListed};
use super::report::Phase;
use crate::crypto::count_unsealed_discards;
use crate::db_operations::atom_store::blob_counter::{counter_range, BlobQueue};
use crate::db_operations::atom_store::counter::scan::{
    family_range, record_holders, record_molecules, tip_holders, tip_molecules, CounterRead,
    Family, PairRead,
};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use std::collections::BTreeMap;

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
    /// Rows the scan dropped for want of an at-rest envelope. They count toward
    /// a full page.
    pub scan_unsealed: u64,
    /// Counter and body reads that dropped such a row. The row reads as absent.
    pub point_unsealed: u64,
    /// Rows that did not decode.
    pub unreadable: u64,
    pub unreadable_keys: Vec<String>,
    pub atoms: Vec<AtomRead>,
    /// The blobs that the tips of the page name in their metadata (tips phase).
    pub blobs: Vec<BlobListed>,
    /// Rows of the page for each molecule (tips, or counted conflict records).
    pub molecules: BTreeMap<String, u32>,
}

/// A page of the atoms phase.
pub(super) struct AtomsPage {
    pub rows: usize,
    pub last_key: Option<Vec<u8>>,
    pub scan_unsealed: u64,
    pub point_unsealed: u64,
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
        Phase::Records => Family::Records,
        Phase::Atoms => Family::Atoms,
        Phase::Headers => Family::Headers,
        Phase::Bindings => Family::BindingCounters,
        Phase::BindingRecords => Family::BindingRecords,
        Phase::Blobs => return Some(counter_range()),
        Phase::BlobQueue => return Some(BlobQueue::entries_range()),
        Phase::Done => return None,
    };
    Some(family_range(family, scope))
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
            let (rows, scan_unsealed) =
                count_unsealed_discards(self.raw().inner().scan_range_paged(&start, &end, asked))
                    .await;
            let rows = rows.map_err(SchemaError::from)?;
            let last_key = rows.last().map(|(key, _)| key.clone());
            let (holders, bad) = match phase {
                Phase::Records => record_holders(&rows),
                _ => tip_holders(&rows),
            };
            let blob_lists = if matches!(phase, Phase::Tips) {
                blobs_of_tips(&rows)
            } else {
                BTreeMap::new()
            };
            let molecules = match phase {
                Phase::Records => record_molecules(&rows),
                _ => tip_molecules(&rows),
            };
            let (atoms, point_unsealed) =
                count_unsealed_discards(self.read_atoms_of(holders, scope)).await;
            let blobs = self.read_blob_listed(blob_lists).await?;
            Ok(ReferencedPage {
                rows: rows.len(),
                last_key,
                scan_unsealed,
                point_unsealed,
                unreadable: bad.total,
                unreadable_keys: bad.keys,
                atoms: atoms?,
                blobs,
                molecules,
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

    /// Read one page of the `atom:` range and pair each atom's body and counter
    /// at one root.
    pub(super) async fn read_atoms_page(
        &self,
        scope: Option<&str>,
        (start, end, asked): Request,
    ) -> Result<AtomsPage, SchemaError> {
        self.atom_read_scope(async {
            let (rows, scan_unsealed) =
                count_unsealed_discards(self.raw().inner().scan_range_paged(&start, &end, asked))
                    .await;
            let rows = rows.map_err(SchemaError::from)?;
            let last_key = rows.last().map(|(key, _)| key.clone());
            let paired = self.pair_rows(&rows, &start, scope).await?;
            let blobs = self.read_blob_listed(blobs_of_pairs(&paired.pairs)).await?;
            Ok(AtomsPage {
                rows: rows.len(),
                last_key,
                scan_unsealed,
                point_unsealed: paired.unsealed,
                foreign: paired.foreign,
                pairs: paired.pairs,
                blobs,
            })
        })
        .await
    }
}
