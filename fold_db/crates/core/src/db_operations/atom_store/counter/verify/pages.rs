//! The reads of the verifier. One page is one captured read scope: the rows of
//! the range, the counters and the bodies of the atoms they name are all read
//! at one committed root. Nothing here takes the write gate and nothing here
//! writes.

use super::report::Phase;
use crate::db_operations::atom_store::counter::scan::{
    family_range, record_holders, tip_holders, CounterRead, Family, PairRead,
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
    /// Rows that did not decode.
    pub unreadable: u64,
    pub unreadable_keys: Vec<String>,
    pub atoms: Vec<AtomRead>,
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
    let family = match phase {
        Phase::Tips => Family::Tips,
        Phase::Records => Family::Records,
        Phase::Atoms => Family::Atoms,
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

    /// Read one page of the `atom:` range and pair each atom's body and counter
    /// at one root.
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
            let (pairs, foreign) = self.pair_rows(&rows, &start, scope).await?;
            Ok(AtomsPage {
                rows: rows.len(),
                last_key,
                foreign,
                pairs,
            })
        })
        .await
    }
}
