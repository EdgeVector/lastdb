//! Keep audited candidate roots and their reclaim fence through the final flush.

use super::*;
use crate::db_operations::atom_store::{AtomGcCandidatePage, AtomGcReapReport};

pub(crate) struct RetiredAtomGcPage {
    _reclaim: OwnedRwLockReadGuard<()>,
    roots: HashSet<String>,
}

impl RetiredAtomGcPage {
    pub(crate) fn retains(&self, atom_uuid: &str, report: &mut AtomGcReapReport) -> bool {
        if !self.roots.contains(atom_uuid) {
            return false;
        }
        report.retained_retired_intent_reference =
            report.retained_retired_intent_reference.saturating_add(1);
        true
    }
}

impl AtomStore {
    pub(crate) async fn retired_gc_page(
        &self,
        page: &AtomGcCandidatePage,
        storage_prefix: Option<&str>,
    ) -> Result<RetiredAtomGcPage, SchemaError> {
        let reclaim = self.retired_intent_reclaim_guard().await;
        let ids: Vec<String> = page
            .candidates
            .iter()
            .map(|candidate| candidate.atom_uuid.clone())
            .collect();
        let roots = self.retired_intent_atom_roots(&ids, storage_prefix).await?;
        Ok(RetiredAtomGcPage {
            _reclaim: reclaim,
            roots,
        })
    }
}
