//! Purge history by the requested keys, including keys with no current tip.
use super::targets::{candidate_plans, LoadedTargets};
use super::*;
use crate::db_operations::atom_store::counter::{TipChangeSet, TipCommit};

pub(super) async fn release_key_history(
    db: &Arc<DbOperations>,
    loaded: &LoadedTargets,
    spared: &HashSet<usize>,
    scope: Option<&str>,
) -> Result<TipCommit, SchemaError> {
    let plans = candidate_plans(db, &loaded.schema, &loaded.keys)?;
    let mut slots: std::collections::BTreeMap<String, Vec<(String, String)>> =
        std::collections::BTreeMap::new();
    for (index, candidate) in plans {
        if !spared.contains(&index) {
            slots
                .entry(candidate.molecule_uuid)
                .or_default()
                .push((candidate.hash, candidate.range));
        }
    }
    let slots: Vec<_> = slots
        .iter()
        .map(|(m, keys)| (m.as_str(), keys.as_slice()))
        .collect();
    let release = db
        .atoms()
        .slot_release_requests(scope, &slots, false)
        .await?;
    db.atoms()
        .commit_tip_changes(TipChangeSet {
            scope,
            releases: release.releases,
            rows: release.rows,
            ..TipChangeSet::default()
        })
        .await
}
