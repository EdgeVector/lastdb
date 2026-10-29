//! Purge history by the requested keys, including keys with no current tip.
use super::targets::{candidate_plans, LoadedTargets};
use super::*;
use crate::db_operations::atom_store::counter::{TipChangeSet, TipCommit};

pub(in crate::fold_db_core) async fn history_key_membership(
    db: &Arc<DbOperations>,
    schema: &crate::schema::types::Schema,
    keys: &[KeyValue],
    scope: Option<&str>,
) -> Result<Vec<bool>, SchemaError> {
    let mut schema = schema.clone();
    crate::fold_db_core::mutation_manager::helpers::clear_storage_prefix_on_schema(&mut schema);
    crate::fold_db_core::mutation_manager::helpers::apply_storage_prefix_to_schema(
        &mut schema,
        scope,
    );
    let plans = candidate_plans(db, &schema, keys)?;
    let slots: Vec<_> = plans.iter().map(|(_, p)| p.mk_key.clone()).collect();
    let presence = db.atoms().atom_history_presence(&slots).await?;
    let mut result = vec![false; keys.len()];
    for ((index, _), present) in plans.into_iter().zip(presence) {
        result[index] |= present;
    }
    Ok(result)
}

pub(super) async fn release_key_history(
    db: &Arc<DbOperations>,
    loaded: &LoadedTargets,
    spared: &HashSet<usize>,
    scope: Option<&str>,
) -> Result<(TipCommit, HashSet<usize>), SchemaError> {
    let plans = candidate_plans(db, &loaded.schema, &loaded.keys)?;
    let paths: Vec<_> = plans.iter().map(|(_, p)| p.mk_key.clone()).collect();
    let present = db.atoms().atom_history_presence(&paths).await?;
    let erased = plans
        .iter()
        .zip(present)
        .filter_map(|((i, _), found)| (found && !spared.contains(i)).then_some(*i))
        .collect();
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
    let commit = db
        .atoms()
        .commit_tip_changes(TipChangeSet {
            scope,
            releases: release.releases,
            rows: release.rows,
            ..TipChangeSet::default()
        })
        .await?;
    Ok((commit, erased))
}

pub(super) async fn release_for_erase(
    db: &Arc<DbOperations>,
    loaded: &LoadedTargets,
    spared: &HashSet<usize>,
    options: super::erase::EraseOptions<'_>,
) -> Result<(TipCommit, HashSet<usize>), SchemaError> {
    if options.verb.is_delete() {
        return Ok((TipCommit::default(), HashSet::new()));
    }
    release_key_history(db, loaded, spared, options.storage_prefix).await
}
