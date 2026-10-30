use super::*;
use crate::db_operations::atom_store::counter::{TipChange, TipChangeSet};

pub(super) async fn shadow(db: &FoldDB, change: &mut Change) -> Result<bool, SchemaError> {
    let mut molecules = BTreeMap::new();
    for target in &change.targets {
        db.db_ops
            .molecule_keys()
            .load_schema(&target.target)
            .await?;
        for id in target
            .target
            .field_molecule_uuids
            .iter()
            .flat_map(|ids| ids.values())
        {
            molecules.insert(id.clone(), false);
        }
        for name in target.target.alternate_keys.keys() {
            molecules.insert(crate::alternate_keys::molecule(&target.target, name), true);
        }
    }
    let items: Vec<_> = molecules.into_iter().collect();
    let mut budget = 128;
    while change.cleaned < items.len() && budget > 0 {
        let (molecule, alternate) = &items[change.cleaned];
        let atoms = db.db_ops.atoms();
        let counters = atoms
            .molecule_counter_views(std::slice::from_ref(molecule), None)
            .await?;
        if counters.len() != 1 || counters[0].suspect || counters[0].bindings != 1 {
            return Err(invalid(
                "another owner binds an unpublished conversion molecule",
            ));
        }
        let prefix = format!("{}:{molecule}:", if *alternate { "ak" } else { "mk" });
        let rows = atoms
            .raw()
            .inner()
            .scan_prefix_paged(prefix.as_bytes(), budget)
            .await?;
        budget -= rows.len();
        if rows.is_empty() {
            change.cleaned += 1;
            continue;
        }
        if *alternate {
            atoms
                .commit_tip_changes(TipChangeSet {
                    deletes: rows.into_iter().map(|(key, _)| key).collect(),
                    ..TipChangeSet::default()
                })
                .await?;
        } else {
            let tips = rows
                .into_iter()
                .map(|(key, _)| {
                    Ok(TipChange {
                        key: String::from_utf8(key)
                            .map_err(|_| invalid("conversion tip key is unreadable"))?,
                        new: None,
                    })
                })
                .collect::<Result<Vec<_>, SchemaError>>()?;
            atoms
                .commit_tip_changes(TipChangeSet {
                    tips,
                    ..TipChangeSet::default()
                })
                .await?;
        }
    }
    Ok(change.cleaned == items.len())
}
