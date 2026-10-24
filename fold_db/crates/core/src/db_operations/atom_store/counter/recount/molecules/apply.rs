//! Preserve header metadata and publish each repaired molecule in a whole batch.

use super::{invalid, Count, RecountReport};
use crate::atom::molecule_key_codec::header_key;
use crate::db_operations::atom_store::molecule_drain::Candidate;
use crate::db_operations::atom_store::{AtomStore, MoleculeBindings};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde_json::json;

impl AtomStore {
    pub(super) async fn apply_molecule_recount(
        &self,
        counts: &[(String, Count)],
        scope: Option<&str>,
        execute: bool,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let candidates: Vec<_> = counts
            .iter()
            .filter_map(|(_, count)| {
                (count.bindings == 0 && count.header.is_some())
                    .then_some(count.molecule.as_deref())
                    .flatten()
                    .map(|molecule| Candidate::new(scope, molecule))
            })
            .collect();
        let present = self
            .raw()
            .inner()
            .exists_many(candidates.iter().map(|c| c.key().into_bytes()).collect())
            .await?;
        let mut mutations = Vec::new();
        for (token, count) in counts {
            plan_header(&mut mutations, scope, count, report)?;
            plan_bindings(&mut mutations, scope, token, count, report)?;
        }
        for (candidate, present) in candidates.into_iter().zip(present) {
            if !present {
                let (key, value) = candidate.row();
                mutations.push(KvMutation::put(key, value));
                report.molecules.drains_queued += 1;
            }
        }
        if !mutations.is_empty() {
            report.operations += mutations.len() as u64;
            report.batches += 1;
            if execute {
                self.raw().inner().batch_mutate(mutations).await?;
            }
        }
        Ok(())
    }
}

fn plan_header(
    out: &mut Vec<KvMutation>,
    scope: Option<&str>,
    count: &Count,
    report: &mut RecountReport,
) -> Result<(), SchemaError> {
    if let (Some(molecule), Some(header)) = (&count.molecule, &count.header) {
        let mut next = header.clone();
        let object = next
            .as_object_mut()
            .ok_or_else(|| invalid("header is not an object"))?;
        object.insert("slots".into(), json!(count.slots));
        object.insert("records".into(), json!(count.records));
        object.remove("suspect");
        if &next != header {
            out.push(KvMutation::put(
                build_storage_key(scope, &header_key(molecule)).into_bytes(),
                serde_json::to_vec(&next).map_err(invalid)?,
            ));
            report.molecules.headers_rewritten += 1;
        }
    }
    Ok(())
}

fn plan_bindings(
    out: &mut Vec<KvMutation>,
    scope: Option<&str>,
    token: &str,
    count: &Count,
    report: &mut RecountReport,
) -> Result<(), SchemaError> {
    let key = build_storage_key(scope, &format!("mref:v1:n:{token}"));
    if count.bindings == 0 {
        if count.binding_counter.is_some() {
            out.push(KvMutation::delete(key.into_bytes()));
            report.molecules.bindings_deleted += 1;
        }
    } else {
        let next = MoleculeBindings {
            version: 1,
            bindings: count.bindings,
            suspect: false,
        };
        let current = count
            .binding_counter
            .as_deref()
            .and_then(|bytes| serde_json::from_slice::<MoleculeBindings>(bytes).ok());
        if current.as_ref() != Some(&next) {
            out.push(KvMutation::put(
                key.into_bytes(),
                serde_json::to_vec(&next).map_err(invalid)?,
            ));
            report.molecules.bindings_rewritten += 1;
        }
    }
    Ok(())
}
