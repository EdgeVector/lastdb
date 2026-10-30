use super::super::molecules::PreparedFieldAtom;
use super::*;
use crate::atom::AtomEntry;
use crate::storage::disk_pack::logical_capture::outcome::AcceptedFieldOutcomeRef;
use crate::storage::disk_pack::logical_capture::{preflight_outcome_size, AcceptedFieldOutcome};
use std::collections::BTreeMap;
use std::sync::Mutex;

pub(in crate::fold_db_core::mutation_manager) type Candidates = BTreeMap<String, Candidate>;

pub(in crate::fold_db_core::mutation_manager) struct Candidate {
    pub outcome: AcceptedFieldOutcome,
    bytes: usize,
}

#[derive(Default)]
struct Collector {
    candidates: Candidates,
    bytes: usize,
}

tokio::task_local! {
    static CANDIDATES: Mutex<Collector>;
}

pub(in crate::fold_db_core::mutation_manager) async fn collect<T>(
    runtime: &PackRuntime,
    future: impl std::future::Future<Output = Result<T, SchemaError>>,
) -> Result<(T, Candidates), SchemaError> {
    if !runtime.logical_capture_enabled() {
        return Ok((future.await?, Candidates::new()));
    }
    CANDIDATES
        .scope(Mutex::new(Collector::default()), async {
            let result = future.await?;
            let candidates = CANDIDATES.with(|state| {
                std::mem::take(&mut state.lock().expect("logical capture candidates").candidates)
            });
            Ok((result, candidates))
        })
        .await
}

impl MutationManager {
    /// Keep candidate facts, not caller predicates. Certification follows all groups.
    pub(in crate::fold_db_core::mutation_manager) fn collect_logical_delta(
        &self,
        delta: &super::super::molecules::PreparedSchemaDelta,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        if !runtime.logical_capture_enabled() {
            return Ok(());
        }
        if !delta.share_prefixes.is_empty() {
            return Err(unsupported("share effects"));
        }
        let (identity, alternate_revision) = capture_schema_contract(&delta.schema)?;
        CANDIDATES
            .try_with(|state| {
                let mut state = state.lock().expect("logical capture candidates");
                for PreparedFieldAtom {
                    mutation_index: index,
                    field_name: name,
                    atom,
                } in &delta.atom_results
                {
                    let mutation = &delta.schema_mutations[*index];
                    let key = &delta.mutation_key_values[*index];
                    let field = delta
                        .schema
                        .runtime_fields
                        .get(name)
                        .ok_or_else(|| unsupported("a missing prepared field"))?;
                    let Some(tip) = field.atom_entry_at_key(key) else {
                        continue;
                    };
                    if !is_source_tip(tip, mutation, atom.uuid()) {
                        continue;
                    }
                    let molecule = field
                        .common()
                        .molecule_uuid()
                        .ok_or_else(|| unsupported("a field without a molecule identity"))?;
                    let (hash, range) = field
                        .slot_coordinates(key)
                        .ok_or_else(|| unsupported("a field without exact coordinates"))?;
                    let storage_key = self
                        .db_ops
                        .atoms()
                        .key_codec_for_molecule(molecule)?
                        .api_hash_range_record_key(molecule, &hash, &range)
                        .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
                    let metadata = field
                        .molecule_data()
                        .and_then(|data| data.get_key_metadata(&hash, &range));
                    let view =
                        AcceptedFieldOutcomeRef {
                            schema_name: &delta.schema_name,
                            schema_identity: identity,
                            alternate_key_revision: alternate_revision.as_deref(),
                            field_name: name,
                            field_identity: delta.schema.field_hashes.get(name).ok_or_else(
                                || unsupported("a field without a canonical identity"),
                            )?,
                            molecule_uuid: molecule,
                            key,
                            atom,
                            tip,
                            metadata,
                        };
                    let bytes = preflight_outcome_size(&view)?;
                    let previous = state
                        .candidates
                        .get(&storage_key)
                        .map_or(0, |candidate| candidate.bytes);
                    let total = state.bytes - previous + bytes;
                    if total > 1024 * 1024 {
                        return Err(unsupported("final field outcomes above 1 MiB"));
                    }
                    if !state.candidates.contains_key(&storage_key)
                        && state.candidates.len() >= MAX_OUTCOMES
                    {
                        return Err(unsupported("more than 1024 final fields"));
                    }
                    state.candidates.insert(
                        storage_key,
                        Candidate {
                            outcome: view.into_owned(),
                            bytes,
                        },
                    );
                    state.bytes = total;
                }
                Ok(())
            })
            .map_err(|_| unsupported("an uncaptured native preparation"))?
    }
}

fn capture_schema_contract(
    schema: &crate::schema::types::Schema,
) -> Result<(&str, Option<String>), SchemaError> {
    let identity = schema
        .identity_hash
        .as_deref()
        .filter(|identity| !identity.is_empty())
        .ok_or_else(|| unsupported("a missing schema identity"))?;
    let revision = schema
        .uses_independent_fields()
        .then(|| schema.alternate_key_revision());
    Ok((identity, revision))
}

fn is_source_tip(tip: &AtomEntry, mutation: &Mutation, atom: &str) -> bool {
    tip.atom_uuid == atom
        && tip.mutation_uuid == mutation.uuid
        && Some(tip.written_at) == mutation.imported_written_at
        && tip.logical_counter == mutation.logical_counter
        && tip.lww_device() == mutation.author_clock_writer_id
}
