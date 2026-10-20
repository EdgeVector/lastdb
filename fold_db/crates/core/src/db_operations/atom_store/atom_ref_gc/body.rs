//! Physical removal of one audited zero-count atom.

use super::*;
use crate::atom::Atom;

/// Storage keys and bytes of the atom body forms that exist on disk.
struct PresentBodies {
    locator_key: String,
    present: Vec<(String, Vec<u8>)>,
}

impl AtomStore {
    pub(super) async fn delete_atom_gc_candidate_body(
        &self,
        candidate: &AtomGcCandidate,
        storage_prefix: Option<&str>,
    ) -> Result<DeletedAtom, SchemaError> {
        let PresentBodies {
            locator_key,
            present,
        } = self.load_present_bodies(candidate, storage_prefix).await?;
        let atom = match present.first() {
            Some((_, raw)) => Some(self.decode_atom_bytes(raw).await.map_err(|error| {
                SchemaError::InvalidData(format!("decode atom before audited GC delete: {error}"))
            })?),
            None => None,
        };
        let mut mutations = Vec::new();
        let mut bytes_approx = 0u64;
        for (key, value) in &present {
            bytes_approx = bytes_approx.saturating_add((key.len() + value.len()) as u64);
            mutations.push(KvMutation::delete(key.as_bytes().to_vec()));
        }
        mutations.extend(related_delete_mutations(
            candidate,
            storage_prefix,
            atom.as_ref(),
            locator_key,
        )?);
        // The schema marker lives in a separate namespace, so LastStore cannot
        // include it in the main-store transaction. Delete it first and keep
        // both candidate rows until that delete succeeds. If the following
        // main transaction fails, the intact candidate retries the idempotent
        // schema-marker delete and the body transaction on the next pass.
        if let Some(atom) = atom.as_ref() {
            self.delete_schema_marker(atom, candidate, storage_prefix)
                .await?;
        }
        mutations.extend(candidate_delete_mutations(candidate, storage_prefix));
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("delete audited zero-count atom: {error}"))
            })?;
        Ok(DeletedAtom {
            atom: !present.is_empty(),
            storage_keys: u64::try_from(present.len()).unwrap_or(u64::MAX),
            bytes_approx,
        })
    }

    /// Read the locator and every body form that exists for the candidate.
    async fn load_present_bodies(
        &self,
        candidate: &AtomGcCandidate,
        storage_prefix: Option<&str>,
    ) -> Result<PresentBodies, SchemaError> {
        let locator_key = build_storage_key(
            storage_prefix,
            &atom_locator_codec::locator_key(&candidate.atom_uuid),
        );
        let locator: Option<Value> =
            self.main_store
                .get_item(&locator_key)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("load atom locator before GC delete: {error}"))
                })?;
        let mut body_keys = vec![build_storage_key(
            storage_prefix,
            &atom_key_codec::storage_key(AtomKeyEncoding::Flat, None, &candidate.atom_uuid),
        )];
        if let Some(partition) = locator.as_ref().and_then(atom_locator_codec::decode_value) {
            body_keys.push(build_storage_key(
                storage_prefix,
                &atom_key_codec::storage_key(
                    AtomKeyEncoding::PartitionPrefix,
                    Some(&partition),
                    &candidate.atom_uuid,
                ),
            ));
        }
        body_keys.sort();
        body_keys.dedup();
        let raw_bodies = self
            .main_store
            .inner()
            .get_many(
                body_keys
                    .iter()
                    .map(|key| key.as_bytes().to_vec())
                    .collect(),
            )
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("load atom body before GC delete: {error}"))
            })?;
        let present = body_keys
            .into_iter()
            .zip(raw_bodies)
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect();
        Ok(PresentBodies {
            locator_key,
            present,
        })
    }

    async fn delete_schema_marker(
        &self,
        atom: &Atom,
        candidate: &AtomGcCandidate,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let index_key = build_storage_key(
            storage_prefix,
            &super::super::helpers::schema_index_codec::record_key(
                atom.source_schema_name(),
                &candidate.atom_uuid,
            ),
        );
        self.schema_index_store
            .delete_item(&index_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("delete audited atom schema marker: {error}"))
            })?;
        Ok(())
    }
}

/// Deletes for the blob-ref edges, the locator, and the live ref count.
fn related_delete_mutations(
    candidate: &AtomGcCandidate,
    storage_prefix: Option<&str>,
    atom: Option<&Atom>,
    locator_key: String,
) -> Result<Vec<KvMutation>, SchemaError> {
    let mut mutations = Vec::new();
    if let Some(atom) = atom {
        for blob_ref in
            crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
        {
            mutations.extend(
                super::super::BlobRefEdge::atom(&candidate.atom_uuid, &blob_ref, storage_prefix)
                    .delete_mutations(),
            );
        }
    }
    mutations.push(KvMutation::delete(locator_key.into_bytes()));
    mutations.push(KvMutation::delete(
        super::super::atom_ref_edges::atom_live_ref_count_key(
            &candidate.atom_uuid,
            storage_prefix,
        )?
        .into_bytes(),
    ));
    Ok(mutations)
}
