//! Physical removal of one audited zero-count atom.

use super::*;
use crate::atom::Atom;

impl AtomStore {
    pub(super) async fn delete_atom_gc_candidate_body(
        &self,
        candidate: &AtomGcCandidate,
        storage_prefix: Option<&str>,
    ) -> Result<DeletedAtom, SchemaError> {
        let body_key = build_storage_key(storage_prefix, &atom_key_codec::body_key(&candidate.atom_uuid));
        let raw = self
            .main_store
            .inner()
            .get(body_key.as_bytes())
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("load atom body before GC delete: {error}"))
            })?;
        let atom = match raw.as_ref() {
            Some(raw) => Some(self.decode_atom_bytes(raw).await.map_err(|error| {
                SchemaError::InvalidData(format!("decode atom before audited GC delete: {error}"))
            })?),
            None => None,
        };
        let mut mutations = Vec::new();
        let mut bytes_approx = 0u64;
        if let Some(raw) = raw.as_ref() {
            bytes_approx = (body_key.len() + raw.len()) as u64;
            mutations.push(KvMutation::delete(body_key.into_bytes()));
        }
        mutations.extend(related_delete_mutations(
            candidate,
            storage_prefix,
            atom.as_ref(),
        ));
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
            atom: raw.is_some(),
            storage_keys: u64::from(raw.is_some()),
            bytes_approx,
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

/// Deletes for the blob-ref edges and the live ref count.
fn related_delete_mutations(
    candidate: &AtomGcCandidate,
    storage_prefix: Option<&str>,
    atom: Option<&Atom>,
) -> Vec<KvMutation> {
    let mut mutations = Vec::new();
    if let Some(atom) = atom {
        for blob_ref in
            crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
        {
            mutations.push(KvMutation::delete(
                super::super::BlobRefEdge::atom(&candidate.atom_uuid, &blob_ref)
                    .storage_key(storage_prefix)
                    .into_bytes(),
            ));
        }
    }
    mutations.push(KvMutation::delete(
        super::super::atom_ref_edges::atom_live_ref_count_key(&candidate.atom_uuid, storage_prefix)
            .into_bytes(),
    ));
    mutations
}
