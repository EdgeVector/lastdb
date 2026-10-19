//! Fixed native proof over one encrypted, exact-key preserved outcome.

use super::*;
use crate::crypto::CryptoProvider;
use crate::storage::TypedKvStore;
use crate::sync::engine::{AuthenticatedReplayObject, RetiredReplaySource};
use crate::sync::log::LogOp;
use std::collections::BTreeSet;

/// No source identifier, schema name, key, receipt value, or payload is public.
#[derive(Default, Serialize)]
pub struct PreservedRetiredIntentProof {
    pub journal_present: bool,
    pub journal_strictly_decoded: bool,
    pub original_source_authenticated: bool,
    pub original_record_matches: bool,
    pub original_mutations: usize,
    pub source_objects: usize,
    pub transaction_group: bool,
    pub retirement_proofs: usize,
    pub source_owner_exact: bool,
    pub atom_roots: u64,
    pub blob_roots: u64,
    pub atom_body_or_inline_source_complete: bool,
    pub local_blob_presence_complete: bool,
    pub native_author_clock_validation_passed: bool,
    pub authority_write_attempted: bool,
    pub schema_heal_attempted: bool,
}

pub(super) fn decode_outcome(raw: &[u8], source_id: &str) -> Result<PreservedIntent, SchemaError> {
    if raw.len() > MAX_OUTCOME_BYTES || !pointer::exact_id(source_id) {
        return Err(refused("retired outcome input is invalid"));
    }
    let outcome: PreservedIntent =
        serde_json::from_slice(raw).map_err(|_| refused("retired outcome is unreadable"))?;
    let canonical = serde_json::to_value(&outcome)
        .and_then(|value| serde_json::to_vec(&value))
        .map_err(|_| refused("retired outcome serialization failed"))?;
    let record = &outcome.original_record;
    if canonical != raw
        || outcome.version != 1
        || outcome.outcome != "retired-authority-preserved"
        || outcome.source_id != source_id
        || retired_intent_source_id(record) != source_id
        || record.target_id != "personal"
        || !record.target_prefix.is_empty()
        || record.entry.seq != record.frontier_after
        || record.entry.device_id != record.writer_id
        || outcome.source_objects.is_empty()
        || outcome.source_objects.len() > MAX_SOURCE_OBJECTS
        || outcome
            .source_objects
            .iter()
            .try_fold(0usize, |size, object| {
                size.checked_add(object.ciphertext_bytes)
            })
            .is_none_or(|size| size > MAX_SOURCE_BYTES)
    {
        return Err(refused(
            "retired outcome identity or supported shape differs",
        ));
    }
    Ok(outcome)
}

pub async fn probe_preserved_retired_intent(
    store: &Arc<dyn NamespacedStore>,
    source_id: &str,
    content_key: &[u8; 32],
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<PreservedRetiredIntentProof, SchemaError> {
    let Some(raw) = read_preserved_retired_intent(store, source_id).await? else {
        return Ok(PreservedRetiredIntentProof::default());
    };
    let outcome = decode_outcome(&raw, source_id)?;
    let LogOp::MutationIntent { mutations } = &outcome.original_record.entry.op else {
        return Err(refused("retired outcome has no original intent"));
    };
    if mutations.is_empty()
        || mutations.iter().any(|member| {
            member.aggregate_set.is_some()
                || member
                    .storage_prefix
                    .as_ref()
                    .is_some_and(|prefix| !prefix.is_empty())
        })
    {
        return Err(refused("unsupported retired outcome envelope"));
    }
    let source = authenticated_source(store, &outcome).await?;
    crate::sync::engine::verify_retired_replay_source(&source, crypto)
        .await
        .map_err(|_| refused("retired original source authentication or binding failed"))?;
    let schemas = proof_schema_store(store, content_key).await?;
    let names: BTreeSet<_> = mutations.iter().map(|member| &member.schema_name).collect();
    let proof_names: BTreeSet<_> = outcome
        .retirement_proofs
        .iter()
        .map(|proof| &proof.schema_name)
        .collect();
    if names != proof_names || proof_names.len() != outcome.retirement_proofs.len() {
        return Err(refused("retired outcome proof set differs"));
    }
    for proof in &outcome.retirement_proofs {
        schemas.validate_retired_replay_proof(proof)?;
    }
    let atoms = proof_atom_store(store, content_key).await?;
    let retention: super::super::atom_store::RetiredIntentRetentionProof = atoms
        .verify_retired_intent_retention(source_id, mutations)
        .await?;
    let materialized =
        crate::sync::mutation_intent::materialize_field_values(mutations.clone(), &atoms)
            .await
            .map_err(|_| refused("retired proof materialization failed"))?;
    let (decoded, prefix) = crate::sync::mutation_intent::decode_mutations(&materialized);
    if decoded.len() != mutations.len()
        || prefix.as_ref().is_some_and(|prefix| !prefix.is_empty())
        || decoded
            .iter()
            .any(|member| !crate::schema::types::author_clock::verify_mutation_author_clock(member))
    {
        return Err(refused("retired proof native author signature differs"));
    }
    Ok(PreservedRetiredIntentProof {
        journal_present: true,
        journal_strictly_decoded: true,
        original_source_authenticated: true,
        original_record_matches: true,
        original_mutations: mutations.len(),
        source_objects: source.objects.len(),
        transaction_group: outcome.transaction_group,
        retirement_proofs: proof_names.len(),
        source_owner_exact: retention.source_owner_exact,
        atom_roots: retention.atom_roots,
        blob_roots: retention.blob_roots,
        atom_body_or_inline_source_complete: true,
        local_blob_presence_complete: true,
        native_author_clock_validation_passed: true,
        authority_write_attempted: false,
        schema_heal_attempted: false,
    })
}

async fn authenticated_source(
    store: &Arc<dyn NamespacedStore>,
    outcome: &PreservedIntent,
) -> Result<RetiredReplaySource, SchemaError> {
    let keys: Vec<_> = outcome
        .source_objects
        .iter()
        .map(|object| {
            if !pointer::exact_id(&object.ciphertext_sha256) {
                return Err(refused("retired source object identifier is invalid"));
            }
            Ok(object.ciphertext_sha256.as_bytes().to_vec())
        })
        .collect::<Result<_, _>>()?;
    let kv = store
        .open_namespace(RETIRED_SOURCE_NAMESPACE)
        .await
        .map_err(storage_error)?;
    let values = strict_values(&kv, keys).await?;
    let objects = values
        .into_iter()
        .zip(&outcome.source_objects)
        .map(|(value, expected)| {
            let value = value.ok_or_else(|| refused("retired source object is absent"))?;
            if value.len() != expected.ciphertext_bytes
                || format!("{:x}", Sha256::digest(&value)) != expected.ciphertext_sha256
            {
                return Err(refused("retired source object identity differs"));
            }
            Ok(AuthenticatedReplayObject {
                segment: expected.segment.clone(),
                ciphertext: Arc::from(value),
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(RetiredReplaySource {
        record: outcome.original_record.clone(),
        record_index: outcome.record_index,
        transaction_group: outcome.transaction_group,
        objects,
    })
}

async fn proof_schema_store(
    store: &Arc<dyn NamespacedStore>,
    key: &[u8; 32],
) -> Result<super::super::schema_store::SchemaStore, SchemaError> {
    let (bodies, states, successors) = tokio::try_join!(
        store.open_namespace("schemas"),
        store.open_namespace("schema_states"),
        store.open_namespace("schema_superseded_by")
    )
    .map_err(storage_error)?;
    Ok(super::super::schema_store::SchemaStore::new(
        Arc::new(TypedKvStore::new(bodies)),
        Arc::new(TypedKvStore::new(states)),
        Arc::new(TypedKvStore::new(successors)),
        store.disk_pack_runtime(),
    )
    .with_catalog_unwrap_key(*key))
}

async fn proof_atom_store(
    store: &Arc<dyn NamespacedStore>,
    key: &[u8; 32],
) -> Result<super::super::AtomStore, SchemaError> {
    let (main, index) = tokio::try_join!(
        store.open_namespace("main"),
        store.open_namespace("schema_index")
    )
    .map_err(storage_error)?;
    // Exact body reads need no boot resolver work.
    Ok(super::super::AtomStore::new_with_content_key(
        Arc::new(TypedKvStore::new(main)),
        Arc::new(TypedKvStore::new(index)),
        Some(*key),
    )
    .with_namespaced_store(Arc::clone(store)))
}
