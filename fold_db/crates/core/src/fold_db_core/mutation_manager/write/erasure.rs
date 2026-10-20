//! Attribution roots and protein reverse-membership erasure.

use std::collections::{HashMap, HashSet};

use crate::db_operations::AttributionPendingScope;
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, MutationType};
use crate::schema::SchemaError;

use super::attribution::{attribution_events, live_write_attribution_path};
use super::MutationManager;

impl MutationManager {
    /// Persist the durable attribution root for a batch, then append its
    /// source events and clear the pending-scope markers.
    ///
    /// This runs once per mutation batch, from exactly one of two mutually
    /// exclusive call sites: the live request write
    /// (`write_mutations_batch_with_receipt_cloud`) or a captured-mutation
    /// replay (`apply_replayed_mutations`). A given batch takes one path or
    /// the other, never both, so the two call sites are not two attribution
    /// writes for the same operation — they are the shared attribution tail
    /// for two different upstream pipelines. No-op when source events are
    /// disabled or the batch carries no attribution scopes.
    pub(super) async fn record_attribution_scopes(
        &self,
        attribution_scopes: &[AttributionPendingScope],
    ) -> Result<(), SchemaError> {
        if attribution_scopes.is_empty() {
            return Ok(());
        }
        // One durable path row per written object, before the event append's
        // trailing flush — so a caller that observes the response also has a
        // durable attribution root for what it just wrote, not only a source
        // event that a later batch walk has yet to classify.
        for scope in attribution_scopes {
            self.db_ops
                .attribution()
                .put_attribution_path(&live_write_attribution_path(scope))
                .await?;
        }
        let mutation_ids = attribution_scopes
            .iter()
            .map(|scope| scope.mutation_id.clone())
            .collect::<Vec<_>>();
        self.db_ops
            .attribution()
            .append_events_and_clear_pending_scopes(
                attribution_events(attribution_scopes),
                &mutation_ids,
            )
            .await?;
        Ok(())
    }

    /// Add the reverse-key deletes for a protein-backed HashRange membership.
    ///
    /// A HashRange membership has two keyed views of one edge: `holder → item`
    /// and `item → holder`. A protein keeps writes in both views coherent, but
    /// a hard delete has no payload from which the normal fold can derive the
    /// sibling key. Point-read the fixed schema field set before we tombstone
    /// the entry key, then enqueue the sibling key under its own schema. This
    /// reads one fixed-field row and checks the resident schema and protein
    /// metadata. It never traverses a data partition or scans stored records.
    pub(in crate::fold_db_core::mutation_manager) async fn expand_protein_hashrange_erasures(
        &self,
        erasures: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Mutation>, SchemaError> {
        let schemas = self.schema_manager.get_schemas()?;
        let mut expanded = Vec::with_capacity(erasures.len());
        let mut seen: HashSet<(String, String)> = HashSet::new();

        for erasure in erasures {
            let source_schema = schemas.get(&erasure.schema_name).ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Schema '{}' not found for reverse membership delete",
                    erasure.schema_name
                ))
            })?;
            let source_key = source_schema.key.as_ref();
            let source_is_hashrange =
                matches!(source_schema.schema_type, DeclarativeSchemaType::HashRange);
            seen.insert((
                erasure.schema_name.clone(),
                erasure.key_value.to_storage_key(),
            ));
            expanded.push(erasure.clone());

            if !source_is_hashrange {
                continue;
            }
            let Some(source_key) = source_key else {
                continue;
            };
            if source_key.hash_field.is_none() {
                continue;
            }

            let proteins = self.proteins_of_schema_molecules(source_schema).await?;
            if proteins.is_empty() {
                continue;
            }

            let source_fields: Vec<String> = source_schema.runtime_fields.keys().cloned().collect();
            let row = match self
                .read_current_row_fields(&erasure, &source_fields, storage_prefix)
                .await?
            {
                super::super::cas::CurrentRowFields::Present(row) => row,
                super::super::cas::CurrentRowFields::Absent => continue,
                super::super::cas::CurrentRowFields::Corrupt { .. } => {
                    return Err(SchemaError::MissingSelectedAtom);
                }
            };

            let mut candidates = Vec::new();
            for (peer_name, peer_schema) in &schemas {
                if peer_name == &erasure.schema_name
                    || !matches!(peer_schema.schema_type, DeclarativeSchemaType::HashRange)
                {
                    continue;
                }
                let Some(reverse) = Self::reverse_erasure_for_peer(
                    &erasure,
                    peer_name,
                    peer_schema,
                    &proteins,
                    &row,
                ) else {
                    continue;
                };
                if seen.contains(&(peer_name.clone(), reverse.key_value.to_storage_key())) {
                    continue;
                }
                if expanded.len().saturating_add(candidates.len()) >= 10_000 {
                    return Err(SchemaError::WriteResourceLimit {
                        detail: "Delete peer expansion exceeds 10000 keys".into(),
                    });
                }
                candidates.push(reverse);
            }
            for confirmed in self
                .confirmed_peer_erasures(&erasure, candidates, storage_prefix)
                .await?
            {
                seen.insert((
                    confirmed.schema_name.clone(),
                    confirmed.key_value.to_storage_key(),
                ));
                expanded.push(confirmed);
            }
        }

        Ok(expanded)
    }

    /// Proteins that any molecule of `source_schema` belongs to.
    pub(in crate::fold_db_core::mutation_manager) async fn proteins_of_schema_molecules(
        &self,
        source_schema: &crate::schema::Schema,
    ) -> Result<Vec<crate::protein::Protein>, SchemaError> {
        let source_molecules: Vec<String> = source_schema
            .runtime_fields
            .values()
            .filter_map(|field| field.common().molecule_uuid().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if source_molecules.is_empty() {
            return Ok(Vec::new());
        }
        let mut protein_uuids: Vec<String> = self
            .db_ops
            .atoms()
            .protein_of_molecules(&source_molecules)
            .await?
            .into_iter()
            .flatten()
            .collect();
        protein_uuids.sort_unstable();
        protein_uuids.dedup();
        if protein_uuids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self
            .db_ops
            .atoms()
            .protein_get_many(&protein_uuids)
            .await?
            .into_iter()
            .flatten()
            .collect())
    }

    /// The sibling-view delete for `erasure` on `peer_schema`, when the peer is
    /// a protein member and the forward row carries its hash and range values.
    fn reverse_erasure_for_peer(
        erasure: &Mutation,
        peer_name: &str,
        peer_schema: &crate::schema::Schema,
        proteins: &[crate::protein::Protein],
        row: &std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Option<Mutation> {
        let peer_key = peer_schema.key.as_ref()?;
        let (peer_hash_field, peer_range_field) = (
            peer_key.hash_field.as_deref()?,
            peer_key.range_field.as_deref()?,
        );
        let peer_molecules: HashSet<&str> = peer_schema
            .runtime_fields
            .values()
            .filter_map(|field| field.common().molecule_uuid().map(String::as_str))
            .collect();
        let peer_is_member = proteins.iter().any(|protein| {
            protein.members.iter().any(|member| {
                peer_molecules.contains(member.molecule_uuid.as_str())
                    && member.hash_field == peer_hash_field
                    && member.range_field.as_deref() == Some(peer_range_field)
            })
        });
        if !peer_is_member {
            return None;
        }

        let hash = row.get(peer_hash_field).and_then(scalar_key_coordinate)?;
        let range = row.get(peer_range_field).and_then(scalar_key_coordinate)?;
        let key_value = KeyValue::new(Some(hash.to_string()), Some(range.to_string()));

        let mut reverse = Mutation::new(
            peer_name.to_string(),
            HashMap::new(),
            key_value,
            erasure.pub_key.clone(),
            erasure.mutation_type,
        );
        // Current selected tips must certify this candidate's association
        // before removal. The caller's presence condition stays on its key.
        reverse.synchronous = erasure.synchronous;
        reverse.erase_tips_before_nanos = erasure.erase_tips_before_nanos;
        if erasure.mutation_type == MutationType::Delete {
            // Both molecule keys describe one signed source Delete.
            // The derived key has no separate signature, but its
            // durable winner must retain the source device's order.
            reverse.uuid = erasure.uuid.clone();
            reverse.imported_written_at = erasure.imported_written_at;
            reverse.logical_counter = erasure.logical_counter;
            reverse.author_clock_writer_id = erasure.author_clock_writer_id.clone();
        }
        Some(reverse)
    }
}
