//! Protein member-delete expansion and the shared exact-row read result type. CAS locks, preconditions, current-state reads and idempotency live in the sibling `cas_*` modules.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::schema::types::{KeyValue, Mutation, MutationType, Schema};
use crate::schema::SchemaError;

use super::MutationManager;

/// Integrity-aware result for an exact fixed-field row read.
///
/// Aggregate rows must distinguish a row with no tips from a row whose tips
/// exist but whose bodies cannot resolve. Treating both as an empty map can
/// apply the same contribution twice and certify a corrupt summary.
#[derive(Debug)]
pub(super) enum CurrentRowFields {
    Absent,
    Present(BTreeMap<String, serde_json::Value>),
    Corrupt { field: String, atom_uuid: String },
}

/// Resolve one sibling key value from the source row.
///
/// `Ok(None)` means the source row has no atom for that key field. A row that
/// was written without its sibling key (BoardCards residue: a `slug` atom with
/// no `sk`) cannot address a sibling row, so there is no sibling tip to
/// remove. The caller skips that sibling and still deletes the addressed row.
/// A present but non-scalar value is still an error: it names a real key the
/// engine cannot encode.
fn protein_delete_key_value(
    values: &BTreeMap<String, serde_json::Value>,
    field: &str,
    schema_name: &str,
) -> Result<Option<String>, SchemaError> {
    let Some(value) = values.get(field) else {
        return Ok(None);
    };
    if let Some(value) = value.as_str() {
        return Ok(Some(value.to_string()));
    }
    if value.is_null() || value.is_array() || value.is_object() {
        return Err(SchemaError::InvalidData(format!(
            "protein delete key field '{field}' for sibling schema '{schema_name}' must be a scalar"
        )));
    }
    Ok(Some(value.to_string()))
}

/// One sibling schema that a protein delete must reach:
/// `(schema name, hash key field, optional range key field)`.
type ProteinDeletePeer = (String, String, Option<String>);

/// Sibling schemas whose key fields are declared on the source schema and
/// whose fields share a sibling molecule. Sorted by schema name.
fn protein_delete_peers(
    schemas: &HashMap<String, Schema>,
    source_name: &str,
    source: &Schema,
    sibling_molecules: &HashSet<String>,
) -> Vec<ProteinDeletePeer> {
    let source_is_range = source
        .key
        .as_ref()
        .is_some_and(|key| key.range_field.is_some());
    let mut peers: Vec<ProteinDeletePeer> = schemas
        .iter()
        .filter(|(name, _)| name.as_str() != source_name)
        .filter_map(|(name, schema)| {
            let key = schema.key.as_ref()?;
            let hash_field = key.hash_field.as_ref()?.clone();
            // A sibling must address rows of the same shape. A
            // HashRange membership row and a Hash-keyed record can
            // share a record protein (BoardCards copies Card and
            // Milestone payload fields), but deleting one membership
            // row must not delete the record it names. Measured on a
            // CoW copy of the primary 2026-09-24: a BoardCards delete
            // expanded to Milestone(slug) and Card(slug) deletes.
            if key.range_field.is_some() != source_is_range {
                return None;
            }
            // A sibling key must be a declared source field. The
            // engine does not infer an access value from another
            // schema, so an ordinary shared payload protein cannot
            // turn into a coindex delete by accident.
            if !source.runtime_fields.contains_key(&hash_field)
                || key
                    .range_field
                    .as_ref()
                    .is_some_and(|field| !source.runtime_fields.contains_key(field))
            {
                return None;
            }
            let shares_member = schema.runtime_fields.values().any(|field| {
                field
                    .common()
                    .molecule_uuid()
                    .is_some_and(|uuid| sibling_molecules.contains(uuid))
            });
            shares_member.then(|| (name.clone(), hash_field, key.range_field.clone()))
        })
        .collect();
    peers.sort_by(|left, right| left.0.cmp(&right.0));
    peers
}

/// Sorted, de-duplicated key field names needed to address every peer.
fn protein_delete_peer_fields(peers: &[ProteinDeletePeer]) -> Vec<String> {
    let mut fields: Vec<String> = peers
        .iter()
        .flat_map(|(_, hash, range)| std::iter::once(hash).chain(range.iter()))
        .cloned()
        .collect();
    fields.sort();
    fields.dedup();
    fields
}

/// The caller's own key names the source key fields exactly. A row
/// can lack the atom of its own key field (BoardCards residue has
/// no `sk` atom at `(board, sk)`), and a sibling that shares that
/// key field is still addressable through the caller's key.
fn overlay_caller_key(
    values: &mut BTreeMap<String, serde_json::Value>,
    source: &Schema,
    mutation: &Mutation,
) {
    if let Some(key) = source.key.as_ref() {
        let own = [
            (key.hash_field.as_ref(), mutation.key_value.hash.as_ref()),
            (key.range_field.as_ref(), mutation.key_value.range.as_ref()),
        ];
        for (field, value) in own {
            if let (Some(field), Some(value)) = (field, value) {
                values
                    .entry(field.clone())
                    .or_insert_with(|| serde_json::Value::String(value.clone()));
            }
        }
    }
}

/// Push one delete mutation per peer whose key values the source row supplies.
fn push_peer_deletes(
    expanded: &mut Vec<Mutation>,
    mutation: &Mutation,
    peers: Vec<ProteinDeletePeer>,
    values: &BTreeMap<String, serde_json::Value>,
) -> Result<(), SchemaError> {
    for (schema_name, hash_field, range_field) in peers {
        let Some(hash) = protein_delete_key_value(values, &hash_field, &schema_name)? else {
            tracing::debug!(
                "protein delete: source row in '{}' has no '{hash_field}' atom; \
                 skip sibling '{schema_name}'",
                mutation.schema_name
            );
            continue;
        };
        let range = match range_field.as_deref() {
            None => None,
            Some(field) => {
                if let Some(range) = protein_delete_key_value(values, field, &schema_name)? {
                    Some(range)
                } else {
                    tracing::debug!(
                        "protein delete: source row in '{}' has no '{field}' atom; \
                             skip sibling '{schema_name}'",
                        mutation.schema_name
                    );
                    continue;
                }
            }
        };
        let mut peer = mutation.clone();
        peer.schema_name = schema_name;
        peer.key_value = KeyValue::new(Some(hash), range);
        // A source must-exist check applies only to the caller's key.
        // Protein fold is eventual, so a peer can already be absent.
        peer.expected = None;
        peer.must_exist = None;
        if expanded.len() >= 10_000 {
            return Err(SchemaError::WriteResourceLimit {
                detail: "Delete peer expansion exceeds 10000 keys".into(),
            });
        }
        expanded.push(peer);
    }
    Ok(())
}

impl MutationManager {
    /// Molecules of sibling schemas that share a protein with the source
    /// schema's own molecules.
    async fn protein_sibling_molecules(
        &self,
        source: &Schema,
    ) -> Result<HashSet<String>, SchemaError> {
        let store = self.db_ops.atoms();
        let source_molecules: HashSet<String> = source
            .runtime_fields
            .values()
            .filter_map(|field| field.common().molecule_uuid().cloned())
            .collect();
        let mut sibling_molecules = HashSet::new();
        for molecule_uuid in &source_molecules {
            let Some(protein_uuid) = store.protein_of_molecule(molecule_uuid).await? else {
                continue;
            };
            let Some(protein) = store.protein_get(&protein_uuid).await? else {
                continue;
            };
            for member in protein.members {
                if !source_molecules.contains(&member.molecule_uuid) {
                    sibling_molecules.insert(member.molecule_uuid);
                }
            }
        }
        Ok(sibling_molecules)
    }

    /// Expand one delete mutation into the delete plus its sibling deletes.
    async fn expand_one_protein_delete(
        &self,
        mutation: Mutation,
        schemas: &HashMap<String, Schema>,
        storage_prefix: Option<&str>,
        expanded: &mut Vec<Mutation>,
    ) -> Result<(), SchemaError> {
        let source = self
            .schema_manager
            .get_schema_metadata(&mutation.schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!("Schema '{}' not found", mutation.schema_name))
            })?;
        let sibling_molecules = self.protein_sibling_molecules(&source).await?;
        if sibling_molecules.is_empty() {
            expanded.push(mutation);
            return Ok(());
        }

        let peers =
            protein_delete_peers(schemas, &mutation.schema_name, &source, &sibling_molecules);
        if peers.is_empty() {
            expanded.push(mutation);
            return Ok(());
        }

        let fields = protein_delete_peer_fields(&peers);
        let mut values = match self
            .read_current_delete_fields(&mutation, &fields, storage_prefix)
            .await?
        {
            CurrentRowFields::Present(values) => values,
            CurrentRowFields::Absent => {
                expanded.push(mutation);
                return Ok(());
            }
            CurrentRowFields::Corrupt { .. } => return Err(SchemaError::MissingLocalAtom),
        };
        overlay_caller_key(&mut values, &source, &mutation);

        expanded.push(mutation.clone());
        push_peer_deletes(expanded, &mutation, peers, &values)
    }

    /// Add one idempotent delete target for each sibling schema that shares a
    /// protein member with the source schema.
    ///
    /// The app submits one delete through the key it addressed. The core reads
    /// the source row by that exact key, obtains the app-declared peer key
    /// values, and removes the peer tips through the same hard-delete lane.
    /// It does not derive an access pattern or scan a schema. Protein membership
    /// is the authority for the sibling relationship.
    pub(super) async fn expand_protein_member_deletes(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Mutation>, SchemaError> {
        // An org replica owns its catalog membership separately. This local
        // open-set path must not manufacture a peer delete inside that prefix.
        if storage_prefix.is_some()
            || !mutations
                .iter()
                .any(|mutation| matches!(mutation.mutation_type, MutationType::Delete))
        {
            return Ok(mutations);
        }

        let schemas = self.schema_manager.get_schemas()?;
        let mut expanded = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            if !matches!(mutation.mutation_type, MutationType::Delete) {
                expanded.push(mutation);
                continue;
            }
            self.expand_one_protein_delete(mutation, &schemas, storage_prefix, &mut expanded)
                .await?;
        }
        Ok(expanded)
    }
}
