//! Database-catalog bindings of molecules.
//!
//! A catalog entry binds the field molecules of one schema into a named
//! database. The binding is one molecule reference edge for each field. It
//! adds no reference to an atom: the atom counter counts tips and conflict
//! records, and a slot of a shared molecule is shared by every database that
//! binds the molecule.
//!
//! Both calls are idempotent and write one batch. A caller binds before it
//! writes the catalog row, and unbinds after it deletes the catalog row.

use super::molecule_ref_edges::MoleculeRefEdge;
use super::AtomStore;
use crate::schema::types::Schema;
use crate::schema::SchemaError;
use crate::storage::KvMutation;

/// Value of the marker that says a database catalog has bound a molecule.
const BINDING_MARKER: &[u8] = &[0x01];

impl AtomStore {
    /// Write the molecule reference edge of every field of `schema` for the
    /// database `db_locator`, and the marker that a binding exists.
    pub(crate) async fn bind_catalog_molecules(
        &self,
        db_locator: &str,
        schema_name: &str,
        schema: &Schema,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let edges = catalog_edges(db_locator, schema_name, schema);
        if edges.is_empty() {
            return Ok(());
        }
        let mut mutations = Vec::with_capacity(edges.len() + 1);
        for edge in &edges {
            let value = serde_json::to_vec(edge).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "serialize database-catalog molecule reference: {error}"
                ))
            })?;
            mutations.push(KvMutation::put(
                edge.storage_key(storage_prefix).into_bytes(),
                value,
            ));
        }
        mutations.push(KvMutation::put(
            Self::database_catalog_ref_present_key(storage_prefix).into_bytes(),
            BINDING_MARKER,
        ));
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("bind database-catalog molecules: {error}"))
            })
    }

    /// Delete the molecule reference edge of every field of `schema` for the
    /// database `db_locator`.
    pub(crate) async fn unbind_catalog_molecules(
        &self,
        db_locator: &str,
        schema_name: &str,
        schema: &Schema,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let mutations: Vec<KvMutation> = catalog_edges(db_locator, schema_name, schema)
            .iter()
            .map(|edge| KvMutation::delete(edge.storage_key(storage_prefix).into_bytes()))
            .collect();
        if mutations.is_empty() {
            return Ok(());
        }
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("unbind database-catalog molecules: {error}"))
            })
    }
}

/// One edge for each field molecule of `schema`, in field order.
fn catalog_edges(db_locator: &str, schema_name: &str, schema: &Schema) -> Vec<MoleculeRefEdge> {
    let mut fields: Vec<(&String, &String)> = schema
        .field_molecule_uuids
        .as_ref()
        .into_iter()
        .flat_map(|molecules| molecules.iter())
        .collect();
    fields.sort_unstable();
    fields
        .into_iter()
        .map(|(field, molecule)| {
            MoleculeRefEdge::database_catalog_field(db_locator, schema_name, field, molecule)
        })
        .collect()
}
