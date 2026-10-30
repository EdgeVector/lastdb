//! Database-catalog bindings of molecules.
//!
//! A catalog entry binds the field molecules of one schema into a named
//! database. The binding is one binding record for each field and one step of
//! the `bindings` counter of each molecule. It adds no reference to an atom: the
//! atom counter counts tips and conflict records, and a slot of a shared
//! molecule is shared by every database that binds the molecule.
//!
//! The cost of a bind or an unbind follows the number of fields of the schema.
//! It does not follow the number of keys of a molecule, and it reads and writes
//! no atom and no tip.
//!
//! Both calls are idempotent and write one batch. A caller joins them with the
//! write of the catalog row in one native write scope (`fold_db_core/fold_db/
//! catalog_*.rs`), so a crash keeps both or neither.

use super::molecule_ref_edges::MoleculeRefEdge;
use super::AtomStore;
use crate::schema::types::Schema;
use crate::schema::SchemaError;

impl AtomStore {
    /// Write the binding record of every field of `schema` for the database
    /// `db_locator`, and raise the `bindings` counter of each molecule.
    pub(crate) async fn bind_catalog_molecules(
        &self,
        db_locator: &str,
        schema_name: &str,
        schema: &Schema,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.require_catalog_conversion_unfrozen().await?;
        let edges = catalog_edges(db_locator, schema_name, schema);
        self.add_molecule_bindings(&edges, storage_prefix)
            .await
            .map(|_| ())
    }

    /// Delete the binding record of every field of `schema` for the database
    /// `db_locator`, and lower the `bindings` counter of each molecule.
    pub(crate) async fn unbind_catalog_molecules(
        &self,
        db_locator: &str,
        schema_name: &str,
        schema: &Schema,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.require_catalog_conversion_unfrozen().await?;
        let edges = catalog_edges(db_locator, schema_name, schema);
        self.remove_molecule_bindings(&edges, storage_prefix)
            .await
            .map(|_| ())
    }

    async fn require_catalog_conversion_unfrozen(&self) -> Result<(), SchemaError> {
        use crate::fold_db_core::fold_db::app_schema_conversion::{authorized, ACTIVE, NAMESPACE};
        if !authorized() {
            if let Some(store) = &self.namespaced_store {
                if store
                    .open_namespace(NAMESPACE)
                    .await?
                    .get(ACTIVE)
                    .await?
                    .is_some()
                {
                    return Err(SchemaError::Blocked(
                        "app schema conversion freezes database bindings".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// One binding record for each field molecule of `schema`, in field order.
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
