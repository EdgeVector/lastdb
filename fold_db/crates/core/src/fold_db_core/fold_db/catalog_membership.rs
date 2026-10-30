//! Catalog rows that bind the molecules of their schema.
//!
//! A catalog row names a schema instance for a database. Each field molecule of
//! that schema in the instance scope has one binding record for the row, and the
//! `bindings` counter of the molecule counts it. The share route and the cloud
//! import bind and unbind through `AtomStore`. The two functions here are the
//! other writers of a catalog row, the put route and the declare of a schema
//! under a named database. They bind before they write the row, so a row never
//! names an unbound molecule. A bind that already exists changes nothing, so a
//! repeat and a retry after a crash are safe.

use super::FoldDB;
use crate::db_operations::{DbCatalogEntry, DbCatalogKeySelection};
use crate::error::FoldDbResult;
use crate::schema::SchemaError;

enum CatalogMoleculeChange {
    Bind,
    Unbind,
}

impl FoldDB {
    /// Write one catalog row, and bind the field molecules of its schema in the
    /// scope of the row. A row that moves to another instance first unbinds the
    /// scope it leaves. A schema that is not installed has no molecules to bind.
    pub async fn put_catalog_membership(&self, entry: &DbCatalogEntry) -> FoldDbResult<()> {
        // The bindings and the row are one batch, so a crash cannot leave a
        // binding for a row that was never written.
        self.db_ops()
            .atoms()
            .in_write_scope(self.put_catalog_entry_in_scope(entry))
            .await?;
        Ok(())
    }

    async fn put_catalog_entry_in_scope(&self, entry: &DbCatalogEntry) -> Result<(), SchemaError> {
        let catalog = self.db_ops().db_catalog();
        let old = catalog.get(&entry.db_locator, &entry.schema_name).await?;
        if let Some(old) = old
            .as_ref()
            .filter(|old| old.instance_id != entry.instance_id)
        {
            self.change_catalog_molecules(old, CatalogMoleculeChange::Unbind)
                .await?;
        }
        self.change_catalog_molecules(entry, CatalogMoleculeChange::Bind)
            .await?;
        catalog.put(entry).await?;
        Ok(())
    }

    /// The row of a named database for a schema, written when it is absent. An
    /// existing row wins, so a re-declare does not mint a second instance, and
    /// its molecules are bound again. The read, bind, and optional insert share
    /// one native write scope, so another writer cannot remove or replace the
    /// selected row before its bindings are established.
    pub async fn ensure_catalog_membership(
        &self,
        db_locator: &str,
        schema_name: &str,
        instance_id: Option<String>,
    ) -> FoldDbResult<DbCatalogEntry> {
        Ok(self
            .db_ops()
            .atoms()
            .in_write_scope(async {
                let catalog = self.db_ops().db_catalog();
                if let Some(existing) = catalog.get(db_locator, schema_name).await? {
                    self.change_catalog_molecules(&existing, CatalogMoleculeChange::Bind)
                        .await?;
                    return Ok(existing);
                }
                let entry = DbCatalogEntry {
                    db_locator: db_locator.to_string(),
                    schema_name: schema_name.to_string(),
                    instance_id,
                    key_selection: DbCatalogKeySelection::WholeSchema,
                };
                self.put_catalog_entry_in_scope(&entry).await?;
                Ok(entry)
            })
            .await?)
    }

    /// Bind or unbind the field molecules of one catalog row.
    /// A missing schema still does nothing.
    async fn change_catalog_molecules(
        &self,
        entry: &DbCatalogEntry,
        change: CatalogMoleculeChange,
    ) -> Result<(), SchemaError> {
        let Some(schema) = self
            .schema_manager()
            .get_schema_metadata(&entry.schema_name)?
        else {
            return Ok(());
        };
        let atoms = self.db_ops().atoms();
        let db = entry.db_locator.as_str();
        let name = entry.schema_name.as_str();
        let prefix = entry.instance_id.as_deref();
        match change {
            CatalogMoleculeChange::Bind => {
                atoms
                    .bind_catalog_molecules(db, name, &schema, prefix)
                    .await
            }
            CatalogMoleculeChange::Unbind => {
                atoms
                    .unbind_catalog_molecules(db, name, &schema, prefix)
                    .await
            }
        }
    }
}
