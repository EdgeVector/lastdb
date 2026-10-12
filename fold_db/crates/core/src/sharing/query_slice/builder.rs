//! Per-leg accumulation of a query-defined slice.

use super::*;

/// Accumulates the schemas, molecules, atoms and blobs for the legs of one slice.
#[derive(Default)]
pub(super) struct SliceBuilder {
    pub(super) schemas: Vec<SliceSchema>,
    pub(super) molecules: Vec<SignedMolecule>,
    pub(super) atoms_by_ref: BTreeMap<String, ContentAddressedAtom>,
    pub(super) blobs_by_ref: BTreeMap<String, ContentAddressedBlob>,
    pub(super) seen_schema: BTreeSet<String>,
}

impl SliceBuilder {
    /// Run one query leg and fold its rows into the slice.
    pub(super) async fn add_leg(
        &mut self,
        db: &FoldDB,
        leg: &QuerySliceLeg,
        access: &AccessContext,
        blob_resolver: Option<&BlobBytesResolver>,
    ) -> Result<(), FoldDbError> {
        if leg.fields.is_empty() {
            return Err(FoldDbError::Other(format!(
                "query leg for schema '{}' has no fields",
                leg.schema_name
            )));
        }

        let schema = db
            .schema_manager()
            .get_schema_following_supersession(&leg.schema_name)
            .await?
            .ok_or_else(|| {
                FoldDbError::Schema(crate::schema::types::SchemaError::InvalidData(format!(
                    "schema '{}' not found",
                    leg.schema_name
                )))
            })?;

        if self.seen_schema.insert(leg.schema_name.clone()) {
            let definition = serde_json::to_value(&schema).map_err(FoldDbError::from)?;
            self.schemas.push(SliceSchema {
                schema_name: leg.schema_name.clone(),
                definition,
                fields: leg.fields.clone(),
            });
        }

        let query = leg.to_query();
        let result = db.query_executor().query_with_access(query, access).await?;

        let mut keys: HashSet<KeyValue> = HashSet::new();
        for field_map in result.values() {
            for key in field_map.keys() {
                keys.insert(key.clone());
            }
        }

        for key in keys {
            let record_key = key.to_storage_key();
            for field_name in &leg.fields {
                let Some(field_map) = result.get(field_name) else {
                    continue;
                };
                let Some(fv) = field_map.get(&key) else {
                    continue;
                };
                let atom_value = normalize_file_field_value(
                    db,
                    &fv.value,
                    blob_resolver,
                    &mut self.blobs_by_ref,
                )
                .await?;
                let atom = content_addressed_atom(atom_value)?;
                self.molecules.push(SignedMolecule {
                    schema_name: leg.schema_name.clone(),
                    record_key: record_key.clone(),
                    field_name: field_name.clone(),
                    atom_ref: atom.atom_ref.clone(),
                    molecule_uuid: fv.molecule_uuid.clone(),
                    molecule_version: fv.molecule_version,
                    writer_pubkey: fv.writer_pubkey.clone(),
                    signature: None,
                });
                self.atoms_by_ref
                    .entry(atom.atom_ref.clone())
                    .or_insert(atom);
            }
        }

        Ok(())
    }
}
