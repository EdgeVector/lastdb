//! Per-leg accumulation of a query-defined slice.

use super::*;
use std::collections::BTreeSet;

/// Accumulates the schemas, field keys, records and blobs for the legs of one slice.
#[derive(Default)]
pub(super) struct SliceBuilder {
    pub(super) schemas: Vec<SliceSchema>,
    pub(super) field_keys: Vec<SignedFieldKey>,
    pub(super) records_by_ref: BTreeMap<String, ContentAddressedValue>,
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

        // A keyed leg reads in pages, so a run of deleted records does not
        // end the delivery at the read limit (`pages.rs`).
        let result = super::pages::read_leg(db, &schema, leg.to_query(), access).await?;

        for (key, record) in &result {
            let record_key = key.to_storage_key();
            for field_name in &leg.fields {
                let Some(value) = record.fields.get(field_name) else {
                    continue;
                };
                let metadata = record.metadata.get(field_name);
                let value =
                    normalize_file_field_value(db, value, blob_resolver, &mut self.blobs_by_ref)
                        .await?;
                let record = content_addressed_value(value)?;
                self.field_keys.push(SignedFieldKey {
                    schema_name: leg.schema_name.clone(),
                    record_key: record_key.clone(),
                    field_name: field_name.clone(),
                    value_ref: record.value_ref.clone(),
                    field_key_id: metadata.and_then(|metadata| metadata.field_key_id.clone()),
                    writer_pubkey: metadata.and_then(|metadata| metadata.writer_pubkey.clone()),
                    signature: None,
                });
                self.records_by_ref
                    .entry(record.value_ref.clone())
                    .or_insert(record);
            }
        }

        Ok(())
    }
}
