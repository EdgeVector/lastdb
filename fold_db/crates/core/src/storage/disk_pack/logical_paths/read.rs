//! Complete field paths, rather than intermediate stored coordinates, stay warm.

use super::*;
use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::read::ReadContext;
use std::collections::HashMap;

type FieldResults = HashMap<String, HashMap<KeyValue, FieldValue>>;

/// Field resolution supplies exact canonical identities and the selected atom.
/// All preceding physical bytes remain bounded call-local buffers.
pub(crate) fn admit_query_results(
    runtime: Option<Arc<PackRuntime>>,
    schema: &Schema,
    results: &mut FieldResults,
) -> StorageResult<()> {
    let Some(runtime) = runtime else {
        return Ok(());
    };
    let context = ReadContext::captured(&runtime).ok_or_else(|| {
        StorageError::InvalidOperation(
            "native logical read requires its complete query view".into(),
        )
    })?;
    let schema_identity = schema_identity(schema)?;
    for (field, values) in results {
        let canonical = schema.field_hashes.get(field).ok_or_else(|| {
            StorageError::BackendError("native read field has no canonical identity".into())
        })?;
        let declared = schema.runtime_fields.get(field).ok_or_else(|| {
            StorageError::BackendError(
                "native read field is absent from its captured schema".into(),
            )
        })?;
        for (key, value) in values {
            // Computed key projections do not claim an atom fetch.
            if value.atom_uuid.is_empty() {
                continue;
            }
            let selected = context.paths.lock().expect("native resolved paths").one(
                field,
                key,
                &value.atom_uuid,
            );
            if let Some(id) = selected {
                runtime.admit_field_id(&context, &id, value)?;
                continue;
            }
            let molecule = value
                .molecule_uuid
                .as_deref()
                .or_else(|| declared.common().molecule_uuid().map(String::as_str))
                .ok_or_else(|| {
                    StorageError::BackendError("native read field has no selected molecule".into())
                })?;
            let path = LogicalPath {
                scope: declared.common().storage_prefix.clone(),
                schema: schema_identity.clone(),
                field: canonical.clone(),
                molecule: molecule.to_string(),
                hash: key.hash.clone().unwrap_or_default(),
                range: key.range.clone().unwrap_or_default(),
                atom: value.atom_uuid.clone(),
            };
            runtime.admit_field_path(&context, &path, value, None)?;
            context.paths.lock().expect("native resolved paths").note(
                field,
                key,
                &value.atom_uuid,
                path.id()?,
            );
        }
    }
    Ok(())
}

impl PackRuntime {
    pub(super) fn admit_field_path(
        &self,
        context: &Arc<ReadContext>,
        path: &LogicalPath,
        value: &FieldValue,
        facts: Option<source::AtomFacts>,
    ) -> StorageResult<()> {
        let id = path.id()?;
        self.admit_field_id_with_facts(context, &id, value, facts, false)
    }

    pub(super) fn admit_field_id(
        &self,
        context: &Arc<ReadContext>,
        id: &str,
        value: &FieldValue,
    ) -> StorageResult<()> {
        self.admit_field_id_with_facts(context, id, value, None, true)
    }

    fn admit_field_id_with_facts(
        &self,
        context: &Arc<ReadContext>,
        id: &str,
        value: &FieldValue,
        facts: Option<source::AtomFacts>,
        preserve: bool,
    ) -> StorageResult<()> {
        #[derive(Deserialize)]
        struct Facts {
            atom_facts: Option<source::AtomFacts>,
        }
        let current = field::provenance_current();
        let mut call = crate::read_receipts::lock(&context.call);
        let mut logical = self.logical.lock().expect("native complete logical paths");
        let mut facts = facts.or_else(|| {
            if !preserve {
                return None;
            }
            logical
                .inspect_record(PATH_COLLECTION, id, |body| {
                    body.and_then(|body| serde_json::from_slice::<Facts>(body).ok())
                        .and_then(|body| body.atom_facts)
                        .filter(|facts| facts.valid(context))
                })
                .flatten()
        });
        if let Some(facts) = facts.as_mut() {
            facts.renew(context);
        }
        let length = if current {
            body::PathBody::from_field_encoded_len(context.version, value, None, facts.as_ref())?
        } else {
            body::PathBody::content_encoded_len(&value.value)?
        };
        context.account_memory_record(&mut call, id.len(), length)?;
        context.hold_record(&mut logical, PATH_COLLECTION, id)?;
        let value = if current {
            body::PathBody::from_field(context.version, value, None, facts)
        } else {
            body::PathBody::content(&value.value)
        };
        let body = serde_json::to_vec(&value).map_err(|error| serialization(&error))?;
        if body.len() != length {
            let result = Err(StorageError::BackendError(
                "native logical body encoded length differs from its admitted length".into(),
            ));
            context.remember(&result);
            return result;
        }
        let epoch = logical.record_epoch(id);
        if !logical.admit_record(PATH_COLLECTION, id, Some(body), epoch) {
            return Err(StorageError::ReadResourceLimit {
                detail: "native logical read lost its protected path slot".into(),
            });
        }
        Ok(())
    }
}
