//! A field resolver carries the complete identity before it asks for atom bytes.

use super::body::PathBody;
use super::*;
use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::read::ReadContext;

#[derive(Clone)]
pub(crate) struct FieldScope {
    schema: String,
    field: String,
}

tokio::task_local! {
    static FIELD: FieldScope;
}

impl FieldScope {
    pub(crate) fn for_runtime(
        runtime: Option<Arc<PackRuntime>>,
        schema: &Schema,
        name: &str,
    ) -> StorageResult<Self> {
        if runtime.is_none() {
            return Ok(Self {
                schema: String::new(),
                field: String::new(),
            });
        }
        Self::new(schema, name)
    }

    pub(crate) fn new(schema: &Schema, name: &str) -> StorageResult<Self> {
        Ok(Self {
            schema: schema_identity(schema)?,
            field: schema.field_hashes.get(name).cloned().ok_or_else(|| {
                StorageError::BackendError("native read field has no canonical identity".into())
            })?,
        })
    }

    pub(crate) async fn run<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        if self.schema.is_empty() {
            return future.await;
        }
        FIELD.scope(self.clone(), future).await
    }

    fn path(&self, scope: Option<&str>, molecule: &str, key: &KeyValue, atom: &str) -> LogicalPath {
        LogicalPath {
            scope: scope.map(str::to_owned),
            schema: self.schema.clone(),
            field: self.field.clone(),
            molecule: molecule.to_string(),
            hash: key.hash.clone().unwrap_or_default(),
            range: key.range.clone().unwrap_or_default(),
            atom: atom.to_string(),
        }
    }
}

/// A miss reserves the path before any body is fetched. The key budget is shared.
pub(crate) fn lookup(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: Option<&str>,
    key: &KeyValue,
    atom: &str,
) -> StorageResult<Option<FieldValue>> {
    let Some((runtime, binding, molecule)) = active(runtime, molecule) else {
        return Ok(None);
    };
    let context = ReadContext::captured(&runtime).ok_or_else(missing_context)?;
    let id = binding.path(scope, &molecule, key, atom).id()?;
    let mut call = crate::read_receipts::lock(&context.call);
    let mut logical = runtime
        .logical
        .lock()
        .expect("native complete logical paths");
    context.hold_record(&mut logical, PATH_COLLECTION, &id)?;
    let value = match logical.fetched_record(PATH_COLLECTION, &id).flatten() {
        Some(bytes) => {
            call.account_memory_record(id.len(), bytes.len())
                .map_err(map)?;
            serde_json::from_slice::<PathBody>(&bytes)
                .map_err(|error| serialization(&error))?
                .field(context.version)
        }
        None => None,
    };
    if value.is_some() {
        laststore::record_logical_warm_hits(1);
    } else {
        laststore::record_logical_warm_misses(1);
    }
    Ok(value)
}

/// Admission occurs before content tombstone and predicate filters remove values.
pub(crate) fn admit(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: Option<&str>,
    key: &KeyValue,
    value: &FieldValue,
) -> StorageResult<()> {
    let Some((runtime, binding, molecule)) = active(runtime, molecule) else {
        return Ok(());
    };
    let context = ReadContext::captured(&runtime).ok_or_else(missing_context)?;
    let path = binding.path(scope, &molecule, key, &value.atom_uuid);
    runtime.admit_field_path(&context, &path, value)
}

fn active(
    runtime: Option<Arc<PackRuntime>>,
    molecule: Option<&str>,
) -> Option<(Arc<PackRuntime>, FieldScope, String)> {
    Some((
        runtime?,
        FIELD.try_with(Clone::clone).ok()?,
        molecule?.to_string(),
    ))
}

fn missing_context() -> StorageError {
    StorageError::InvalidOperation("native logical read requires its complete query view".into())
}
