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
    name: String,
}

pub(crate) struct AtomSelection<'a> {
    pub entry: &'a crate::schema::types::field::KeyedAtomMatch,
    pub primary: String,
}

tokio::task_local! {
    static FIELD: FieldScope;
    static CURRENT_PROVENANCE: bool;
}

pub(crate) async fn query_scope<T>(
    current: bool,
    future: impl std::future::Future<Output = T>,
) -> T {
    CURRENT_PROVENANCE.scope(current, future).await
}

pub(super) fn provenance_current() -> bool {
    CURRENT_PROVENANCE
        .try_with(|current| *current)
        .unwrap_or(false)
}

impl FieldScope {
    pub(crate) fn for_runtime(native: bool, schema: &Schema, name: &str) -> StorageResult<Self> {
        if !native {
            return Ok(Self {
                schema: String::new(),
                field: String::new(),
                name: String::new(),
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
            name: name.to_string(),
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
    selection: AtomSelection<'_>,
) -> StorageResult<Option<FieldValue>> {
    let (key, atom, metadata, writer, _) = selection.entry;
    let Some((runtime, binding, molecule)) = active(runtime, molecule) else {
        return Ok(None);
    };
    let context = ReadContext::captured(&runtime).ok_or_else(missing_context)?;
    let id = binding.path(scope, &molecule, key, atom).id()?;
    context
        .paths
        .lock()
        .expect("native resolved paths")
        .note(&binding.name, key, atom, id.clone());
    let mut call = crate::read_receipts::lock(&context.call);
    let mut logical = runtime
        .logical
        .lock()
        .expect("native complete logical paths");
    context.hold_record(&mut logical, PATH_COLLECTION, &id)?;
    let value = match logical.fetched_record_bytes(PATH_COLLECTION, &id).flatten() {
        Some(length) => {
            context.account_memory_record(&mut call, id.len(), length)?;
            let bytes = logical
                .fetched_record(PATH_COLLECTION, &id)
                .flatten()
                .ok_or_else(|| {
                    StorageError::BackendError("held native logical body disappeared".into())
                })?;
            let body = serde_json::from_slice::<PathBody>(&bytes)
                .map_err(|error| serialization(&error))?;
            (provenance_current()
                && body
                    .atom_facts
                    .as_ref()
                    .is_some_and(|facts| facts.primary(&selection.primary)))
            .then(|| body.selected_field(&context, atom, metadata.as_ref(), writer.as_deref()))
            .flatten()
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
    hint: Option<&crate::atom::AtomPartition>,
) -> StorageResult<()> {
    let Some((runtime, binding, molecule)) = active(runtime, molecule) else {
        return Ok(());
    };
    let context = ReadContext::captured(&runtime).ok_or_else(missing_context)?;
    let path = binding.path(scope, &molecule, key, &value.atom_uuid);
    let facts =
        super::source::atom_facts(Some(Arc::clone(&runtime)), scope, &value.atom_uuid, hint)?;
    runtime.admit_field_path(&context, &path, value, facts)?;
    context.paths.lock().expect("native resolved paths").note(
        &binding.name,
        key,
        &value.atom_uuid,
        path.id()?,
    );
    Ok(())
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
