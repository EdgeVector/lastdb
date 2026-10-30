//! Existing core fields share one partial molecule and atom model.
use super::*;
use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::read::ReadContext;

#[derive(Clone)]
pub(crate) struct FieldScope {
    pub(super) schema: String,
    pub(super) field: String,
}

pub(crate) struct AtomSelection<'a> {
    pub entry: &'a crate::schema::types::field::KeyedAtomMatch,
    pub primary: String,
}

tokio::task_local! {
    static FIELD: FieldScope;
    static CURRENT_PROVENANCE: ();
}

pub(crate) async fn query_scope<T>(future: impl std::future::Future<Output = T>) -> T {
    CURRENT_PROVENANCE.scope((), future).await
}

pub(in crate::storage::disk_pack) fn provenance_current() -> bool {
    CURRENT_PROVENANCE.try_with(|_| ()).is_ok()
}

impl FieldScope {
    pub(crate) fn new(schema: &Schema, name: &str) -> StorageResult<Self> {
        Ok(Self {
            schema: schema_identity(schema)?,
            field: schema.field_hashes.get(name).cloned().ok_or_else(|| {
                StorageError::BackendError("native read field has no canonical identity".into())
            })?,
        })
    }

    pub(crate) async fn run<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        FIELD.scope(self.clone(), future).await
    }

    pub(in crate::storage::disk_pack) fn current() -> Option<Self> {
        FIELD.try_with(Clone::clone).ok()
    }

    pub(in crate::storage::disk_pack) fn schema_binding_identity(
        &self,
    ) -> StorageResult<(String, Option<String>)> {
        serde_json::from_str(&self.schema).map_err(|_| {
            StorageError::InvalidOperation("native field schema identity is invalid".into())
        })
    }

    pub(in crate::storage::disk_pack) fn path(
        &self,
        scope: Option<&str>,
        molecule: &str,
        key: &KeyValue,
        atom: &str,
    ) -> LogicalPath {
        LogicalPath {
            scope: scope.map(str::to_owned),
            schema: self.schema.clone(),
            field: self.field.clone(),
            molecule: molecule.to_owned(),
            hash: key.hash.clone().unwrap_or_default(),
            range: key.range.clone().unwrap_or_default(),
            atom: atom.to_owned(),
        }
    }
}

/// The normal resolver selected this tip; reuse its full shared atom object.
pub(crate) fn lookup(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: Option<&str>,
    selection: &AtomSelection<'_>,
) -> StorageResult<Option<FieldValue>> {
    let Some((runtime, binding, molecule)) = active(runtime, molecule) else {
        return Ok(None);
    };
    if !provenance_current() || transaction::PendingBatch::current(&runtime).is_some() {
        return Ok(None);
    }
    let context = ReadContext::captured(&runtime).ok_or_else(missing_context)?;
    let (key, atom, metadata, writer) = selection.entry;
    let path = binding.path(scope, &molecule, key, atom);
    let tip = core_read::selected_tip(Some(Arc::clone(&runtime)), scope, &molecule, key, atom)?;
    let Some(mut held) = super::lock_current_receipt(
        &runtime,
        &context,
        "native current core lookup",
        "native shared core model",
    ) else {
        return Ok(None);
    };
    let path_key = path.core();
    context.hold_core(&mut held.core, std::slice::from_ref(&path_key))?;
    let Some(selected) = held.core.lookup(&path_key).into_value() else {
        laststore::record_logical_warm_misses(1);
        return Ok(None);
    };
    if selected.atom.uuid() != atom {
        laststore::record_logical_warm_misses(1);
        return Ok(None);
    }
    if !source::body_matches(&held.core, &path_key, &selection.primary) {
        laststore::record_logical_warm_misses(1);
        return Ok(None);
    }
    if let Some(tip) = tip {
        super::read::refresh_tip(&mut held.core, &path_key, tip, &selection.primary);
    }
    context.account_memory_record(&mut held.call, path.id()?.len(), selected.bytes)?;
    crate::read_receipts::warm_hit();
    Ok(Some(crate::schema::types::field::field_value_from_atom(
        &selected.atom,
        atom.to_owned(),
        metadata.clone(),
        writer.clone(),
    )))
}

pub(crate) fn admit(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: Option<&str>,
    key: &KeyValue,
    atom: crate::atom::Atom,
) -> StorageResult<()> {
    let Some((runtime, binding, molecule)) = active(runtime, molecule) else {
        return Ok(());
    };
    if !provenance_current() {
        return Ok(());
    }
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(());
    };
    let path = binding.path(scope, &molecule, key, atom.uuid());
    if let Some(facts) = source::atom_facts(Some(Arc::clone(&runtime)), scope, atom)? {
        runtime.admit_field_path(&context, &path, facts, key)?;
    }
    Ok(())
}

fn active(
    runtime: Option<Arc<PackRuntime>>,
    molecule: Option<&str>,
) -> Option<(Arc<PackRuntime>, FieldScope, String)> {
    Some((runtime?, FieldScope::current()?, molecule?.to_owned()))
}

fn missing_context() -> StorageError {
    StorageError::InvalidOperation("native core read requires its captured query view".into())
}
