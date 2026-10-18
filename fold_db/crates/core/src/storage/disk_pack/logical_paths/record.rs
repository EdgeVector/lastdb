//! Document envelopes reuse the same logical molecule and full atom owner.

use super::*;
use crate::atom::AtomPartition;
use crate::db_operations::atom_store::PerKeyRecord;
use crate::resident::CoreWarmLookup;
use crate::schema::types::key_value::KeyValue;
use crate::storage::disk_pack::read::ReadContext;

pub(crate) fn lookup(
    runtime: Option<Arc<PackRuntime>>,
    schema: &Schema,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
) -> StorageResult<Option<Option<Arc<Atom>>>> {
    let Some(runtime) = runtime else {
        return Ok(None);
    };
    if !field::provenance_current() || transaction::PendingBatch::current(&runtime).is_some() {
        return Ok(None);
    }
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(None);
    };
    let binding = field::FieldScope::record(schema)?;
    let path = binding.path(scope, molecule, key, "");
    let captured = super::read::captured_lookup(&runtime, &context, &path.core())?;
    if captured.is_absent() {
        let mut call = crate::read_receipts::lock(&context.call);
        context.account_memory_record(&mut call, path.id()?.len(), 0)?;
        crate::read_receipts::warm_hit();
        return Ok(Some(None));
    }
    if let Some(selected) = captured.into_value() {
        let mut call = crate::read_receipts::lock(&context.call);
        context.account_memory_record(&mut call, path.id()?.len(), selected.bytes)?;
        crate::read_receipts::warm_hit();
        return Ok(Some(Some(selected.atom)));
    }
    let view = runtime.view.read().expect("record envelope captured view");
    if view.revision != context.revision {
        return Ok(None);
    }
    let mut call = crate::read_receipts::lock(&context.call);
    let mut core = runtime.core.lock().expect("record envelope core model");
    let path_key = path.core();
    if !core.path_authorized(&path_key) {
        return Ok(None);
    }
    context.hold_core(&mut core, std::slice::from_ref(&path_key))?;
    let selected: CoreWarmLookup = core.lookup(&path_key);
    if selected.is_unknown() {
        return Ok(None);
    }
    if selected.is_absent() {
        context.account_memory_record(&mut call, path.id()?.len(), 0)?;
        crate::read_receipts::warm_hit();
        return Ok(Some(None));
    }
    let Some(selected) = selected.into_value() else {
        return Ok(None);
    };
    context.account_memory_record(&mut call, path.id()?.len(), selected.bytes)?;
    crate::read_receipts::warm_hit();
    Ok(Some(Some(selected.atom)))
}

pub(crate) fn admit(
    runtime: Option<Arc<PackRuntime>>,
    schema: &Schema,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    tip: (&str, &PerKeyRecord, &[String], crate::atom::AtomKeyEncoding),
    hint: Option<&AtomPartition>,
) -> StorageResult<()> {
    let Some(runtime) = runtime else {
        return Ok(());
    };
    if !field::provenance_current() || transaction::PendingBatch::current(&runtime).is_some() {
        return Ok(());
    }
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(());
    };
    let (base, record, candidates, encoding) = tip;
    let source = crate::schema::types::field::build_storage_key(scope, base);
    core_read::note_winning_tip(
        Some(Arc::clone(&runtime)),
        scope,
        molecule,
        key,
        &record.entry,
        record.meta.as_ref(),
        &source,
    )?;
    core_read::note_candidates(
        Some(Arc::clone(&runtime)),
        scope,
        molecule,
        key,
        &record.entry.atom_uuid,
        candidates,
        encoding,
    )?;
    let binding = field::FieldScope::record(schema)?;
    let path = binding.path(scope, molecule, key, &record.entry.atom_uuid);
    if let Some(facts) = source::atom_facts(
        Some(Arc::clone(&runtime)),
        scope,
        &record.entry.atom_uuid,
        hint,
    )? {
        runtime.admit_field_path(&context, &path, facts, key)?;
    }
    Ok(())
}

pub(crate) fn admit_absent(
    runtime: Option<Arc<PackRuntime>>,
    schema: &Schema,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    candidates: &[String],
) -> StorageResult<()> {
    let binding = field::FieldScope::record(schema)?;
    super::absence::admit_bound(runtime, &binding, scope, molecule, key, candidates)
}
