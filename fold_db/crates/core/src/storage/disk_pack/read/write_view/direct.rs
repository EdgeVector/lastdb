//! Native typed hits never pass through a JSON byte buffer or parser.

use super::*;
use crate::atom::Atom;
use crate::db_operations::atom_store::{counter::AtomCounter, PerKeyRecord};
use crate::resident::{clone_record_value, CoreSourceRecord, CoreWarmSet, TypedRecordValue};
use std::any::{Any, TypeId};

pub(in crate::storage::disk_pack::read) fn select<T: Any + Send + Sync>(
    core: &mut CoreWarmSet,
    context: &ReadContext,
    call: &mut PhysicalRead,
    coordinate: &Coordinate,
    counts: &mut WriteViewCounts,
) -> StorageResult<Option<Option<T>>> {
    let target = TypeId::of::<T>();
    if let Some(fact) = facts::get(core, coordinate) {
        let value = match &**fact {
            facts::WriteFact::Absent => {
                charge(context, call, coordinate, 0)?;
                counts.note_direct(coordinate, 0, true, WriteOwner::Fact, false);
                return Ok(Some(None));
            }
            facts::WriteFact::Counter { value, wire_bytes }
                if compatible::<AtomCounter>(target) =>
            {
                charge(context, call, coordinate, *wire_bytes)?;
                let result =
                    clone_record_value(value, target).map_err(|error| serialization(&error))?;
                (*wire_bytes, result)
            }
            _ => return Ok(None),
        };
        counts.note_direct(
            coordinate,
            value.0,
            false,
            WriteOwner::Fact,
            is_value(target),
        );
        return cast(value.1).map(|value| value.map(Some));
    }
    let uuid = crate::atom::atom_key_codec::body_uuid_of(&coordinate.1);
    if uuid.is_some() || crate::kind_partition::rest_of(&coordinate.1, "mk").is_some() {
        if let Some(record) = core.source_record(&coordinate.0, &coordinate.1, uuid) {
            return source(context, call, coordinate, record, counts);
        }
    }
    let Some(reader) = core.memory_record(&coordinate.0, &coordinate.1) else {
        return Ok(None);
    };
    if reader.record_present(&coordinate.0) == Some(false) {
        charge(context, call, coordinate, 0)?;
        counts.note_direct(coordinate, 0, true, WriteOwner::Definition, false);
        return Ok(Some(None));
    }
    if reader.record_type(&coordinate.0) != target && !is_value(target) {
        return Ok(None);
    }
    let bytes = reader
        .wire_bytes(&coordinate.0)
        .map_err(|error| serialization(&error))?;
    charge(context, call, coordinate, bytes)?;
    let result = reader
        .clone_record(&coordinate.0, target)
        .map_err(|error| serialization(&error))?;
    counts.note_direct(
        coordinate,
        bytes,
        false,
        WriteOwner::Definition,
        is_value(target),
    );
    cast(result).map(|value| value.map(Some))
}

fn source<T: Any + Send + Sync>(
    context: &ReadContext,
    call: &mut PhysicalRead,
    coordinate: &Coordinate,
    record: CoreSourceRecord<'_>,
    counts: &mut WriteViewCounts,
) -> StorageResult<Option<Option<T>>> {
    let target = TypeId::of::<T>();
    let (value, bytes, owner) = match record {
        CoreSourceRecord::Atom(atom, bytes) if target == TypeId::of::<Arc<Atom>>() => {
            charge(context, call, coordinate, bytes)?;
            (
                Some(Box::new(Arc::clone(atom)) as TypedRecordValue),
                bytes,
                WriteOwner::Atom,
            )
        }
        CoreSourceRecord::Atom(atom, bytes) if compatible::<Atom>(target) => {
            charge(context, call, coordinate, bytes)?;
            (
                clone_record_value(atom.as_ref(), target).map_err(|error| serialization(&error))?,
                bytes,
                WriteOwner::Atom,
            )
        }
        CoreSourceRecord::Tip(entry, meta, bytes) if compatible::<PerKeyRecord>(target) => {
            charge(context, call, coordinate, bytes)?;
            let tip = PerKeyRecord {
                entry: entry.clone(),
                meta: meta.cloned(),
            };
            let value = if is_value(target) {
                Some(
                    Box::new(serde_json::to_value(tip).map_err(|error| serialization(&error))?)
                        as TypedRecordValue,
                )
            } else {
                Some(Box::new(tip) as TypedRecordValue)
            };
            (value, bytes, WriteOwner::Tip)
        }
        _ => return Ok(None),
    };
    counts.note_direct(coordinate, bytes, false, owner, is_value(target));
    cast(value).map(|value| value.map(Some))
}

fn cast<T: Any + Send + Sync>(value: Option<TypedRecordValue>) -> StorageResult<Option<T>> {
    value
        .map(|value| {
            value
                .downcast::<T>()
                .map(|value| *value)
                .map_err(|_| StorageError::BackendError("direct record type differs".into()))
        })
        .transpose()
}

fn compatible<T: Any>(target: TypeId) -> bool {
    target == TypeId::of::<T>() || is_value(target)
}
fn is_value(target: TypeId) -> bool {
    target == TypeId::of::<serde_json::Value>()
}

fn charge(
    context: &ReadContext,
    call: &mut PhysicalRead,
    coordinate: &Coordinate,
    bytes: usize,
) -> StorageResult<()> {
    context.account_memory_record(call, coordinate.0.len() + coordinate.1.len(), bytes)
}

fn serialization(error: &serde_json::Error) -> StorageError {
    StorageError::SerializationError(error.to_string())
}

pub(in crate::storage::disk_pack::read) fn retain<T: Any>(
    core: &mut CoreWarmSet,
    coordinate: &Coordinate,
    value: Option<&T>,
    bytes: usize,
) {
    facts::retain_typed(
        core,
        coordinate,
        value.map(|value| (value as &dyn Any, bytes)),
    );
}
