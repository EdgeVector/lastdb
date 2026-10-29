//! Serialize borrowed typed owners into one temporary, budgeted call result.

use super::*;
use crate::resident::{CoreSourceRecord, CoreWarmSet};
use std::io::Write;

#[derive(serde::Serialize)]
struct Tip<'a> {
    entry: &'a crate::atom::AtomEntry,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<&'a crate::atom::KeyMetadata>,
}

pub(super) fn read(
    core: &mut CoreWarmSet,
    context: &ReadContext,
    call: &mut PhysicalRead,
    coordinate: &Coordinate,
) -> StorageResult<Option<WriteRecord>> {
    if let Some(value) = facts::get(core, coordinate) {
        let body = match &**value {
            facts::WriteFact::Absent => absent(context, call, coordinate)?,
            facts::WriteFact::Counter { value: counter, .. } => {
                encode(context, call, coordinate, |out| {
                    serde_json::to_writer(out, counter)
                })?
            }
        };
        return Ok(Some(WriteRecord::Core {
            body,
            owner: WriteOwner::Fact,
        }));
    }
    let uuid = crate::atom::atom_key_codec::body_uuid_of(&coordinate.1);
    if let Some(value) = core.source_record(&coordinate.0, &coordinate.1, uuid) {
        let (body, owner) = match value {
            CoreSourceRecord::Atom(atom, _) => (
                encode(context, call, coordinate, |out| {
                    serde_json::to_writer(out, atom.as_ref())
                })?,
                WriteOwner::Atom,
            ),
            CoreSourceRecord::Tip(entry, meta, _) => (
                encode(context, call, coordinate, |out| {
                    serde_json::to_writer(out, &Tip { entry, meta })
                })?,
                WriteOwner::Tip,
            ),
        };
        return Ok(Some(WriteRecord::Core { body, owner }));
    }
    let Some(value) = core.memory_record(&coordinate.0, &coordinate.1) else {
        return Ok(None);
    };
    let body = if value.record_present(&coordinate.0) == Some(false) {
        absent(context, call, coordinate)?
    } else {
        encode(context, call, coordinate, |out| {
            value.write_record(&coordinate.0, out)
        })?
    };
    Ok(Some(WriteRecord::Core {
        body,
        owner: WriteOwner::Definition,
    }))
}

fn absent(
    context: &ReadContext,
    call: &mut PhysicalRead,
    coordinate: &Coordinate,
) -> StorageResult<Option<Vec<u8>>> {
    context.account_memory_record(call, coordinate.0.len() + coordinate.1.len(), 0)?;
    Ok(None)
}

fn encode(
    context: &ReadContext,
    call: &mut PhysicalRead,
    coordinate: &Coordinate,
    write: impl Fn(&mut dyn Write) -> serde_json::Result<()>,
) -> StorageResult<Option<Vec<u8>>> {
    let mut length = EncodedLength(0);
    write(&mut length).map_err(|error| serialization(&error))?;
    context.account_memory_record(call, coordinate.0.len() + coordinate.1.len(), length.0)?;
    let mut bytes = Vec::with_capacity(length.0);
    write(&mut bytes).map_err(|error| serialization(&error))?;
    Ok(Some(bytes))
}

struct EncodedLength(usize);

impl Write for EncodedLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("typed record length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialization(error: &serde_json::Error) -> StorageError {
    StorageError::SerializationError(format!("encode common typed write record: {error}"))
}
