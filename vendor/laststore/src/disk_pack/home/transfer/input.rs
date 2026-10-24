use super::super::{HomeBuilder, HomeDescriptor};
use super::wire::{self, DigestRead};
use super::TransferReceipt;
use crate::Result;
use std::io::Read;

pub(super) fn read_records(
    input: &mut dyn Read,
    builder: &mut HomeBuilder,
    descriptor: &HomeDescriptor,
    frontier: u64,
) -> Result<TransferReceipt> {
    let mut stream = DigestRead::new(input);
    let mut actual = TransferReceipt::default();
    let mut pending = stream.next_record()?;
    while let Some(first) = pending.take() {
        let collection = first.collection.clone();
        let group = first.group;
        let mut writer = builder.group(&collection, group)?;
        let mut row = first;
        loop {
            let version = row.version(frontier)?;
            if descriptor.group(&row.key)? != group {
                return Err(wire::refuse("record placement differs"));
            }
            if row.deleted {
                writer.tombstone(&row.key, version)?;
            } else {
                let mut body = (&mut stream).take(row.body_bytes);
                writer.put(&row.key, version, row.body_bytes, &mut body)?;
                if body.limit() != 0 {
                    return Err(wire::refuse("record body was not consumed"));
                }
            }
            actual.include(&row)?;
            pending = stream.next_record()?;
            let Some(next) = pending.as_ref() else {
                break;
            };
            if next.collection != collection || next.group != group {
                break;
            }
            row = pending.take().expect("pending native transfer record");
        }
        writer.finish()?;
    }
    actual.record_sha256 = stream.finish();
    let expected: TransferReceipt = wire::read_metadata(input)?;
    if actual != expected {
        return Err(wire::refuse("complete source record digest differs"));
    }
    let mut trailing = [0];
    if input.read(&mut trailing)? != 0 {
        return Err(wire::refuse("trailing transfer bytes"));
    }
    Ok(actual)
}
