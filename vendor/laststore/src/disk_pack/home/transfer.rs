//! Complete opaque native-record transfer between separately owned homes.
//!
//! The source reader owns its format. The target writer emits new files and
//! preserves every stored key, value, deletion and mutation version. This
//! protocol does not claim compatibility with a different logical record codec.

mod input;
mod wire;

use super::{DiskHome, HomeBuilder};
use crate::disk_pack::maintenance::visit_generation;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::Path;
use wire::{Header, Record, DIGEST_BYTES};

/// Complete stream verification. It contains no keys, values, or secret material.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransferReceipt {
    /// Live records whose full body passed the source reader's checksum.
    pub records: u64,
    /// Physical deletion records retained at their original mutation versions.
    pub tombstones: u64,
    /// Complete opaque body bytes, without native file headers or obsolete space.
    pub body_bytes: u64,
    /// Digest of every ordered record identity, version, deletion and body.
    pub record_sha256: [u8; DIGEST_BYTES],
}

/// Export one captured generation. The caller must keep its source quiescent.
/// The stream is for an owner-only pipe, never a log or a plaintext data dump.
pub fn export(home: &DiskHome, output: &mut dyn Write) -> Result<TransferReceipt> {
    let selected = home.read(wire::budget())?;
    let current = selected.current().clone();
    let header = Header::new(home, &current);
    wire::write_metadata(output, &header)?;
    let mut records = wire::DigestWrite::new(output);
    let mut receipt = TransferReceipt::default();
    visit_generation(home.path(), &current, |stored| {
        records.write_all(&[1])?;
        let record = Record::from_stored(&stored);
        wire::write_metadata(&mut records, &record)?;
        if let Some(body) = stored.body {
            let copied = std::io::copy(body, &mut records)?;
            if copied != record.body_bytes {
                return Err(wire::refuse("source body length differs"));
            }
        }
        receipt.include(&record)
    })?;
    receipt.record_sha256 = records.finish();
    // This offline operation must not silently select another source boundary.
    let after = home.read(wire::budget())?;
    if after.current() != &current {
        return Err(wire::refuse("source changed during export"));
    }
    output.write_all(&[0])?;
    wire::write_metadata(
        output,
        &wire::Footer {
            header_sha256: header.digest()?,
            receipt: receipt.clone(),
        },
    )?;
    output.flush()?;
    Ok(receipt)
}

/// Import a complete stream into a nonexistent physical target. An incomplete
/// transfer never publishes CURRENT and cannot serve as an active database.
/// This initial protocol requires a fresh target for a retry; it does not resume.
pub fn import(root: &Path, input: &mut dyn Read) -> Result<TransferReceipt> {
    let header: Header = wire::read_metadata(input)?;
    header.validate()?;
    let descriptor = super::HomeDescriptor::new(header.hash_fanout, header.layout_epoch)?;
    let mut builder = HomeBuilder::create_native_transfer(
        root,
        descriptor.clone(),
        header.frontier,
        header.required_features,
    )?;
    let receipt = input::read_records(
        input,
        &mut builder,
        &descriptor,
        header.frontier,
        header.digest()?,
    )?;
    builder.finish()?;
    let target = DiskHome::open(root)?;
    let verified = export(&target, &mut std::io::sink())?;
    if verified != receipt {
        return Err(wire::refuse("complete target record digest differs"));
    }
    Ok(receipt)
}

impl TransferReceipt {
    fn include(&mut self, record: &Record) -> Result<()> {
        if record.deleted {
            self.tombstones = self
                .tombstones
                .checked_add(1)
                .ok_or_else(|| wire::refuse("deletion count overflow"))?;
        } else {
            self.records = self
                .records
                .checked_add(1)
                .ok_or_else(|| wire::refuse("record count overflow"))?;
        }
        self.body_bytes = self
            .body_bytes
            .checked_add(record.body_bytes)
            .ok_or_else(|| wire::refuse("body byte count overflow"))?;
        Ok(())
    }
}
