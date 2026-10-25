//! Modern bounded record export, separate from the portable transfer wire codec.
use super::transfer::{self, TransferHeader, TransferReceipt};
use super::DiskHome;
use crate::disk_pack::{Current, ReadBudget};
use crate::Result;
use sha2::{Digest, Sha256};
use std::io::{self, Read, Write};
use std::path::Path;

/// Export all stored records under the central backup memory allowance.
/// The caller keeps the source quiescent; no logical record-count cap applies.
pub fn export(home: &DiskHome, output: &mut dyn Write) -> Result<TransferReceipt> {
    let current = selected_current(home)?;
    let header = TransferHeader::new(home, &current);
    transfer::write_header(output, &header)?;
    let mut records = DigestWrite {
        output,
        digest: Sha256::new(),
    };
    let mut receipt = TransferReceipt::default();
    crate::disk_pack::maintenance::visit_generation_streaming(home.path(), &current, |stored| {
        transfer::write_stored_record_prefix(&mut records, &stored, current.required_features)?;
        let deleted = stored.body.is_none();
        if let Some(body) = stored.body {
            if std::io::copy(body, &mut records)? != stored.body_bytes {
                return Err(refuse("source body length differs"));
            }
        }
        receipt.include_body(deleted, stored.body_bytes)
    })?;
    receipt.record_sha256 = records.finish();
    if selected_current(home)? != current {
        return Err(refuse("source changed during export"));
    }
    transfer::write_footer(output, &header, &receipt)?;
    output.flush()?;
    Ok(receipt)
}

/// Import and verify all stored records under the central backup memory policy.
/// This retains the existing transfer bytes and accepts no partial target.
pub fn import(root: &Path, input: &mut dyn Read) -> Result<TransferReceipt> {
    transfer::import_with_verifier(root, input, export)
}

fn selected_current(home: &DiskHome) -> Result<Current> {
    let selected = home.read(ReadBudget {
        pages: lastdb_storage_policy::backup_decode::NATIVE_STREAM_PAGES,
        blocks: lastdb_storage_policy::backup_decode::NATIVE_STREAM_BLOCKS,
        bytes: lastdb_storage_policy::BACKUP_RESOURCES.decoded_values_bytes,
        keys: 1,
    })?;
    Ok(selected.current().clone())
}

fn refuse(message: &'static str) -> crate::Error {
    crate::Error::Config(format!("native record transfer: {message}"))
}

struct DigestWrite<'a> {
    output: &'a mut dyn Write,
    digest: Sha256,
}
impl DigestWrite<'_> {
    fn finish(self) -> [u8; 32] {
        self.digest.finalize().into()
    }
}
impl Write for DigestWrite<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.output.write(bytes)?;
        self.digest.update(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}
