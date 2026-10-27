//! I/O wrappers measure successful userspace bytes and explicit status calls.

use crate::read_receipt::{TemporaryBytes, Timer};
use std::cell::Cell;
use std::fs::{File, Metadata};
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Deref;
use std::os::unix::fs::FileExt;
use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) enum ReadClass {
    Footer,
    Payload,
}

thread_local! {
    static CLASS: Cell<ReadClass> = const { Cell::new(ReadClass::Payload) };
}

pub(crate) struct ReadClassScope(ReadClass);

impl ReadClass {
    pub(crate) fn enter(self) -> ReadClassScope {
        ReadClassScope(CLASS.with(|class| class.replace(self)))
    }
}

impl Drop for ReadClassScope {
    fn drop(&mut self) {
        CLASS.with(|class| class.set(self.0));
    }
}

pub(crate) struct MeasuredFile(File);

pub(crate) fn open(path: impl AsRef<Path>) -> io::Result<MeasuredFile> {
    let timer = Timer::start();
    let result = File::open(path);
    timer.finish(|receipt, nanos| {
        receipt.file_open_attempts = receipt.file_open_attempts.saturating_add(1);
        receipt.file_opens = receipt.file_opens.saturating_add(u64::from(result.is_ok()));
        receipt.open_nanos = receipt.open_nanos.saturating_add(nanos);
    });
    result.map(MeasuredFile)
}

impl MeasuredFile {
    pub(crate) fn metadata(&self) -> io::Result<Metadata> {
        status(|| self.0.metadata())
    }
}

impl Deref for MeasuredFile {
    type Target = File;
    fn deref(&self) -> &File {
        &self.0
    }
}

impl Read for MeasuredFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let timer = Timer::start();
        let result = self.0.read(bytes);
        record_read(timer, result.as_ref().copied().unwrap_or(0));
        result
    }
}

/// A positional read that fills `bytes` from `offset`, measured as one read
/// operation under the current class. A failed read counts no bytes.
pub(crate) fn read_exact_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<()> {
    let timer = Timer::start();
    let result = file.read_exact_at(bytes, offset);
    record_read(timer, if result.is_ok() { bytes.len() } else { 0 });
    result
}

fn record_read(timer: Timer, count: usize) {
    let class = CLASS.with(Cell::get);
    timer.finish(|receipt, nanos| {
        receipt.read_operations = receipt.read_operations.saturating_add(1);
        receipt.read_nanos = receipt.read_nanos.saturating_add(nanos);
        let target = match class {
            ReadClass::Footer => &mut receipt.footer_bytes,
            ReadClass::Payload => &mut receipt.payload_bytes,
        };
        *target = target.saturating_add(count as u64);
    });
}

impl Seek for MeasuredFile {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.0.seek(from)
    }
}

pub(crate) fn status<T>(work: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    let timer = Timer::start();
    let result = work();
    timer.finish(|receipt, nanos| {
        receipt.metadata_operations = receipt.metadata_operations.saturating_add(1);
        receipt.metadata_nanos = receipt.metadata_nanos.saturating_add(nanos);
    });
    result
}

pub(crate) fn decode<T>(
    work: impl FnOnce() -> crate::Result<T>,
    bytes: impl Fn(&T) -> (usize, usize),
) -> crate::Result<T> {
    let timer = Timer::start();
    let result = work();
    timer.finish(|receipt, nanos| {
        receipt.decode_nanos = receipt.decode_nanos.saturating_add(nanos);
        receipt.decoded_bytes = receipt
            .decoded_bytes
            .saturating_add(result.as_ref().map_or(0, |value| bytes(value).0 as u64));
    });
    let _output_charge = result
        .as_ref()
        .map(|value| TemporaryBytes::new(bytes(value).1));
    result
}
