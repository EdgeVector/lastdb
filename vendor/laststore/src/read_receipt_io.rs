//! I/O wrappers measure successful userspace bytes and explicit status calls.

use crate::read_receipt::{TemporaryBytes, Timer};
use std::cell::Cell;
use std::fs::{File, Metadata};
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Deref;
use std::path::Path;
use std::sync::{LockResult, Mutex, MutexGuard};

#[derive(Clone, Copy)]
pub(crate) enum ReadClass {
    LegacyReplay,
    JournalReplay,
    Footer,
    Payload,
    FormatProbe,
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
        let class = CLASS.with(Cell::get);
        timer.finish(|receipt, nanos| {
            receipt.read_operations = receipt.read_operations.saturating_add(1);
            receipt.read_nanos = receipt.read_nanos.saturating_add(nanos);
            let count = result.as_ref().copied().unwrap_or(0) as u64;
            let target = match class {
                ReadClass::LegacyReplay => &mut receipt.legacy_replay_bytes,
                ReadClass::JournalReplay => &mut receipt.journal_replay_bytes,
                ReadClass::Footer => &mut receipt.footer_bytes,
                ReadClass::Payload => &mut receipt.payload_bytes,
                ReadClass::FormatProbe => &mut receipt.format_probe_bytes,
            };
            *target = target.saturating_add(count);
        });
        result
    }
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

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> LockResult<MutexGuard<'_, T>> {
    let timer = Timer::start();
    let result = mutex.lock();
    timer.finish(|receipt, nanos| {
        receipt.lock_wait_nanos = receipt.lock_wait_nanos.saturating_add(nanos);
    });
    result
}

pub(crate) fn attach<T>(work: impl FnOnce() -> T) -> impl FnOnce() -> T {
    let context = crate::read_receipt::DiskReadContext::capture();
    let class = CLASS.with(Cell::get);
    move || {
        let _class = class.enter();
        match context {
            Some(context) => context.with(work),
            None => work(),
        }
    }
}

pub(crate) fn exists(path: impl AsRef<Path>) -> bool {
    try_exists(path).unwrap_or(false)
}

pub(crate) fn try_exists(path: impl AsRef<Path>) -> io::Result<bool> {
    status(|| path.as_ref().try_exists())
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

pub(crate) fn plain_copy(bytes: &[u8]) -> crate::Result<Vec<u8>> {
    decode(
        || Ok(bytes.to_vec()),
        |bytes| (bytes.len(), bytes.capacity()),
    )
}

pub(crate) struct ReadBuffer {
    bytes: Vec<u8>,
    _charge: TemporaryBytes,
}

impl Deref for ReadBuffer {
    type Target = Vec<u8>;
    fn deref(&self) -> &Vec<u8> {
        &self.bytes
    }
}

pub(crate) fn read_footer(path: impl AsRef<Path>) -> io::Result<ReadBuffer> {
    let _class = ReadClass::Footer.enter();
    fs::read(path)
}

pub(crate) fn read_replay(path: impl AsRef<Path>) -> io::Result<ReadBuffer> {
    let _class = ReadClass::LegacyReplay.enter();
    fs::read(path)
}

pub(crate) fn charge_if(bytes: usize, enabled: bool) -> TemporaryBytes {
    TemporaryBytes::new(if enabled { bytes } else { 0 })
}

pub(crate) mod fs {
    pub(crate) use std::fs::*;

    pub(crate) fn read(path: impl AsRef<std::path::Path>) -> std::io::Result<super::ReadBuffer> {
        use std::io::Read;
        let mut file = super::open(path)?;
        let length = usize::try_from(file.metadata()?.len()).unwrap_or(0);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::OutOfMemory, error))?;
        let mut charge = super::TemporaryBytes::new(bytes.capacity());
        file.read_to_end(&mut bytes)?;
        charge.resize(bytes.capacity());
        Ok(super::ReadBuffer {
            bytes,
            _charge: charge,
        })
    }

    pub(crate) fn metadata(path: impl AsRef<std::path::Path>) -> std::io::Result<Metadata> {
        super::status(|| std::fs::metadata(path))
    }

    pub(crate) fn symlink_metadata(path: impl AsRef<std::path::Path>) -> std::io::Result<Metadata> {
        super::status(|| std::fs::symlink_metadata(path))
    }

    pub(crate) fn read_dir(
        path: impl AsRef<std::path::Path>,
    ) -> std::io::Result<super::MeasuredReadDir> {
        let timer = super::Timer::start();
        let result = std::fs::read_dir(path);
        timer.finish(|receipt, nanos| {
            receipt.directory_reads = receipt.directory_reads.saturating_add(1);
            receipt.metadata_nanos = receipt.metadata_nanos.saturating_add(nanos);
        });
        result.map(super::MeasuredReadDir)
    }
}

pub(crate) struct MeasuredReadDir(std::fs::ReadDir);

impl Iterator for MeasuredReadDir {
    type Item = io::Result<MeasuredDirEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        let timer = Timer::start();
        let result = self.0.next();
        timer.finish(|receipt, nanos| {
            receipt.directory_entries = receipt
                .directory_entries
                .saturating_add(u64::from(matches!(result, Some(Ok(_)))));
            receipt.metadata_nanos = receipt.metadata_nanos.saturating_add(nanos);
        });
        result.map(|entry| entry.map(MeasuredDirEntry))
    }
}

pub(crate) struct MeasuredDirEntry(std::fs::DirEntry);

impl Deref for MeasuredDirEntry {
    type Target = std::fs::DirEntry;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MeasuredDirEntry {
    pub(crate) fn file_type(&self) -> io::Result<std::fs::FileType> {
        status(|| self.0.file_type())
    }
}
