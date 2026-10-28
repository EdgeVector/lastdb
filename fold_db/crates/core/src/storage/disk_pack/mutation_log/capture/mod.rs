//! One immutable bounded native log range, retained through an exact cloud terminal.
mod absent;
mod model;
mod reader;
mod retirement;
mod terminal;
use super::*;
use cloud_backup_protocol::v2::Begin;
pub use model::{log_path, FrozenLogCapture, FrozenLogFile};
pub use reader::MutationLogReader;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct MutationLogCapture {
    inner: Arc<Inner>,
}
struct Inner {
    runtime: Arc<PackRuntime>,
    directory: PathBuf,
    state: Mutex<FrozenLogCapture>,
    readers: AtomicUsize,
    _admission: super::super::NativeCloudWorkGuard,
}

impl PackRuntime {
    /// The coordinator proves no begin submission under its durable owner lock.
    /// Cancellation must not create an otherwise absent capture.
    pub fn cancel_unsubmitted_mutation_log_capture(
        self: &Arc<Self>,
        begin: &Begin,
    ) -> StorageResult<()> {
        let admission = super::super::NativeCloudWorkGuard::acquire()?;
        let root = self.mutation_log.root.join("captures");
        let directory = root.join(begin.job.commit_id.as_str());
        let Some(state): Option<FrozenLogCapture> = store::read(&directory.join("capture.json"))?
        else {
            return absent::cancel(&root, &begin.job.commit_id);
        };
        state.validate()?;
        if state.begin != *begin
            || state.anchor.main_home != self.mutation_log.main_home
            || state.anchor.cas_home != self.mutation_log.cas_home
        {
            return Err(invalid("mutation cancellation job differs"));
        }
        retirement::prepare(&root, &begin.job.commit_id)?;
        let capture = MutationLogCapture {
            inner: Arc::new(Inner {
                runtime: Arc::clone(self),
                directory,
                state: Mutex::new(state),
                readers: AtomicUsize::new(0),
                _admission: admission,
            }),
        };
        capture.cancel_unsubmitted()?;
        store::write(&root.join("active.json"), &begin.job.commit_id)?;
        retirement::finish(&root, &begin.job.commit_id)
    }

    pub fn prepare_mutation_log_capture(
        self: &Arc<Self>,
        begin: &Begin,
        from: &LogPosition,
    ) -> StorageResult<MutationLogCapture> {
        let admission = super::super::NativeCloudWorkGuard::acquire()?;
        let _owner = self.mutation_log.lock()?;
        let root = self.mutation_log.root.join("captures");
        if !root.try_exists()? {
            store::create(&root)?;
        }
        retirement::prepare(&root, &begin.job.commit_id)?;
        let directory = root.join(begin.job.commit_id.as_str());
        let mut state: FrozenLogCapture =
            if let Some(state) = store::read(&directory.join("capture.json"))? {
                state
            } else {
                if !directory.try_exists()? {
                    store::create(&directory)?;
                }
                let state = plan(self, begin, from)?;
                store::write(&directory.join("capture.json"), &state)?;
                state
            };
        state.validate()?;
        if state.begin != *begin
            || state.from != *from
            || state.anchor.main_home != self.mutation_log.main_home
            || state.anchor.cas_home != self.mutation_log.cas_home
        {
            return Err(invalid("mutation capture frozen identity changed"));
        }
        if !state.ready && state.terminal.is_none() && !state.local_cancelled {
            for file in &state.files {
                retain(self, &directory, &state.anchor.epoch, file)?;
            }
            File::open(&directory)?.sync_all()?;
            state.ready = true;
            store::write(&directory.join("capture.json"), &state)?;
        }
        store::write(&root.join("active.json"), &begin.job.commit_id)?;
        retirement::finish(&root, &begin.job.commit_id)?;
        Ok(MutationLogCapture {
            inner: Arc::new(Inner {
                runtime: Arc::clone(self),
                directory,
                state: Mutex::new(state),
                readers: AtomicUsize::new(0),
                _admission: admission,
            }),
        })
    }

    pub fn resume_mutation_log_capture(
        self: &Arc<Self>,
        request: &Id,
    ) -> StorageResult<MutationLogCapture> {
        let root = self.mutation_log.root.join("captures");
        let state: FrozenLogCapture =
            store::read(&root.join(request.as_str()).join("capture.json"))?
                .ok_or_else(|| invalid("saved mutation capture is absent"))?;
        self.prepare_mutation_log_capture(&state.begin, &state.from)
    }
}

fn plan(
    runtime: &PackRuntime,
    begin: &Begin,
    from: &LogPosition,
) -> StorageResult<FrozenLogCapture> {
    let state = runtime.mutation_log.state(&from.epoch)?;
    if state.gap.is_some() || *from != state.released || state.head.sequence <= from.sequence {
        return Err(invalid(
            "mutation capture needs a complete nonempty unacknowledged prefix",
        ));
    }
    let mut position = from.clone();
    let mut entries = Vec::new();
    let mut files = Vec::new();
    let mut bytes = 0u64;
    let end = state
        .head
        .sequence
        .min(from.sequence.saturating_add(model::MAX_BATCH_ENTRIES));
    for sequence in from.sequence + 1..=end {
        let entry: LogEntry = store::read(
            &store::entry(&runtime.mutation_log.root, &from.epoch, sequence)?.join("entry.json"),
        )?
        .ok_or_else(|| invalid("mutation capture complete entry is absent"))?;
        position = entry.position_after(&position)?;
        for part in &entry.parts {
            files.push(FrozenLogFile {
                entry_sequence: entry.sequence,
                role: part.role,
                run_id: part.run_id.clone(),
                path: cloud_backup_protocol::RelativePath::try_from(model::log_path(
                    &from.epoch,
                    sequence,
                    part.role,
                    &part.run_id,
                ))
                .map_err(|_| invalid("mutation capture native path"))?,
                bytes: part.bytes,
            });
            bytes = bytes
                .checked_add(part.bytes)
                .ok_or_else(|| invalid("mutation capture size overflow"))?;
        }
        entries.push(entry);
    }
    let capture = FrozenLogCapture {
        version: 1,
        begin: begin.clone(),
        anchor: state.anchor,
        from: from.clone(),
        through: position,
        entries,
        files,
        native_bytes: bytes,
        ready: false,
        commit: None,
        terminal: None,
        local_cancelled: false,
        released: false,
    };
    capture.validate()?;
    Ok(capture)
}

fn retain(
    runtime: &PackRuntime,
    directory: &std::path::Path,
    epoch: &Id,
    file: &FrozenLogFile,
) -> StorageResult<()> {
    let source = store::entry(&runtime.mutation_log.root, epoch, file.entry_sequence)?
        .join(file.role.directory())
        .join(format!("{}.ldrun", file.run_id.as_str()));
    store::directory(
        source
            .parent()
            .ok_or_else(|| invalid("mutation log source parent"))?,
    )?;
    let original = input(&source, file.bytes)?;
    let before = original.metadata()?;
    let target = directory.join(format!("{}.ldrun", file.run_id.as_str()));
    if !target.try_exists()? {
        fs::hard_link(&source, &target)?;
    }
    let retained = input(&target, file.bytes)?.metadata()?;
    if (before.dev(), before.ino()) != (retained.dev(), retained.ino()) {
        return Err(invalid("mutation capture original inode changed"));
    }
    Ok(())
}

fn input(path: &std::path::Path, bytes: u64) -> StorageResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.len() != bytes
    {
        return Err(invalid("mutation capture private original file"));
    }
    Ok(file)
}

impl MutationLogCapture {
    pub fn manifest(&self) -> StorageResult<FrozenLogCapture> {
        Ok(self
            .inner
            .state
            .lock()
            .map_err(|_| invalid("mutation capture state owner"))?
            .clone())
    }
    pub fn retained_source_bytes(&self) -> StorageResult<u64> {
        let state = self.manifest()?;
        if state.released {
            return Ok(0);
        }
        state.files.iter().try_fold(0u64, |bytes, file| {
            let path = self
                .inner
                .directory
                .join(format!("{}.ldrun", file.run_id.as_str()));
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    input(&path, file.bytes)?;
                    bytes
                        .checked_add(file.bytes)
                        .ok_or_else(|| invalid("mutation capture retained size"))
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound && state.terminal.is_some() =>
                {
                    Ok(bytes)
                }
                Err(error) => Err(error.into()),
            }
        })
    }
}
