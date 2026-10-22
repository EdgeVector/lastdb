//! One native commit preserves every ordering edge before its final full sync.
//!
//! Apple's fcntl(2) specifies that F_BARRIERFSYNC orders previously fsynced
//! writes on the same device before later I/O. F_FULLFSYNC then makes all those
//! prior writes durable. This changes no disk format, file, or read route.

use crate::{Error, Result};
use std::cell::RefCell;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

thread_local! {
    static ACTIVE: RefCell<Option<State>> = const { RefCell::new(None) };
}

const MAX_FILES: usize = 32;

struct Pending {
    inode: u64,
    file: File,
}

struct State {
    device: u64,
    files: Vec<Pending>,
}

struct Scope;

impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.with(|active| *active.borrow_mut() = None);
    }
}

pub(super) fn complete<T>(root: &Path, commit: impl FnOnce() -> Result<T>) -> Result<T> {
    let file = File::open(root)?;
    let device = file.metadata()?.dev();
    if ACTIVE.with(|active| active.borrow().is_some()) {
        return Err(Error::Config("nested native durability scope".into()));
    }
    ACTIVE.with(|active| {
        *active.borrow_mut() = Some(State {
            device,
            files: Vec::new(),
        });
    });
    let scope = Scope;
    let result = commit()?;
    flush_pending()?;
    // Do not use the scoped helper here: success requires the full device drain.
    super::timed(&super::DATA_SYNCS, &super::DATA_SYNC_MICROS, || {
        super::full_barrier(&file)
    })?;
    drop(scope);
    Ok(result)
}

pub(super) fn sync_if_active(file: &File) -> Result<bool> {
    ACTIVE.with(|active| {
        let mut active = active.borrow_mut();
        let Some(state) = active.as_mut() else {
            return Ok(false);
        };
        let metadata = file.metadata()?;
        if metadata.dev() != state.device {
            return Ok(false);
        }
        if !state
            .files
            .iter()
            .any(|pending| pending.inode == metadata.ino())
        {
            state.files.push(Pending {
                inode: metadata.ino(),
                file: file.try_clone()?,
            });
        }
        if state.files.len() >= MAX_FILES {
            let files = std::mem::take(&mut state.files);
            drop(active);
            flush(files)?;
        }
        Ok(true)
    })
}

pub(super) fn publication_if_active(file: &File) -> Result<bool> {
    let device = ACTIVE.with(|active| active.borrow().as_ref().map(|state| state.device));
    let Some(device) = device else {
        return Ok(false);
    };
    // A device barrier cannot cover another mount. Such a file keeps its full sync.
    if file.metadata()?.dev() != device {
        return Ok(false);
    }
    flush_pending()?;
    super::timed(&super::ORDERED_SYNCS, &super::ORDERED_SYNC_MICROS, || {
        nix::fcntl::fcntl(file, nix::fcntl::FcntlArg::F_BARRIERFSYNC)
            .map_err(std::io::Error::from)?;
        Ok(())
    })?;
    Ok(true)
}

fn flush_pending() -> Result<()> {
    let files = ACTIVE.with(|active| {
        active
            .borrow_mut()
            .as_mut()
            .map(|state| std::mem::take(&mut state.files))
    });
    flush(files.unwrap_or_default())
}

fn flush(files: Vec<Pending>) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }
    super::timed(&super::FILE_SET_SYNCS, &super::FILE_SET_SYNC_MICROS, || {
        flush_files(files)
    })
}

fn flush_files(files: Vec<Pending>) -> Result<()> {
    if files.len() < 2 {
        for pending in files {
            rustix::fs::fsync(&pending.file).map_err(std::io::Error::from)?;
        }
        return Ok(());
    }
    let workers = files.len().min(4);
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let files = &files;
            handles.push(
                std::thread::Builder::new()
                    .name("native-file-sync".into())
                    // These workers only issue fsync. Do not inherit a daemon's
                    // potentially large RUST_MIN_STACK for recursive queries.
                    .stack_size(256 * 1024)
                    .spawn_scoped(scope, move || {
                        for pending in files.iter().skip(worker).step_by(workers) {
                            rustix::fs::fsync(&pending.file).map_err(std::io::Error::from)?;
                        }
                        Ok::<(), Error>(())
                    })?,
            );
        }
        let mut result = Ok(());
        for handle in handles {
            let synced = handle
                .join()
                .unwrap_or_else(|_| Err(Error::Config("native file-sync worker panicked".into())));
            if result.is_ok() {
                result = synced;
            }
        }
        result
    })
}
