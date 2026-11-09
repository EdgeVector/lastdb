//! Copy an already verified immutable root into a private replay workspace.
use super::{files, invalid, map, RunSnapshotReader, StorageResult};
use crate::storage::disk_backup::mutation_log::{LogAnchor, LogPosition};
use crate::storage::disk_pack::RUN_DIR;
use laststore::disk_run::home::RUNS;
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

impl RunSnapshotReader {
    /// Seed a fresh stage from the exact verified predecessor. Runtime locks
    /// and the empty local CAS home are created separately by the caller.
    pub fn seed_replay(
        &self,
        destination: &Path,
        anchor: &LogAnchor,
        position: &LogPosition,
    ) -> StorageResult<()> {
        let native = &self.0;
        if native.profile.main.home_id != anchor.main_home
            || native.profile.main.frontier.get() != position.frontiers.main
            || position.epoch != anchor.epoch
            || native.profile.cas.is_some()
        {
            return Err(invalid("cached mutation replay predecessor"));
        }
        native.check_binding()?;
        files::directory(destination)?;
        let bytes = native.files.iter().try_fold(0u64, |sum, file| {
            sum.checked_add(file.bytes)
                .ok_or_else(|| invalid("cached mutation replay bytes"))
        })?;
        let _space = laststore::space::reserve_space(
            destination,
            &laststore::space::SpacePolicy::default(),
            bytes,
        )
        .map_err(map)?;
        let main = destination.join(RUN_DIR);
        let runs = main.join(RUNS);
        for directory in [&main, &runs] {
            DirBuilder::new().mode(0o700).create(directory)?;
        }
        for file in &native.files {
            copy(
                &native.root.join(&file.path),
                &destination.join(&file.path),
                file.bytes,
            )?;
        }
        for directory in [runs.as_path(), main.as_path(), destination] {
            File::open(directory)?.sync_all()?;
        }
        native.check_binding()
    }
}

fn copy(source: &Path, destination: &Path, bytes: u64) -> StorageResult<()> {
    let mut input = files::regular(source, false)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(destination)?;
    let before = input.metadata()?;
    let copied = io::copy(&mut (&mut input).take(bytes.saturating_add(1)), &mut output)?;
    files::same_entry(&input, source)?;
    let after = input.metadata()?;
    if copied != bytes || before.len() != bytes || after.len() != bytes {
        return Err(invalid("cached mutation replay file length"));
    }
    output.sync_all()?;
    Ok(())
}
