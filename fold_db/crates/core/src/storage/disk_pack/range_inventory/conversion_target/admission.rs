//! Authenticate existing progress before any native writer can clean up files.

use super::{budget, invalid, map, run_node, Checkpoint, Path, Phase, StorageResult};
use super::{CHECKPOINT_COLLECTION, CHECKPOINT_KEY};
use laststore::disk_run::home::RunRead;

pub(super) fn check(root: &Path, key: &[u8; 32], job: &str, published: bool) -> StorageResult<()> {
    let mut main = open(&root.join(run_node::RUN_DIR), &run_node::home_key(key))?;
    let cas = open(&root.join(run_node::CAS_DIR), &run_node::cas_key(key))?;
    if published && (main.is_none() || cas.is_none()) {
        return Err(invalid("published target lacks an existing native home"));
    }
    let row = main
        .as_mut()
        .map(|read| read.get_bounded(CHECKPOINT_COLLECTION, CHECKPOINT_KEY, 1024 * 1024))
        .transpose()
        .map_err(map)?
        .flatten();
    let Some(row) = row else {
        if published
            || main
                .as_ref()
                .into_iter()
                .chain(cas.as_ref())
                .any(|read| read.frontier() != 0)
        {
            return Err(invalid("an existing target has no encrypted progress"));
        }
        return Ok(());
    };
    let checkpoint: Checkpoint = serde_json::from_slice(&row)
        .map_err(|_| invalid("encrypted conversion progress is invalid"))?;
    let (Some(main), Some(cas)) = (main.as_ref(), cas.as_ref()) else {
        return Err(invalid("encrypted progress lacks both native homes"));
    };
    validate(&checkpoint, job, [main.home_id(), cas.home_id()], published)
}

pub(super) fn validate(
    checkpoint: &Checkpoint,
    job: &str,
    identities: [uuid::Uuid; 2],
    published: bool,
) -> StorageResult<()> {
    if checkpoint.version != 1 || checkpoint.job != job || checkpoint.homes != identities {
        return Err(invalid("encrypted progress does not match its target"));
    }
    if published && checkpoint.phase != Phase::Complete {
        return Err(invalid(
            "published target lacks complete encrypted progress",
        ));
    }
    if checkpoint.phase != Phase::Main
        && checkpoint.after.is_some()
        && checkpoint.phase != Phase::Cas
    {
        return Err(invalid("completed progress retains a copy cursor"));
    }
    if checkpoint.phase == Phase::Main && checkpoint.after.is_none() && checkpoint.records != [0; 2]
    {
        return Err(invalid(
            "initial conversion progress disagrees with its counts",
        ));
    }
    Ok(())
}

fn open(path: &Path, key: &[u8; 32]) -> StorageResult<Option<RunRead>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            RunRead::open_existing(path, key, budget(8 * 1024 * 1024, 1))
                .map(Some)
                .map_err(map)
        }
        Ok(_) => Err(invalid("target native home must be a real directory")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
