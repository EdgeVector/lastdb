//! Explicit target ownership, encrypted checkpoint admission, and final marker.

use super::{
    conversion_copy::{Checkpoint, Phase, CHECKPOINT_COLLECTION, CHECKPOINT_KEY},
    invalid,
    source::budget,
    StorageResult,
};
use crate::storage::disk_pack::{
    map,
    run_admission::{CONVERSION_INTENT, TYPED_NODE_BYTES},
    run_node, NODE_MARKER,
};
use laststore::disk_run::home::RunHome;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;

const IN_PROGRESS: &[u8] = b"format=typed-range-conversion-in-progress-v1\n";
const STAGED: &str = ".typed-range-node-marker-v1";

pub(super) struct Target {
    pub(super) homes: [Arc<RunHome>; 2],
    pub(super) checkpoint: Checkpoint,
    published: bool,
    _owner: File,
}

impl Target {
    pub(super) fn open(
        root: &Path,
        key: &[u8; 32],
        job: &str,
        resume: bool,
    ) -> StorageResult<Self> {
        if !std::fs::symlink_metadata(root)?.is_dir() {
            return Err(invalid("target root is not a real directory"));
        }
        let owner = File::open(root)?;
        owner
            .try_lock()
            .map_err(|_| invalid("another converter owns the target"))?;
        let intent = format!("version=1\njob={job}\n");
        let mut published = false;
        if resume {
            let marker = std::fs::read(root.join(NODE_MARKER))?;
            if marker != IN_PROGRESS && marker != TYPED_NODE_BYTES {
                return Err(invalid("target conversion marker differs"));
            }
            match std::fs::read(root.join(CONVERSION_INTENT)) {
                Ok(bytes) if bytes == intent.as_bytes() => {}
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound
                        && marker == TYPED_NODE_BYTES =>
                {
                    published = true
                }
                _ => return Err(invalid("target belongs to another conversion job")),
            }
        } else {
            if std::fs::read_dir(root)?.next().is_some() {
                return Err(invalid("a new target must be empty"));
            }
            write_new(&root.join(CONVERSION_INTENT), intent.as_bytes())?;
            write_new(&root.join(NODE_MARKER), IN_PROGRESS)?;
            owner.sync_all()?;
        }
        let homes = [
            open_home(&root.join(run_node::RUN_DIR), &run_node::home_key(key))?,
            open_home(&root.join(run_node::CAS_DIR), &run_node::cas_key(key))?,
        ];
        let identities = [homes[0].identity(), homes[1].identity()];
        let row = homes[0]
            .read(budget(8 * 1024 * 1024, 1))
            .get_bounded(CHECKPOINT_COLLECTION, CHECKPOINT_KEY, 1024 * 1024)
            .map_err(map)?;
        let missing = row.is_none();
        if missing && homes.iter().any(|home| home.current_root().frontier() != 0) {
            return Err(invalid("a nonempty target has no encrypted progress"));
        }
        let checkpoint: Checkpoint = match row {
            Some(row) => serde_json::from_slice(&row)
                .map_err(|_| invalid("encrypted conversion progress is invalid"))?,
            None => Checkpoint {
                version: 1,
                job: job.into(),
                homes: identities,
                phase: Phase::Main,
                after: None,
                records: [0; 2],
                logical_bytes: [0; 2],
            },
        };
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
        if checkpoint.phase == Phase::Main
            && checkpoint.after.is_none()
            && checkpoint.records != [0; 2]
        {
            return Err(invalid(
                "initial conversion progress disagrees with its counts",
            ));
        }
        if missing {
            super::conversion_copy::commit(&homes[0], vec![checkpoint.mutation()?])?;
        }
        Ok(Self {
            homes,
            checkpoint,
            published,
            _owner: owner,
        })
    }

    pub(super) fn reopen(self, root: &Path, key: &[u8; 32]) -> StorageResult<Self> {
        let Self {
            homes,
            checkpoint,
            published,
            _owner,
        } = self;
        drop(homes);
        let homes = [
            open_home(&root.join(run_node::RUN_DIR), &run_node::home_key(key))?,
            open_home(&root.join(run_node::CAS_DIR), &run_node::cas_key(key))?,
        ];
        if [homes[0].identity(), homes[1].identity()] != checkpoint.homes {
            return Err(invalid("target identities changed across reopen"));
        }
        let row = homes[0]
            .read(budget(8 * 1024 * 1024, 1))
            .get_bounded(CHECKPOINT_COLLECTION, CHECKPOINT_KEY, 1024 * 1024)
            .map_err(map)?
            .ok_or_else(|| invalid("target lost its encrypted progress"))?;
        if serde_json::from_slice::<serde_json::Value>(&row)
            .map_err(|_| invalid("invalid target progress after reopen"))?
            != serde_json::to_value(&checkpoint).map_err(|_| invalid("invalid target progress"))?
        {
            return Err(invalid("target progress differs after reopen"));
        }
        Ok(Self {
            homes,
            checkpoint,
            published,
            _owner,
        })
    }

    pub(super) fn publish(&self, root: &Path) -> StorageResult<()> {
        if self.checkpoint.phase != Phase::Complete {
            return Err(invalid("target verification is incomplete"));
        }
        if self.published {
            return Ok(());
        }
        let staged = root.join(STAGED);
        if staged.try_exists()? {
            if std::fs::symlink_metadata(&staged)?.is_file()
                && std::fs::read(&staged)? == TYPED_NODE_BYTES
            {
                std::fs::rename(&staged, root.join(NODE_MARKER))?;
            } else {
                return Err(invalid("staged target marker differs"));
            }
        } else {
            write_new(&staged, TYPED_NODE_BYTES)?;
            std::fs::rename(&staged, root.join(NODE_MARKER))?;
        }
        File::open(root)?.sync_all()?;
        std::fs::remove_file(root.join(CONVERSION_INTENT))?;
        File::open(root)?.sync_all()?;
        Ok(())
    }
}

fn open_home(path: &Path, key: &[u8; 32]) -> StorageResult<Arc<RunHome>> {
    let home = if path.try_exists()? {
        RunHome::open(path, key)
    } else {
        RunHome::create(path, key)
    };
    home.map(Arc::new).map_err(map)
}

fn write_new(path: &Path, bytes: &[u8]) -> StorageResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
