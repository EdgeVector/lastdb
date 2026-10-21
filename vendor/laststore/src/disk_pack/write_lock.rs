use super::{artifact, Current};
use crate::Result;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// A typed proof of exclusive artifact write ownership. No caller can forge it.
pub(super) struct WriteLock {
    root: PathBuf,
    _file: File,
    checked_current: Mutex<Option<[u8; 32]>>,
    /// Retained generations whose parts this lease already verified, with the
    /// digest of each root. Reclaim reads such a receipt without opening every part again.
    verified: Mutex<BTreeMap<u64, [u8; 32]>>,
    pub(super) read_state: Mutex<super::lifetime::WriterReadState>,
}

impl WriteLock {
    pub(super) fn acquire(root: &Path) -> Result<Self> {
        Self::acquire_inner(root, None)
    }

    pub(super) fn acquire_conversion(root: &Path, token: uuid::Uuid) -> Result<Self> {
        Self::acquire_inner(root, Some(token))
    }

    fn acquire_inner(root: &Path, conversion: Option<uuid::Uuid>) -> Result<Self> {
        let root = std::fs::canonicalize(root)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("journal").join("LOCK"))?;
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(std::io::Error::from)?;
        if root.join(super::conversion::REVERSE_FENCE).try_exists()? {
            return Err(crate::Error::Config(
                "native source is sealed after reverse conversion".into(),
            ));
        }
        if let Some(token) = conversion {
            super::conversion_fence::validate(&root, token)?;
        } else {
            super::conversion_fence::refuse_pending(&root)?;
        }
        sweep_unfinished_publications(&root);
        let current = artifact::current(&root)?;
        if current.required_features & super::FEATURE_OBJECT_METADATA == 0 {
            return Err(crate::Error::Config(
                "disk-pack writes require complete object bounds".into(),
            ));
        }
        Ok(Self {
            root,
            _file: file,
            checked_current: Mutex::new(None),
            verified: Mutex::new(BTreeMap::new()),
            read_state: Mutex::new(super::lifetime::WriterReadState::new(current.generation)),
        })
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// This lease retains only a complete-root digest, never pages or part metadata.
    pub(super) fn checked(&self, current: &Current) -> Result<bool> {
        let checked = self
            .checked_current
            .lock()
            .map_err(|_| super::corrupt("verified writer root lock is poisoned"))?;
        Ok(*checked == Some(super::bytes::digest(&current.encode()?)))
    }

    /// True when this lease verified this exact root, or published it.
    pub(super) fn verified(&self, current: &Current) -> Result<bool> {
        if self.checked(current)? {
            return Ok(true);
        }
        let verified = self
            .verified
            .lock()
            .map_err(|_| super::corrupt("verified generation lock is poisoned"))?;
        Ok(verified.get(&current.generation) == Some(&super::bytes::digest(&current.encode()?)))
    }

    /// Call only after complete dependency verification of this root.
    pub(super) fn remember_verified(&self, current: &Current) -> Result<()> {
        let mut verified = self
            .verified
            .lock()
            .map_err(|_| super::corrupt("verified generation lock is poisoned"))?;
        verified.insert(current.generation, super::bytes::digest(&current.encode()?));
        Ok(())
    }

    /// Forget every verified root that reclaim no longer retains.
    pub(super) fn keep_verified(&self, keep: &std::collections::BTreeSet<u64>) -> Result<()> {
        let mut verified = self
            .verified
            .lock()
            .map_err(|_| super::corrupt("verified generation lock is poisoned"))?;
        verified.retain(|generation, _| keep.contains(generation));
        Ok(())
    }

    /// Call only after complete dependency verification or successful root barriers.
    pub(super) fn remember_checked(&self, current: &Current) -> Result<()> {
        let mut checked = self
            .checked_current
            .lock()
            .map_err(|_| super::corrupt("verified writer root lock is poisoned"))?;
        *checked = Some(super::bytes::digest(&current.encode()?));
        Ok(())
    }
}

/// Temporary files a publication writes in the root before it renames them.
const PUBLICATION_TEMPORARIES: [&str; 2] = ["CURRENT.pending-", "TAIL.pending-"];

/// Remove the temporary files that a writer killed inside a publication left.
///
/// A publication writes `CURRENT` or `JOURNAL_TAIL` to a `.pending-<uuid>` file
/// and renames it over the target. Only the holder of this lock publishes, so
/// when the lock is held every such file is the leftover of a dead writer. No
/// recovery state names them, and each kill inside a publication would leave
/// one more. The sweep is best effort: a file that stays is only waste.
fn sweep_unfinished_publications(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let leftover = name.to_str().is_some_and(|name| {
            PUBLICATION_TEMPORARIES.iter().any(|prefix| {
                name.strip_prefix(prefix)
                    .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
            })
        });
        if leftover && entry.file_type().is_ok_and(|kind| kind.is_file()) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
