//! Repair only the exact job's bounded marker prefixes and owned staging dirs.

use super::{
    invalid, run_node, write_new, File, OpenOptions, OpenOptionsExt, Path, StorageResult, Write,
    CONVERSION_INTENT, NODE_MARKER, TYPED_NODE_BYTES,
};

const IN_PROGRESS: &[u8] = b"format=typed-range-conversion-in-progress-v1\n";
const STAGED: &str = ".typed-range-node-marker-v1";

pub(super) fn admit(root: &Path, job: &str, resume: bool) -> StorageResult<bool> {
    let expected = format!("version=1\njob={job}\n");
    let empty = std::fs::read_dir(root)?.next().is_none();
    if empty {
        write_new(&root.join(CONVERSION_INTENT), expected.as_bytes())?;
        write_new(&root.join(NODE_MARKER), IN_PROGRESS)?;
    } else {
        if !resume {
            return Err(invalid("a new target must be empty"));
        }
        let intent = read(&root.join(CONVERSION_INTENT), 256)?;
        let marker = read(&root.join(NODE_MARKER), 128)?;
        if intent.is_none() && marker.as_deref() == Some(TYPED_NODE_BYTES) {
            return Ok(true);
        }
        let bytes = intent.ok_or_else(|| invalid("target conversion intent is absent"))?;
        if bytes != expected.as_bytes() {
            if !expected.as_bytes().starts_with(&bytes) || !only_initial_markers(root)? {
                return Err(invalid("target belongs to another conversion job"));
            }
            finish(&root.join(CONVERSION_INTENT), expected.as_bytes())?;
        }
        match marker.as_deref() {
            Some(bytes) if bytes == IN_PROGRESS || bytes == TYPED_NODE_BYTES => {}
            Some(bytes) if IN_PROGRESS.starts_with(bytes) && only_initial_markers(root)? => {
                finish(&root.join(NODE_MARKER), IN_PROGRESS)?;
            }
            None if only_initial_markers(root)? => write_new(&root.join(NODE_MARKER), IN_PROGRESS)?,
            _ => return Err(invalid("target conversion marker differs")),
        }
    }
    File::open(root)?.sync_all()?;
    clear_staging(root)?;
    Ok(false)
}

pub(super) fn publish_marker(root: &Path) -> StorageResult<()> {
    let staged = root.join(STAGED);
    match read(&staged, 128)? {
        Some(bytes) if TYPED_NODE_BYTES.starts_with(&bytes) => finish(&staged, TYPED_NODE_BYTES)?,
        Some(_) => return Err(invalid("staged target marker differs")),
        None => write_new(&staged, TYPED_NODE_BYTES)?,
    }
    std::fs::rename(staged, root.join(NODE_MARKER))?;
    File::open(root)?.sync_all()?;
    Ok(())
}

fn read(path: &Path, maximum: u64) -> StorageResult<Option<Vec<u8>>> {
    match super::super::binding::open_file(path) {
        Ok(mut file) => super::super::binding::read(&mut file, maximum).map(Some),
        Err(crate::storage::StorageError::IoError(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn finish(path: &Path, bytes: &[u8]) -> StorageResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("conversion marker is not a regular file"));
    }
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn only_initial_markers(root: &Path) -> StorageResult<bool> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file()
            || ![CONVERSION_INTENT, NODE_MARKER]
                .iter()
                .any(|name| entry.file_name() == *name)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn clear_staging(root: &Path) -> StorageResult<()> {
    let mut count = 0;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        count += 1;
        if count > 128 {
            return Err(invalid("target root entry allowance exceeded"));
        }
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| invalid("invalid target entry name"))?;
        if [
            CONVERSION_INTENT,
            NODE_MARKER,
            run_node::RUN_DIR,
            run_node::CAS_DIR,
            STAGED,
        ]
        .contains(&name)
        {
            continue;
        }
        let staging = [run_node::RUN_DIR, run_node::CAS_DIR].iter().any(|home| {
            name.strip_prefix(&format!(".{home}.create-"))
                .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
        });
        if !staging || !entry.file_type()?.is_dir() {
            return Err(invalid("target contains an unknown entry"));
        }
        // RunHome documents these uninstalled siblings as discardable. The
        // caller holds this job's target directory lock and checked its intent.
        std::fs::remove_dir_all(entry.path())?;
    }
    File::open(root)?.sync_all()?;
    Ok(())
}
