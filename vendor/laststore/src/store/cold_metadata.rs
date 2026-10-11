//! Bounded physical metadata preflight before legacy cold-group allocation.

use super::*;
use std::io::{Read, Seek, SeekFrom};

pub(super) fn opening_bound(directory: &Path) -> Result<u64> {
    if !directory.exists() {
        return Ok(0);
    }
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "seg") {
            if let Some(sequence) = path
                .file_stem()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<u64>().ok())
            {
                segments.push((sequence, path));
            }
        }
    }
    segments.sort_unstable_by_key(|(sequence, _)| std::cmp::Reverse(*sequence));
    let mut bound = 0u64;
    let mut sorted = false;
    for (_, path) in segments {
        let (complete, bytes) =
            if let Some((complete, bytes)) = crate::sorted_metadata::opening_bound(&path)? {
                sorted = true;
                (complete, bytes)
            } else {
                (false, legacy_file_bound(&path)?)
            };
        bound = add(bound, bytes)?;
        if complete {
            break;
        }
    }
    if !sorted {
        for name in ["chunks", "tail"] {
            bound = add(bound, directory_bound(&directory.join(name), 0)?)?;
        }
    }
    add(bound, directory_bound(&directory.join("sorted-tail"), 1)?)
}

fn directory_bound(directory: &Path, depth: u8) -> Result<u64> {
    if !directory.exists() {
        return Ok(0);
    }
    let mut bound = 0;
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            return Err(Error::Config("metadata group contains a symlink".into()));
        }
        if meta.is_dir() && depth > 0 {
            bound = add(bound, directory_bound(&path, depth - 1)?)?;
        } else if meta.is_file() && path.extension().is_some_and(|extension| extension == "seg") {
            bound = add(bound, legacy_file_bound(&path)?)?;
        }
    }
    Ok(bound)
}

fn legacy_file_bound(path: &Path) -> Result<u64> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_file() {
        return Err(Error::Config(
            "metadata segment is not a regular file".into(),
        ));
    }
    let mut file = File::open(path)?;
    let mut decoded = meta.len();
    let mut offset = 0u64;
    while offset
        .checked_add(frame::header_size() as u64)
        .is_some_and(|end| end <= meta.len())
    {
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0; 48];
        file.read_exact(&mut header)?;
        if !header.starts_with(&frame::MAGIC) {
            break;
        }
        let bytes = frame::encoded_len(&header)? as u64;
        if bytes == 0 {
            return Err(Error::Corrupt("metadata frame is empty".into()));
        }
        if header[5] & 1 != 0 {
            decoded = add(decoded, frame::MAX_DECOMPRESSED_FRAME_BYTES as u64)?;
        }
        offset = add(offset, bytes)?;
    }
    decoded
        .checked_mul(16)
        .ok_or_else(|| Error::Config("metadata cold allocation overflows".into()))
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| Error::Config("metadata allocation size overflows".into()))
}
