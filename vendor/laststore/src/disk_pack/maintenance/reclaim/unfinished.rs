//! Generation directories that a killed writer left, and the errors for the
//! ones reclaim cannot judge.

use crate::disk_pack::bytes::{u64_at, verify_checksum};
use crate::disk_pack::PAGE_BYTES;
use crate::{durability, Result};
use std::fs::File;
use std::path::Path;

/// Remove a generation directory whose receipt its writer never finished.
///
/// A writer creates `generations/<n>` and then appends the receipt pages, one
/// write for each page, and syncs the part files last. A publication names a
/// generation only after that. A kill in between leaves a directory with no
/// part file, an empty part file, a torn first page, or fewer pages than the
/// first page declares. None of them was ever published, so none holds data.
/// The caller passes only generations that CURRENT and the references do not
/// name. A directory with any other content, a gap in its part numbers, or a
/// first page that does not verify is left in place, and reclaim stops with an
/// error that names the directory and the reason. A complete receipt is read as
/// a published one.
pub(super) fn remove_unfinished(
    path: &Path,
    epoch: &crate::disk_pack::private_files::DeletionEpoch,
) -> Result<bool> {
    let mut parts = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let index = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("part-"))
            .filter(|hex| hex.len() == 8)
            .and_then(|hex| u32::from_str_radix(hex, 16).ok());
        let (Some(index), true) = (index, entry.file_type()?.is_file()) else {
            return Err(unreadable(
                path,
                &format!(
                    "holds `{}`, which is not a receipt part file; reclaim leaves it for an operator",
                    entry.file_name().to_string_lossy()
                ),
            ));
        };
        parts.push((index, entry.metadata()?.len()));
    }
    parts.sort_unstable();
    if parts
        .iter()
        .map(|(index, _)| *index)
        .ne(0..parts.len() as u32)
    {
        return Err(unreadable(
            path,
            "its receipt part files have a gap in their numbers; reclaim leaves it for an operator",
        ));
    }
    let page = PAGE_BYTES as u64;
    let present: u64 = parts.iter().map(|(_, length)| length / page).sum();
    if present > 0 {
        match declared_pages(&path.join("part-00000000"))? {
            Some(pages) if pages > present => {}
            Some(_) => return Ok(false),
            None => {
                return Err(unreadable(
                    path,
                    "its first receipt page does not verify; reclaim leaves it for an operator",
                ))
            }
        }
    }
    let Some(_deletion) = epoch.delete(path, true)? else {
        return Ok(false);
    };
    std::fs::remove_dir_all(path)?;
    durability::sync_directory(&File::open(path.parent().expect("generation parent"))?)?;
    Ok(true)
}

/// The error for a generation directory that reclaim cannot judge: it names the
/// directory and the reason.
fn unreadable(path: &Path, reason: &str) -> crate::Error {
    crate::Error::Corrupt(format!(
        "disk-pack v1: generation directory {}: {reason}",
        path.display()
    ))
}

/// Name the generation and its receipt directory in the error of a receipt read,
/// and keep the kind of the error.
pub(super) fn at_generation<T>(result: Result<T>, root: &Path, number: u64) -> Result<T> {
    result.map_err(|error| {
        let place = root.join("generations").join(number.to_string());
        match error {
            crate::Error::Io(io) => crate::Error::Io(std::io::Error::new(
                io.kind(),
                format!("generation receipt {}: {io}", place.display()),
            )),
            crate::Error::Corrupt(reason) => {
                crate::Error::Corrupt(format!("generation receipt {}: {reason}", place.display()))
            }
            other => other,
        }
    })
}

/// The page count a receipt declares in its first page, when that page verifies.
fn declared_pages(first_part: &Path) -> Result<Option<u64>> {
    use std::io::Read;
    let mut bytes = vec![0; PAGE_BYTES];
    File::open(first_part)?.read_exact(&mut bytes)?;
    if &bytes[..8] != b"LDPAGE1\0" || verify_checksum(&bytes, 80).is_err() {
        return Ok(None);
    }
    Ok(Some(u64_at(&bytes, 72)))
}
