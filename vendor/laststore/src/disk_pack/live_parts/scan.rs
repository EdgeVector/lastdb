use super::{add_delta, resolve_references, Changes, Work};
use crate::disk_pack::dependency::{Coordinate, MAX_DEPENDENCIES};
use crate::disk_pack::{artifact, corrupt, Current, TreeKind, PAGE_BYTES, PART_BYTES};
use crate::Result;
use std::path::Path;

/// Inspect only fresh private metadata parts; never walk the current database.
/// Retired intermediate pages cancel their corresponding private-page additions.
pub(in crate::disk_pack) fn observe_new_parts(
    root: &Path,
    current: &Current,
    coordinates: &[Coordinate],
    changes: &mut Changes,
) -> Result<()> {
    if coordinates.len() > MAX_DEPENDENCIES || coordinates.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(corrupt(
            "new live-part coordinates are not bounded and ordered",
        ));
    }
    let mut work = Work::publication();
    for coordinate in coordinates
        .iter()
        .filter(|coordinate| matches!(coordinate.kind, 1 | 2))
    {
        let path = coordinate.path(root)?;
        let bytes = std::fs::metadata(&path)?.len();
        if bytes == 0 || bytes > PART_BYTES || bytes % PAGE_BYTES as u64 != 0 {
            return Err(corrupt("new live metadata part length"));
        }
        let directory = path.parent().expect("coordinate has a parent");
        let kind = if coordinate.kind == 1 {
            TreeKind::Directory
        } else {
            TreeKind::Locator
        };
        let mut call = artifact::metadata_call(root, current.clone());
        for offset in 0..bytes / PAGE_BYTES as u64 {
            work.charge()?;
            if call.pages.len() >= 24 {
                call = artifact::metadata_call(root, current.clone());
            }
            let number = u64::from(coordinate.part) * 512 + offset;
            let page = call.page(directory, coordinate.object, number)?;
            if page.generation() != current.generation {
                return Err(corrupt("new live metadata part contains an old page"));
            }
            changes.observe_page(kind, coordinate.object, number, &page, current, 1)?;
            resolve_references(root, current, changes)?;
        }
    }
    Ok(())
}

/// Empty locator roots are counted by their directory entries. The directory's
/// own empty root has no incoming entry and therefore needs one explicit count.
pub(in crate::disk_pack) fn add_directory_empty_root(
    root: &Path,
    current: &Current,
    sign: i64,
    changes: &mut Changes,
) -> Result<()> {
    if !matches!(sign, -1 | 1) {
        return Err(corrupt("invalid empty-directory reference delta"));
    }
    if current.directory.height != 1 {
        return Ok(());
    }
    let mut call = artifact::metadata_call(root, current.clone());
    let page = call.tree_page(&current.directory, current.directory.page)?;
    if page.count() == 0 {
        add_delta(
            &mut changes.parts,
            Coordinate {
                kind: 1,
                object: current.directory.object,
                part: u32::try_from(current.directory.page / 512)
                    .map_err(|_| corrupt("empty-directory part exceeds format"))?,
            },
            sign,
        )?;
    }
    Ok(())
}
