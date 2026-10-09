use super::*;

/// Return candidate byte-cache paths for one authenticated restore address.
///
/// This reads only the layout descriptor, never opens a store or mutates files.
/// The caller must verify the candidate bytes against the manifest digest.
/// Legacy frame chunks and numbered plain/sorted units are supported. A sorted
/// multipart piece can miss this cache and must use the ordinary cloud path.
pub fn restore_chunk_cache_paths(
    root: &Path,
    collection: &str,
    shard: u16,
    group: Option<u32>,
    chunk_uuid: Uuid,
) -> Result<Vec<PathBuf>> {
    let mut components = Path::new(collection).components();
    if !matches!(components.next(), Some(std::path::Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(Error::Config("invalid restore cache collection".into()));
    }
    let Some(layout) = describe_home(root)? else {
        return Ok(Vec::new());
    };
    if u32::from(shard) >= 1u32.checked_shl(u32::from(layout.shard_bits)).unwrap_or(0)
        || group.is_some_and(|value| {
            value
                >= 1u32
                    .checked_shl(u32::from(layout.hash_group_bits))
                    .unwrap_or(0)
        })
    {
        return Ok(Vec::new());
    }
    let width = usize::from(layout.shard_bits.div_ceil(4)).max(1);
    let mut directory = root
        .join("data")
        .join(collection)
        .join(format!("{shard:0width$x}"));
    if let Some(group) = group {
        let width = usize::from(layout.hash_group_bits.div_ceil(4)).max(1);
        directory = directory.join("g").join(format!("{group:0width$x}"));
    }
    let mut paths = vec![directory.join("chunks").join(format!("{chunk_uuid}.seg"))];
    let mut checked = root.to_path_buf();
    let safe_directory = directory.strip_prefix(root).ok().is_some_and(|relative| {
        relative.components().all(|component| {
            checked.push(component);
            fs::symlink_metadata(&checked).is_ok_and(|metadata| !metadata.file_type().is_symlink())
        })
    });
    if let Ok(entries) = safe_directory.then(|| fs::read_dir(&directory)).transpose() {
        let names = entries
            .into_iter()
            .flatten()
            .take(RESTORE_CACHE_ENTRY_LIMIT)
            .filter_map(|entry| entry.ok().map(|entry| entry.file_name()));
        if let Some(name) = restore_plain_cache_name(collection, shard, group, chunk_uuid, names) {
            paths.push(directory.join(name));
        }
    }
    Ok(paths)
}

pub(super) const RESTORE_CACHE_ENTRY_LIMIT: usize = 4096;

/// Inspect one addressed directory, with no inverse map or home traversal.
/// A directory beyond the entry cap can miss the cache and use cloud download.
pub(super) fn restore_plain_cache_name(
    collection: &str,
    shard: u16,
    group: Option<u32>,
    chunk_uuid: Uuid,
    names: impl Iterator<Item = std::ffi::OsString>,
) -> Option<std::ffi::OsString> {
    for name in names.take(RESTORE_CACHE_ENTRY_LIMIT) {
        let Some(text) = name.to_str() else { continue };
        let Some(number) = text.strip_suffix(".seg") else {
            continue;
        };
        if number.len() != 10 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let Ok(seq) = number.parse::<u64>() else {
            continue;
        };
        if plain_segment_log_uuid(collection, shard, group, seq) == chunk_uuid {
            return Some(name);
        }
    }
    None
}
