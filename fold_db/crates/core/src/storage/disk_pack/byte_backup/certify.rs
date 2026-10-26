//! Same-root atom, key, directory, native-CAS and metadata completeness proof.
use super::super::file_dependencies as files;
use super::{invalid, Inner, StorageResult};
use lastdb_storage_policy::backup_decode as budget;
use laststore::disk_pack::home::HomeRead;
use laststore::disk_run::home::ReadBudget;
use std::fs::{self, File};

pub(super) fn read(inner: &Inner, budget: ReadBudget) -> StorageResult<HomeRead> {
    let source = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?;
    inner.home.read(source, budget)
}
fn bounded_read(inner: &Inner, bytes: u64, keys: u64) -> StorageResult<HomeRead> {
    read(
        inner,
        ReadBudget {
            pages: if bytes == files::ATOM_INVENTORY_READ_BYTES {
                budget::ATOM_INVENTORY_PAGES
            } else {
                budget::NATIVE_STREAM_PAGES
            },
            blocks: budget::NATIVE_STREAM_BLOCKS,
            bytes,
            keys,
        },
    )
}
pub(super) fn certify(inner: &Inner, key: &[u8; 32]) -> StorageResult<()> {
    let source = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?;
    for group in 0..1024u16 {
        let mut start = Vec::new();
        loop {
            let page = {
                let mut reader = bounded_read(inner, files::ATOM_INVENTORY_READ_BYTES, 10000)?;
                let mut cursor = files::atom_inventory(&mut reader, group, &start)?;
                cursor
                    .next_page_yielding_bounded(1, 0, 0, budget::ATOM_BODY_MAX_BYTES)
                    .map_err(super::super::map)?
            };
            let next = page.resume.clone().or_else(|| {
                page.records
                    .last()
                    .map(|record| record.key.as_bytes().to_vec())
            });
            for row in page.records {
                atom(inner, key, row)?;
            }
            if page.exhausted {
                break;
            }
            let mut next = next.ok_or_else(|| invalid("byte atom continuation missing"))?;
            if next.len() >= 65535 || (!start.is_empty() && next < start) {
                return Err(invalid("byte atom continuation differs"));
            }
            next.push(0);
            start = next;
        }
    }
    files::verify_metadata_coverage(source.current(), key, |budget| read(inner, budget))?;
    Ok(())
}
fn atom(
    inner: &Inner,
    key: &[u8; 32],
    row: laststore::disk_pack::RangeRecord,
) -> StorageResult<()> {
    let Some((scope, id)) = files::atom_coordinate(&row.key)? else {
        return Ok(());
    };
    let current = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?
        .current();
    let atom = files::open_atom(key, &row.body, current, |budget| read(inner, budget))?;
    if atom.uuid() != id {
        return Err(invalid("byte atom identity differs"));
    }
    let dependencies = files::expected(&atom)?;
    drop(atom);
    drop(row);
    let mut reader = bounded_read(inner, budget::DIRECTORY_READ_BYTES, 10000)?;
    files::read_directories(&mut reader, scope.as_deref(), &id, &dependencies)?;
    drop(reader);
    for dependency in dependencies {
        native_file(inner, key, &dependency)?;
    }
    Ok(())
}
fn native_file(
    inner: &Inner,
    key: &[u8; 32],
    dependency: &files::FileDependency,
) -> StorageResult<()> {
    scratch_cleanup(&inner.store.root)?;
    let path = inner.store.root.join("native-cas.pending");
    let result = (|| {
        let mut output = files::NativeCasOutput::create(&path, inner.home.space_policy())?;
        let mut reader = bounded_read(inner, budget::NATIVE_STREAM_LOOKUP_READ_BYTES, 1)?;
        let proof = files::read_native_cas(&mut reader, dependency, &mut output)?;
        drop(reader);
        drop(output.finish()?);
        let proof =
            proof.ok_or_else(|| invalid("byte backup remote-only file source unsupported"))?;
        files::verify_native_cas(proof, &path, key, dependency, inner.home.space_policy())?;
        Ok(())
    })();
    scratch_cleanup(&inner.store.root)?;
    result
}
pub(super) fn scratch_cleanup(root: &std::path::Path) -> StorageResult<()> {
    for name in [
        "native-cas.pending",
        "native-cas.json.pending",
        "native-cas.inner.pending",
    ] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(meta) if meta.is_file() => fs::remove_file(root.join(name))?,
            Ok(_) => return Err(invalid("byte source scratch type")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    File::open(root)?.sync_all()?;
    Ok(())
}
