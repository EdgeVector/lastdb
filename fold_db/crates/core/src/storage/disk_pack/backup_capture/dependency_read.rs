//! Every dependency read selects the capture's retained immutable root.
use super::super::file_dependencies as files;
use super::{invalid, ExportInner, StorageResult};
use lastdb_storage_policy::backup_decode as budget;
use laststore::disk_pack::backup::export::SnapshotExport;
use laststore::disk_pack::home::HomeRead;
use laststore::disk_pack::{Current, ReadBudget};

pub(super) fn held_current(inner: &ExportInner) -> StorageResult<&Current> {
    inner
        .source
        .as_ref()
        .map(SnapshotExport::current)
        .ok_or_else(|| invalid("backup retained source is unavailable"))
}

pub(super) fn held_read(inner: &ExportInner, bytes: u64) -> StorageResult<HomeRead> {
    held_read_budget(
        inner,
        ReadBudget {
            pages: if bytes == files::ATOM_INVENTORY_READ_BYTES {
                budget::ATOM_INVENTORY_PAGES
            } else {
                budget::NATIVE_STREAM_PAGES
            },
            blocks: budget::NATIVE_STREAM_BLOCKS,
            bytes,
            keys: if bytes == budget::NATIVE_STREAM_LOOKUP_READ_BYTES {
                1
            } else {
                10000
            },
        },
    )
}

pub(super) fn held_read_budget(inner: &ExportInner, budget: ReadBudget) -> StorageResult<HomeRead> {
    inner
        .runtime
        .home
        .read_from_writer(
            held_current(inner)?.clone(),
            budget,
            &inner.runtime.read_lease,
        )
        .map_err(super::super::map)
}
