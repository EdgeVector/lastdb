//! Shared body of `scan_prefix_partition_undecryptable` for the plain Last
//! Store adapters, which never see undecryptable rows.

use super::*;

/// A non-empty prefix is one ordinary prefix scan. An empty prefix is a
/// catalog/admin walk over every physical page; product `scan_prefix("")` still
/// rejects under `LASTDB_READS_REQUIRE_PARTITION`.
pub(super) async fn scan_prefix_partitioned<S: KvStore + ?Sized>(
    store: &S,
    prefix: &[u8],
) -> StorageResult<PartitionedScan> {
    if !prefix.is_empty() {
        return Ok(PartitionedScan {
            rows: store.scan_prefix(prefix).await?,
            undecryptable: Vec::new(),
        });
    }
    let mut rows = Vec::new();
    let mut cursor = None;
    loop {
        let page = store
            .scan_range_physical_paged(&[], &[0xff, 0xff, 0xff, 0xff], cursor.as_ref(), 256, 16)
            .await?;
        rows.extend(page.rows);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(PartitionedScan {
        rows,
        undecryptable: Vec::new(),
    })
}
