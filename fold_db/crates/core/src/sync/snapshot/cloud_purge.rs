//! Bounded logical share photograph. Large stores use LastStore file cuts.

use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

pub(crate) const MAX_SHARE_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

impl Snapshot {
    pub(crate) async fn create_cloud_purge_share(
        store: &dyn NamespacedStore,
        device_id: &str,
        last_seq: u64,
        prefix: &str,
    ) -> SyncResult<Self> {
        let mut namespaces = Vec::new();
        let mut bytes = 0usize;
        for name in store.list_namespaces().await? {
            if snapshot_should_skip_namespace(&name) || name == "cas_blobs" {
                continue;
            }
            let kv = store.open_namespace(&name).await?;
            let mut entries = Vec::new();
            for native in ["", "emb:", "graveyard:emb:"] {
                let (start, end) =
                    crate::kind_partition::exact_prefix_bounds(&format!("{native}{prefix}:"));
                let mut cursor = start.into_bytes();
                loop {
                    let rows = kv.scan_range_paged(&cursor, end.as_bytes(), 4).await?;
                    let Some((last, _)) = rows.last() else { break };
                    cursor = last.clone();
                    cursor.push(0);
                    for (key, value) in rows {
                        if !key_belongs_to_target(&key, prefix) {
                            return Err(share_limit_error());
                        }
                        bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
                        if bytes > MAX_SHARE_SNAPSHOT_BYTES / 2 {
                            return Err(share_limit_error());
                        }
                        entries.push(SnapshotEntry {
                            key: BASE64.encode(key),
                            value: BASE64.encode(value),
                        });
                    }
                }
            }
            namespaces.push(NamespaceData { name, entries });
        }
        Ok(snapshot_from_namespaces(
            device_id,
            last_seq,
            prepare_logical_photograph(namespaces, true)?,
            SnapshotValueEncoding::Logical,
        ))
    }

    pub(crate) fn cloud_purge_share_bytes(&self) -> SyncResult<usize> {
        let mut count = self.writer_frontier.keys().fold(512usize, |bytes, writer| {
            bytes.saturating_add(writer.len()).saturating_add(48)
        });
        // Entry strings already include base64 expansion. Leave metadata room
        // without allocating a second serialization before the admission check.
        for namespace in &self.namespaces {
            count = count
                .saturating_add(namespace.name.len())
                .saturating_add(128);
            for entry in &namespace.entries {
                count = count
                    .saturating_add(entry.key.len())
                    .saturating_add(entry.value.len())
                    .saturating_add(64);
            }
        }
        if count > MAX_SHARE_SNAPSHOT_BYTES {
            return Err(share_limit_error());
        }
        Ok(count)
    }
}

pub(crate) fn share_limit_error() -> SyncError {
    SyncError::Storage("owned share logical snapshot exceeds its 64 MiB product limit before cloud hold; use file slabs or a LastStore scoped backup".into())
}
