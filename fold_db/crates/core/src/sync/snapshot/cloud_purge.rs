//! Logical share cuts use the process memory guard, without a fixed data cap.

use super::*;
use crate::sync::cloud_purge::{
    memory::{self, Admission},
    org_rows,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

impl Snapshot {
    pub(crate) async fn create_cloud_purge_share(
        store: &dyn NamespacedStore,
        device_id: &str,
        last_seq: u64,
        prefix: &str,
        admission: &Admission,
    ) -> SyncResult<Self> {
        let raw = store
            .raw_last_store()
            .ok_or_else(|| fail("share cut requires LastStore"))?;
        let target_prefixes = [prefix.to_string()];
        let mut namespaces = Vec::new();
        let mut json_bound = 1024u64;
        for name in store.list_namespaces().await? {
            if snapshot_should_skip_namespace(&name) || name == "cas_blobs" {
                continue;
            }
            let identities = if name == "main" {
                org_rows::main_rows(&raw, admission)?
            } else {
                org_rows::physical_keys(&raw, &name, admission)?
            };
            let keys: Vec<_> = identities
                .into_keys()
                .filter(|key| {
                    namespace_key_belongs_to_any_target(
                        key.as_bytes(),
                        &target_prefixes,
                        name == "main",
                    )
                })
                .collect();
            let entries = read_namespace(store, &name, &keys, &mut json_bound, admission).await?;
            namespaces.push(NamespaceData { name, entries });
        }
        admission.check(memory::snapshot_peak(json_bound, memory::row_peak(0)?)?)?;
        Ok(snapshot_from_namespaces(
            device_id,
            last_seq,
            validate_photograph(namespaces)?,
            SnapshotValueEncoding::Logical,
        ))
    }

    /// Conservative JSON and container allowance, without a second serialization.
    pub(crate) fn cloud_purge_share_bytes(&self) -> SyncResult<u64> {
        let mut count = self
            .writer_frontier
            .keys()
            .try_fold(1024u64, |sum, writer| {
                memory::checked_sum(&[sum, writer.len() as u64, 128])
            })?;
        for namespace in &self.namespaces {
            count = memory::checked_sum(&[count, namespace.name.len() as u64, 256])?;
            for entry in &namespace.entries {
                count = memory::checked_sum(&[
                    count,
                    entry.key.len() as u64,
                    entry.value.len() as u64,
                    256,
                ])?;
            }
        }
        Ok(count)
    }
}

async fn read_namespace(
    store: &dyn NamespacedStore,
    name: &str,
    keys: &[String],
    json_bound: &mut u64,
    admission: &Admission,
) -> SyncResult<Vec<SnapshotEntry>> {
    let sizes = memory::raw_lengths(
        admission,
        memory::snapshot_peak(*json_bound, 0)?,
        store,
        name,
        keys,
    )?;
    let kv = store.open_namespace(name).await?;
    let mut entries = Vec::new();
    for (key, size) in keys.iter().zip(sizes) {
        let size = size.ok_or_else(|| fail("owned share source body disappeared"))?;
        let encoded = memory::checked_scale(
            memory::checked_sum(&[key.len() as u64, memory::outer_bound(size), 256])?,
            2,
        )?;
        *json_bound = memory::checked_sum(&[*json_bound, encoded])?;
        admission.check(memory::snapshot_peak(
            *json_bound,
            memory::read_peak(size)?,
        )?)?;
        let value = kv
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| fail("owned share source disappeared after admission"))?;
        entries.push(SnapshotEntry {
            key: BASE64.encode(key),
            value: BASE64.encode(value),
        });
    }
    Ok(entries)
}

fn fail(message: &str) -> SyncError {
    SyncError::Storage(message.into())
}
