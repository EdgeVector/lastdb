//! Every owned share has its own encrypted cut in the exact source inventory.

use super::{protocol::CloudReplacementRoot, RetainedSource};
use crate::sync::{SyncError, SyncResult};
use std::collections::{BTreeMap, BTreeSet};

pub struct RetainedShareSnapshot {
    pub logical_root: String,
    pub manifest_sha256: String,
    pub ciphertext: Vec<u8>,
    pub writer_frontier: BTreeMap<String, u64>,
}

impl RetainedSource {
    pub(super) fn add_source_roots(
        &mut self,
        main_root: &str,
        shares: Vec<RetainedShareSnapshot>,
    ) -> SyncResult<()> {
        let objects: Vec<_> = self
            .files
            .iter()
            .filter(|file| file.object.logical_root == main_root)
            .map(|file| file.object.clone())
            .collect();
        let manifest = objects
            .iter()
            .find(|object| object.role == "manifest")
            .ok_or_else(|| error("main manifest absent"))?;
        let layout = objects
            .iter()
            .find(|object| object.role == "layout")
            .ok_or_else(|| error("main layout absent"))?;
        self.prepared.roots.push(CloudReplacementRoot {
            format: "laststore_v1".into(),
            store_uuid: self.manifest.store_uuid.clone(),
            source_id: self.prepared.source_id.clone(),
            writer_frontier: self.prepared.writer_frontier.clone(),
            logical_root: main_root.into(),
            manifest_sha256: self.prepared.manifest_sha256.clone(),
            manifest_key: manifest.relative_key.clone(),
            layout_sha256: layout.sha256.clone(),
            layout_key: layout.relative_key.clone(),
            objects,
        });
        for share in shares {
            if !share.logical_root.starts_with("share:")
                || !crate::hex::is_lower_hex_sha256(&share.manifest_sha256)
            {
                return Err(error("invalid owned share source identity"));
            }
            let manifest_key = format!("snapshots/{}.enc", share.manifest_sha256);
            self.add_bytes(
                &share.logical_root,
                manifest_key.clone(),
                "share_snapshot",
                &share.ciphertext,
            )?;
            self.add_bytes(
                &share.logical_root,
                "snapshots/latest.enc".into(),
                "share_latest",
                &share.ciphertext,
            )?;
            self.prepared.roots.push(CloudReplacementRoot {
                format: "logical_snapshot_v1".into(),
                store_uuid: String::new(),
                source_id: self.prepared.source_id.clone(),
                writer_frontier: share.writer_frontier,
                logical_root: share.logical_root.clone(),
                manifest_sha256: share.manifest_sha256,
                manifest_key,
                layout_sha256: String::new(),
                layout_key: String::new(),
                objects: self
                    .files
                    .iter()
                    .filter(|file| file.object.logical_root == share.logical_root)
                    .map(|file| file.object.clone())
                    .collect(),
            });
        }
        Ok(())
    }

    pub(super) fn verify_source_roots(&self) -> SyncResult<()> {
        if self.prepared.roots.is_empty() {
            return Err(error("retained source root inventory absent"));
        }
        let mut identities = BTreeSet::new();
        let mut roots = BTreeSet::new();
        let main = self
            .prepared
            .roots
            .first()
            .ok_or_else(|| error("main source root absent"))?;
        if main.format != "laststore_v1"
            || main.store_uuid != self.manifest.store_uuid
            || main.logical_root
                != crate::storage::laststore::cloud_db_hash_for_store_uuid(
                    &self.manifest.store_uuid,
                )
            || main.manifest_sha256 != self.prepared.manifest_sha256
            || main.layout_sha256 != self.prepared.layout_sha256
            || main.writer_frontier != self.prepared.writer_frontier
        {
            return Err(error("main source root differs from its exact cut"));
        }
        for root in &self.prepared.roots {
            if !roots.insert(&root.logical_root) || root.source_id != self.prepared.source_id {
                return Err(error("retained source root identity differs"));
            }
            for object in &root.objects {
                if object.logical_root != root.logical_root
                    || !self.prepared.objects.contains(object)
                    || !identities.insert((&object.logical_root, &object.relative_key))
                {
                    return Err(error(
                        "retained source roots change the exact object inventory",
                    ));
                }
            }
            if root.format == "logical_snapshot_v1" {
                let snapshot = root
                    .objects
                    .iter()
                    .find(|object| {
                        object.role == "share_snapshot" && object.relative_key == root.manifest_key
                    })
                    .ok_or_else(|| error("share source snapshot absent"))?;
                if !root.objects.iter().any(|object| {
                    object.role == "share_latest"
                        && object.relative_key == "snapshots/latest.enc"
                        && object.sha256 == snapshot.sha256
                        && object.bytes == snapshot.bytes
                }) {
                    return Err(error("share latest pointer differs from the retained cut"));
                }
            } else if root.format != "laststore_v1" {
                return Err(error("unknown retained source root format"));
            } else if root.logical_root != main.logical_root {
                return Err(error("retained source has an unexpected second main store"));
            }
        }
        if identities.len() != self.prepared.objects.len() {
            return Err(error("retained source root inventory is incomplete"));
        }
        Ok(())
    }
}

fn error(message: &str) -> SyncError {
    SyncError::Storage(message.into())
}
