//! Explicit logical conversion of a separate inactive native transfer target.
//! The old program retains ownership of its source. Startup never calls this.

use super::{map, StorageError, StorageResult};
use crate::crypto::{at_rest::open_at_rest_bounded, content_key_proof as proof, is_sealed_at_rest};
use crate::db_operations::atom_store::{key_format, AtomStore};
use crate::storage::{encrypting_namespaced_store::CATALOG_NAMESPACES, laststore::native_routing};
use laststore::disk_pack::home::{DiskHome, HomeMutation, HomeRead, HomeSnapshot};
use laststore::disk_pack::{maintenance::visit_generation, ReadBudget};
use std::io::Read;
use std::path::Path;

const MAX_VALUE_BYTES: u64 = 64 * 1024 * 1024;
const BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Authenticate before any write, unwrap only the former namespace envelope,
/// and publish format 6 with its key proof last. An interruption is resumable.
/// The caller must own a separate inactive target, never the migration source.
pub fn convert_transferred_node(root: &Path, account_key: &[u8; 32]) -> StorageResult<()> {
    let home = DiskHome::open(&root.join("disk-pack")).map_err(map)?;
    let mut writer = home.snapshot().map_err(map)?;
    let current = writer.current().clone();
    let mut reader = home
        .read_from_writer(current.clone(), budget(), &writer.read_lease())
        .map_err(map)?;
    if !require_older_format(&mut reader, account_key)? {
        return Ok(());
    }
    drop(reader);
    let mut batch = Vec::new();
    let mut bytes = 0;
    visit_generation(home.path(), &current, |record| {
        let Some(body) = record.body else {
            return Ok(());
        };
        let marker = AtomStore::CURRENT_KEY_FORMAT_KEY;
        if record.collection == "cas_blobs"
            || record.collection == "__native_home"
            || CATALOG_NAMESPACES.contains(&record.collection)
            || record.key == marker
        {
            std::io::copy(body, &mut std::io::sink())?;
            return Ok(());
        }
        if record.body_bytes > MAX_VALUE_BYTES {
            return Err(laststore::Error::Config(
                "conversion value exceeds its bound".into(),
            ));
        }
        let mut stored = Vec::new();
        body.take(MAX_VALUE_BYTES + 1).read_to_end(&mut stored)?;
        if !is_sealed_at_rest(&stored) {
            return Ok(());
        }
        let plain = open_at_rest_bounded(account_key, &stored, MAX_VALUE_BYTES)
            .map_err(|_| laststore::Error::Config("conversion envelope cannot be opened".into()))?;
        bytes += plain.len();
        batch.push(HomeMutation {
            collection: record.collection.into(),
            key: record.key.into(),
            body: Some(plain),
        });
        if batch.len() >= 128 || bytes >= BATCH_BYTES {
            commit(&mut writer, std::mem::take(&mut batch))?;
            bytes = 0;
        }
        Ok(())
    })
    .map_err(map)?;
    if !batch.is_empty() {
        commit(&mut writer, batch).map_err(map)?;
    }
    let marker = AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes();
    commit(
        &mut writer,
        vec![
            HomeMutation {
                collection: native_routing::collection_for_key(marker).into(),
                key: native_routing::encode(marker),
                body: Some(key_format::KEY_FORMAT.to_vec()),
            },
            HomeMutation {
                collection: proof::NAMESPACE.into(),
                key: native_routing::encode(proof::KEY),
                body: Some(proof::create(account_key)?),
            },
            HomeMutation {
                collection: "__native_home".into(),
                key: "namespaces\0metadata".into(),
                body: Some(vec![1]),
            },
        ],
    )
    .map_err(map)?;
    Ok(())
}

fn require_older_format(reader: &mut HomeRead, key: &[u8; 32]) -> StorageResult<bool> {
    let marker = AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes();
    let stored = reader
        .get_bounded(
            native_routing::collection_for_key(marker),
            &native_routing::encode(marker),
            8192,
        )
        .map_err(map)?
        .ok_or_else(|| invalid("conversion format marker is missing"))?;
    if stored == key_format::KEY_FORMAT {
        let stored = reader
            .get_bounded(proof::NAMESPACE, &native_routing::encode(proof::KEY), 8192)
            .map_err(map)?
            .ok_or_else(|| invalid("conversion key proof is missing"))?;
        proof::verify(key, &stored)?;
        return Ok(false);
    }
    if !is_sealed_at_rest(&stored) {
        return Err(invalid(
            "conversion needs an authenticated old format marker",
        ));
    }
    let format = open_at_rest_bounded(key, &stored, 4096)
        .map_err(|_| invalid("conversion account key differs"))?;
    if format != b"4" && format != b"5" {
        return Err(invalid("unsupported conversion logical format"));
    }
    Ok(true)
}

fn commit(writer: &mut HomeSnapshot, batch: Vec<HomeMutation>) -> laststore::Result<()> {
    let reservation = writer.reserve_mutation(&batch)?;
    let _space = writer.reserve_space(
        reservation.quote().physical_bytes(),
        &laststore::disk_pack::maintenance::SpacePolicy::default(),
    )?;
    writer.commit_mutation(batch, reservation)?;
    Ok(())
}

fn budget() -> ReadBudget {
    ReadBudget {
        pages: 4096,
        blocks: 16384,
        bytes: 8 * 1024 * 1024,
        keys: 2,
    }
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}
