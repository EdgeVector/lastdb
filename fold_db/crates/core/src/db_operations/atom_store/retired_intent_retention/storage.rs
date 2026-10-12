//! Exact source ownership and target-keyed retention lookups; no journal walk.

use super::*;
use crate::storage::traits::KvStore;
use sha2::{Digest, Sha256};

const ROOT_BATCH_KEYS: usize = 512;

fn target_key(kind: &str, prefix: Option<&str>, target: &str) -> Vec<u8> {
    let mut hash = Sha256::new();
    hash.update(b"lastdb:retired-intent-root:v1\0");
    for part in [Some(kind), prefix, Some(target)] {
        hash.update([u8::from(part.is_some())]);
        if let Some(part) = part {
            hash.update((part.len() as u64).to_le_bytes());
            hash.update(part.as_bytes());
        }
    }
    format!("{kind}:{}", crate::hex::hex_lower(hash.finalize())).into_bytes()
}

fn target_shape(kind: &str, prefix: Option<&str>, target: &str) -> TargetOwners {
    TargetOwners {
        version: 1,
        kind: kind.to_string(),
        storage_prefix: prefix.map(str::to_string),
        target: target.to_string(),
        owners: BTreeSet::new(),
    }
}

fn decode_target(raw: &[u8], expected: &TargetOwners) -> Result<TargetOwners, SchemaError> {
    let row: TargetOwners = serde_json::from_slice(raw)
        .map_err(|_| refused("retired intent target root is malformed"))?;
    if row.version != 1
        || row.kind != expected.kind
        || row.storage_prefix != expected.storage_prefix
        || row.target != expected.target
        || row.owners.is_empty()
        || !row.owners.iter().all(|id| source_id_valid(id))
    {
        return Err(refused("retired intent target root identity differs"));
    }
    Ok(row)
}

pub(super) async fn read_many(
    store: &dyn KvStore,
    keys: &[Vec<u8>],
) -> Result<Vec<Option<Vec<u8>>>, SchemaError> {
    let mut values = Vec::with_capacity(keys.len());
    for batch in keys.chunks(ROOT_BATCH_KEYS) {
        let (present, part) = tokio::try_join!(
            store.exists_many(batch.to_vec()),
            store.get_many(batch.to_vec()),
        )
        .map_err(|_| refused("retired intent root batch is unreadable"))?;
        if part.len() != batch.len() || present.len() != batch.len() {
            return Err(refused("retired intent root batch is incomplete"));
        }
        if present
            .into_iter()
            .zip(&part)
            .any(|(present, value)| present != value.is_some())
        {
            return Err(refused("retired intent root presence differs"));
        }
        values.extend(part);
    }
    Ok(values)
}

impl AtomStore {
    async fn retired_root_store(&self) -> Result<Arc<dyn KvStore>, SchemaError> {
        let store = self
            .namespaced_store
            .as_ref()
            .ok_or_else(|| refused("retired intent retention requires a native store"))?;
        store
            .open_namespace(RETIRED_INTENT_RETENTION_NAMESPACE)
            .await
            .map_err(|_| refused("retired intent root store is unavailable"))
    }

    pub(crate) async fn retired_intent_atom_roots(
        &self,
        ids: &[String],
        prefix: Option<&str>,
    ) -> Result<HashSet<String>, SchemaError> {
        self.retired_target_roots("atom", ids, prefix).await
    }

    pub(crate) async fn retired_intent_blob_roots(
        &self,
        ids: &[String],
    ) -> Result<HashSet<String>, SchemaError> {
        // The two native local blob stores are global across storage scopes.
        self.retired_target_roots("blob", ids, None).await
    }

    async fn retired_target_roots(
        &self,
        kind: &str,
        ids: &[String],
        prefix: Option<&str>,
    ) -> Result<HashSet<String>, SchemaError> {
        if ids.is_empty() {
            return Ok(HashSet::new());
        }
        let store = self.retired_root_store().await?;
        let keys: Vec<_> = ids.iter().map(|id| target_key(kind, prefix, id)).collect();
        let rows = read_many(store.as_ref(), &keys).await?;
        let mut retained = HashSet::new();
        for (id, raw) in ids.iter().zip(rows) {
            if let Some(raw) = raw {
                decode_target(&raw, &target_shape(kind, prefix, id))?;
                retained.insert(id.clone());
            }
        }
        Ok(retained)
    }

    pub(super) async fn verify_retired_source_roots(
        &self,
        source: &SourceRoots,
    ) -> Result<(), SchemaError> {
        let store = self.retired_root_store().await?;
        let targets = source_targets(source);
        let mut keys: Vec<_> = targets
            .iter()
            .map(|row| target_key(&row.kind, row.storage_prefix.as_deref(), &row.target))
            .collect();
        keys.push(format!("source:{}", source.source_id).into_bytes());
        let mut rows = read_many(store.as_ref(), &keys).await?;
        let raw = rows
            .pop()
            .flatten()
            .ok_or_else(|| refused("retired intent source root is absent"))?;
        let owner: SourceRoots = serde_json::from_slice(&raw)
            .map_err(|_| refused("retired intent source root is malformed"))?;
        if owner != *source {
            return Err(refused("retired intent source dependency identity differs"));
        }
        for (expected, raw) in targets.iter().zip(rows) {
            let raw = raw.ok_or_else(|| refused("retired intent target root is absent"))?;
            if !decode_target(&raw, expected)?
                .owners
                .contains(&source.source_id)
            {
                return Err(refused("retired intent target owner differs"));
            }
        }
        Ok(())
    }

    pub(super) async fn commit_retired_source_roots(
        &self,
        source: &SourceRoots,
    ) -> Result<(), SchemaError> {
        let store = self.retired_root_store().await?;
        let targets = source_targets(source);
        let mut keys: Vec<_> = targets
            .iter()
            .map(|row| target_key(&row.kind, row.storage_prefix.as_deref(), &row.target))
            .collect();
        let source_key = format!("source:{}", source.source_id).into_bytes();
        keys.push(source_key.clone());
        let mut old = read_many(store.as_ref(), &keys).await?;
        if let Some(raw) = old.pop().flatten() {
            let existing: SourceRoots = serde_json::from_slice(&raw)
                .map_err(|_| refused("retired intent source root is malformed"))?;
            if existing != *source {
                return Err(refused("retired intent source dependency identity differs"));
            }
        }
        let mut items = Vec::with_capacity(keys.len());
        for ((mut target, prior), key) in targets.into_iter().zip(old).zip(&keys) {
            if let Some(raw) = prior {
                target = decode_target(&raw, &target)?;
            }
            target.owners.insert(source.source_id.clone());
            items.push((
                key.clone(),
                serde_json::to_vec(&target)
                    .map_err(|_| refused("retired intent root encoding failed"))?,
            ));
        }
        items.push((
            source_key,
            serde_json::to_vec(source)
                .map_err(|_| refused("retired intent source encoding failed"))?,
        ));
        for batch in items.chunks(ROOT_BATCH_KEYS) {
            store
                .batch_put(batch.to_vec())
                .await
                .map_err(|_| refused("retired intent root write failed"))?;
        }
        self.flush_retired_roots(store.as_ref(), &keys).await?;
        let after = read_many(store.as_ref(), &keys).await?;
        if after
            .into_iter()
            .zip(items)
            .any(|(actual, (_, expected))| actual.as_ref() != Some(&expected))
        {
            return Err(refused("retired intent root readback differs"));
        }
        Ok(())
    }

    /// One native barrier for the exact written groups, with the ordinary fallback.
    async fn flush_retired_roots(
        &self,
        root_store: &dyn KvStore,
        keys: &[Vec<u8>],
    ) -> Result<(), SchemaError> {
        let namespaced = self
            .namespaced_store
            .as_ref()
            .ok_or_else(|| refused("retired intent retention requires a native store"))?;
        let Some(native) = namespaced.raw_last_store() else {
            return root_store
                .flush()
                .await
                .map_err(|_| refused("retired intent root flush failed"));
        };
        let mut groups = keys
            .iter()
            .map(|key| {
                let key = std::str::from_utf8(key)
                    .map_err(|_| refused("retired intent root key is invalid"))?;
                Ok(native.shard_key_of(RETIRED_INTENT_RETENTION_NAMESPACE, key))
            })
            .collect::<Result<Vec<_>, SchemaError>>()?;
        groups.sort_unstable();
        groups.dedup();
        namespaced
            .flush_written_keys(&groups)
            .await
            .map_err(|_| refused("retired intent scoped root flush failed"))
    }
}

fn source_targets(source: &SourceRoots) -> Vec<TargetOwners> {
    let mut targets = Vec::new();
    for scope in &source.scopes {
        targets.extend(
            scope
                .atoms
                .iter()
                .map(|id| target_shape("atom", scope.storage_prefix.as_deref(), id)),
        );
    }
    targets.extend(source.blobs.iter().map(|id| target_shape("blob", None, id)));
    targets
}
