use super::keys::{data_key, data_prefix, prefix_end, STATE_KEY};
use super::model::{State, Winner};
use super::*;
use crate::db_operations::DbOperations;
use crate::storage::disk_pack::{transaction, PackNamespacedStore, PackRuntime};
use crate::storage::traits::{KvStore, NamespacedStore};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

/// Isolated protocol state over real v2 disk storage, never a production schema.
pub struct Replica {
    pub(super) kv: Arc<dyn KvStore>,
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) control: Option<Arc<control::RootControl>>,
}

impl Replica {
    pub async fn open(path: &Path, key: &[u8; 32]) -> StorageResult<Self> {
        control::open_replica(path, key).await
    }

    pub(super) async fn open_generation(path: &Path, key: &[u8; 32]) -> StorageResult<Self> {
        let store = PackNamespacedStore::open_keyed(path, Some(key))?;
        let runtime = store
            .disk_pack_runtime()
            .ok_or_else(|| invalid("spike requires the v2 native runtime"))?;
        let shared: Arc<dyn NamespacedStore> = Arc::new(store);
        DbOperations::from_namespaced_store(Arc::clone(&shared))
            .await
            .map_err(|error| invalid(format!("initialize spike v2 store: {error}")))?;
        let kv = shared.open_namespace(NAMESPACE).await?;
        Ok(Self {
            kv,
            runtime,
            control: None,
        })
    }

    pub async fn author(&self, writer: &Writer, changes: Vec<Change>) -> StorageResult<Entry> {
        self.require_ready()?;
        if self
            .control
            .as_ref()
            .is_some_and(|control| control.writer.origin != writer.origin)
        {
            return Err(invalid(
                "writer does not match this replica's stable origin",
            ));
        }
        transaction::request_scope(Some(Arc::clone(&self.runtime)), async {
            let mut state = self.state().await?;
            let position = state
                .frontiers
                .get(&writer.origin)
                .cloned()
                .unwrap_or_default();
            let entry = writer.entry(&position, changes, &mut state.clock)?;
            self.stage_entries(std::slice::from_ref(&entry), state)
                .await?;
            Ok(entry)
        })
        .await
    }

    pub async fn import(&self, entries: &[Entry]) -> StorageResult<ImportReport> {
        super::stream::validate_import(entries)?;
        transaction::request_scope(Some(Arc::clone(&self.runtime)), async {
            self.stage_entries(entries, self.state().await?).await
        })
        .await
    }

    pub async fn frontiers(&self) -> StorageResult<BTreeMap<String, Position>> {
        Ok(self.state().await?.frontiers)
    }

    pub async fn clock(&self) -> StorageResult<crate::schema::types::MutationAuthorClockState> {
        Ok(self.state().await?.clock)
    }

    pub async fn get(&self, hash: &str, range: &str) -> StorageResult<Option<String>> {
        self.require_ready()?;
        if hash.len() > 1024 || range.len() > 1024 {
            return Err(invalid("peer key exceeds 1 KiB"));
        }
        self.kv
            .get(&data_key(hash, range))
            .await?
            .map(|bytes| decode::<Winner>(&bytes).map(|winner| winner.change.value))
            .transpose()
            .map(Option::flatten)
    }

    /// A page visits at most limit stored winners, including retained Deletes.
    pub async fn range(
        &self,
        hash: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<RangePage> {
        self.require_ready()?;
        if limit == 0 || limit > MAX_PAGE || hash.len() > 1024 {
            return Err(invalid("range needs a hash and a page of 1..=128"));
        }
        let prefix = data_prefix(hash);
        let mut start = prefix.clone();
        if let Some(after) = after {
            if after.len() > 1024 {
                return Err(invalid("range cursor exceeds 1 KiB"));
            }
            start.extend_from_slice(after.as_bytes());
            start.push(0);
        }
        let rows = self
            .kv
            .scan_range_paged(&start, &prefix_end(&prefix), limit)
            .await?;
        let next = if rows.len() == limit {
            rows.last()
                .map(|(key, _)| String::from_utf8(key[prefix.len()..].to_vec()))
                .transpose()
                .map_err(|_| invalid("invalid range key"))?
        } else {
            None
        };
        let mut items = Vec::new();
        for (_, bytes) in rows {
            let winner: Winner = decode(&bytes)?;
            if winner.change.value.is_some() {
                items.push(winner.change);
            }
        }
        Ok(RangePage { items, next })
    }

    pub(super) async fn state(&self) -> StorageResult<State> {
        self.require_ready()?;
        self.kv
            .get(STATE_KEY)
            .await?
            .map(|bytes| decode(&bytes))
            .transpose()
            .map(|state| state.unwrap_or_default())
    }

    fn require_ready(&self) -> StorageResult<()> {
        if let Some(control) = &self.control {
            control.require_ready()?;
        }
        Ok(())
    }
}
