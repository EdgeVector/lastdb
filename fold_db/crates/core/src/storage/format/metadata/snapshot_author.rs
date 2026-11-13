//! Give a restored device its own author stream and retain the clock floor.

use super::{decode_author, initial_local_author, invalid_author, Metadata, LOCAL_AUTHOR_KEY};
use crate::storage::disk_pack::PackRuntime;
use crate::storage::error::StorageResult;
use crate::storage::format::METADATA_NAMESPACE;
use std::sync::Arc;

impl Metadata {
    pub(in crate::storage) async fn fork_restored_author(
        runtime: &Arc<PackRuntime>,
    ) -> StorageResult<()> {
        let store = runtime.open_store(METADATA_NAMESPACE).await?;
        let author = store.get(LOCAL_AUTHOR_KEY.as_bytes()).await?;
        let previous = decode_author(author.as_deref())?;
        let mut next = decode_author(Some(&initial_local_author(uuid::Uuid::new_v4())?))?;
        next.clock_floor_ms = previous
            .clock_floor_ms
            .max(previous.progress.applied_through_ms);
        let value = serde_json::to_vec(&next).map_err(invalid_author)?;
        Self::new(store, Arc::clone(runtime))
            .stage(vec![(LOCAL_AUTHOR_KEY.to_owned(), Some(value))])
            .await
    }
}
