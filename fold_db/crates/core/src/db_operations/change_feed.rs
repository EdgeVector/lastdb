//! Durable, ordered local mutation feed for authorized LastDB apps.
//!
//! The feed is node-local coordination metadata. Product rows remain the
//! source of truth; consumers point-read them after receiving a change.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::schema::SchemaError;
use crate::storage::format::change_feed::{self as feed_format, ChangeFeed};
use crate::storage::StorageError;
use feed_format::{APPEND_BYTES, APPEND_EVENTS, LAST_SEQ_KEY};

pub use crate::storage::format::CHANGE_FEED_MAX_EVENTS as MAX_EVENTS;
pub use feed_format::ChangeFeedEvent;

pub(crate) mod native;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeFeedPage {
    pub events: Vec<ChangeFeedEvent>,
    pub gap: bool,
}

#[derive(Clone)]
pub struct ChangeFeedStore {
    format: ChangeFeed,
    native: Arc<crate::storage::disk_pack::PackRuntime>,
    last_seq: Arc<Mutex<u64>>,
}

impl ChangeFeedStore {
    pub(crate) async fn new(format: ChangeFeed) -> Result<Self, StorageError> {
        let last_seq = format.last_seq().await?;
        let native = format.runtime();
        Ok(Self {
            format,
            native,
            last_seq: Arc::new(Mutex::new(last_seq)),
        })
    }

    pub async fn append(&self, event: ChangeFeedEvent) -> Result<u64, SchemaError> {
        self.append_many(vec![event]).await
    }

    /// Preserve caller order in bounded chunks. Each native chunk acquires the
    /// writer before the last sequence and advances it only after durability.
    pub async fn append_many(&self, events: Vec<ChangeFeedEvent>) -> Result<u64, SchemaError> {
        if events.is_empty() {
            return Ok(*self.last_seq.lock().await);
        }
        let mut chunk = Vec::new();
        let mut bytes = 0usize;
        let mut end = 0;
        for event in events {
            let length = feed_format::event_charge(&event)?;
            if length > APPEND_BYTES {
                return Err(SchemaError::InvalidData(
                    "change-feed event exceeds its byte limit".into(),
                ));
            }
            if !chunk.is_empty() && (chunk.len() == APPEND_EVENTS || bytes + length > APPEND_BYTES)
            {
                end = self.append_chunk(std::mem::take(&mut chunk)).await?;
                bytes = 0;
            }
            bytes += length;
            chunk.push(event);
        }
        if !chunk.is_empty() {
            end = self.append_chunk(chunk).await?;
        }
        Ok(end)
    }

    pub async fn list_after(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<ChangeFeedPage, SchemaError> {
        if limit == 0 {
            return Ok(ChangeFeedPage {
                events: Vec::new(),
                gap: false,
            });
        }
        let last_seq = *self.last_seq.lock().await;
        let oldest = last_seq.saturating_sub(MAX_EVENTS).saturating_add(1).max(1);
        let gap = after > 0 && after.saturating_add(1) < oldest;
        let effective_after = if gap { oldest.saturating_sub(1) } else { after };
        let events = self.format.events_after(effective_after, limit).await?;
        Ok(ChangeFeedPage { events, gap })
    }

    pub async fn last_seq(&self) -> u64 {
        *self.last_seq.lock().await
    }
}
