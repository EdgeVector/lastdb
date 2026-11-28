//! Page one signed stream from the first missing position. Every database uses the same reader.

use std::collections::VecDeque;

use super::relay::RelayClient;
use mutation_log::{DeviceId, NodeKey, SignedEntry};

const PAGE: u32 = 90;
const EMPTY_PAGE_LIMIT: usize = 256;

pub(super) struct StreamReader {
    writer: String,
    pub(super) device: String,
    next: Option<u64>,
    through: Option<u64>,
    cursor: Option<String>,
    buffered: VecDeque<SignedEntry>,
}

impl StreamReader {
    pub(super) fn new(writer: &str, device: &str, first: u64, through: Option<u64>) -> Self {
        Self {
            writer: writer.to_owned(),
            device: device.to_owned(),
            next: Some(first).filter(|seq| through.is_none_or(|limit| *seq <= limit)),
            through,
            cursor: None,
            buffered: VecDeque::new(),
        }
    }

    pub(super) fn stop(&mut self) {
        self.next = None;
        self.buffered.clear();
    }

    pub(super) fn front(&self) -> Option<(u64, NodeKey, DeviceId, u64)> {
        let h = self.buffered.front()?.header();
        Some((h.lamport, h.node_key, h.device_id, h.writer_seq))
    }

    pub(super) fn pop(&mut self) -> Option<SignedEntry> {
        self.buffered.pop_front()
    }

    pub(super) async fn fill(
        &mut self,
        database: &str,
        client: &RelayClient,
    ) -> Result<(), String> {
        if !self.buffered.is_empty() {
            return Ok(());
        }
        let Some(first) = self.next else {
            return Ok(());
        };
        for _ in 0..=EMPTY_PAGE_LIMIT {
            let (entries, continuation) = client
                .read(
                    database,
                    &self.writer,
                    Some(&self.device),
                    first,
                    self.cursor.as_deref(),
                    PAGE,
                )
                .await?;
            for entry in entries {
                let Some(expected) = self.next else { break };
                let h = entry.header();
                if h.database_id.to_hex() != database
                    || h.node_key.to_hex() != self.writer
                    || h.device_id.to_hex() != self.device
                {
                    return Err("the relay returned an entry for another database or stream".into());
                }
                if h.writer_seq != expected {
                    if self.through.is_none() && h.writer_seq > expected {
                        // A stream discovered from a citation can hold entries beyond a gap.
                        self.next = None;
                        break;
                    }
                    return Err(format!(
                        "the relay returned sequence {} where {expected} was due",
                        h.writer_seq
                    ));
                }
                self.next = expected
                    .checked_add(1)
                    .filter(|seq| self.through.is_none_or(|limit| *seq <= limit));
                self.buffered.push_back(entry);
            }
            if self.buffered.is_empty() && continuation.is_some() && self.cursor == continuation {
                return Err("relay log.read cursor did not advance".into());
            }
            self.cursor = continuation;
            if self.cursor.is_none() && self.through.is_none() {
                self.next = None;
            }
            if !self.buffered.is_empty() || self.next.is_none() {
                return Ok(());
            }
            if self.cursor.is_none() {
                return Err("the relay holds fewer entries of a stream than it reports".into());
            }
        }
        Err("relay log.read returned too many empty pages".into())
    }
}
