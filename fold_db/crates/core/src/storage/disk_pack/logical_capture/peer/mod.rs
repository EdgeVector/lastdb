//! Owner-pinned peer anchors and bounded import positions share the data commit.

use super::*;
use serde::{Deserialize, Serialize};

mod captured;
mod enrollment;
mod import;
mod state;

pub(super) use captured::positions as captured_positions;
pub(super) const CAPTURE_RECORD_BYTES: u64 = 16 * 1024;

const MAX_PEERS: usize = 32;
const MAX_RECEIPTS: u64 = 65_536;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureExportPage {
    pub anchor: Position,
    pub position: Position,
    pub after: u64,
    pub events: Vec<CaptureEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerPosition {
    pub anchor: Position,
    pub position: Position,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportReceipt {
    pub position: Position,
    pub imported_events: usize,
    pub duplicate_events: usize,
    pub imported_outcomes: usize,
    pub receipt_count: u64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Roster {
    version: u8,
    origins: Vec<String>,
    receipt_count: u64,
}

impl Default for Roster {
    fn default() -> Self {
        Self {
            version: 1,
            origins: Vec::new(),
            receipt_count: 0,
        }
    }
}

impl Roster {
    fn validate(&self, own_origin: &str) -> StorageResult<()> {
        if self.version != 1
            || self.origins.len() > MAX_PEERS
            || self.receipt_count > MAX_RECEIPTS
            || self.origins.windows(2).any(|pair| pair[0] >= pair[1])
            || self.origins.iter().any(|origin| origin == own_origin)
        {
            return Err(invalid("logical peer roster differs"));
        }
        for origin in &self.origins {
            verify::origin_key(origin)?;
        }
        Ok(())
    }

    fn reserve_receipts(&mut self, count: usize) -> StorageResult<()> {
        self.receipt_count = self
            .receipt_count
            .checked_add(count as u64)
            .filter(|count| *count <= MAX_RECEIPTS)
            .ok_or_else(|| StorageError::WriteResourceLimit {
                detail: "logical peer import reached its 65536 digest receipt cap".into(),
            })?;
        Ok(())
    }
}

impl PeerPosition {
    fn validate(&self, origin: &str) -> StorageResult<()> {
        verify::origin_key(origin)?;
        unhex::<32>(&self.anchor.sha256)?;
        unhex::<32>(&self.position.sha256)?;
        if self.anchor.sequence != 0
            || self.anchor.origin != origin
            || self.position.origin != origin
            || self.position.sequence > MAX_RECEIPTS
            || (self.position.sequence == 0 && self.position != self.anchor)
        {
            return Err(invalid("logical peer anchor or position differs"));
        }
        Ok(())
    }
}

fn roster_key() -> String {
    native_routing::encode(b"peers")
}

fn peer_key(origin: &str) -> String {
    native_routing::encode(format!("p\0{origin}").as_bytes())
}

fn receipt_key(origin: &str, sequence: u64) -> String {
    native_routing::encode(format!("i\0{origin}\0{sequence:020}").as_bytes())
}

fn operation(key: String, body: Vec<u8>) -> HomeMutation {
    HomeMutation {
        collection: COLLECTION.into(),
        key,
        body: Some(body),
    }
}

fn enabled_origin(runtime: &PackRuntime) -> StorageResult<String> {
    runtime
        .logical_capture
        .as_ref()
        .map(|capture| capture.identity.origin())
        .ok_or_else(|| invalid("logical capture is disabled"))
}
