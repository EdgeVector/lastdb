//! Durable per-molecule-key order for normal Delete and physical peer Delete.

use super::AtomEntry;
use serde::{Deserialize, Serialize};

/// One Delete winner. The key includes the exact final `mk:` storage key,
/// including its personal or org scope, after all key encodings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DeleteBarrier {
    pub mk_key: String,
    pub written_at: u64,
    pub logical_counter: u64,
    pub device_id: String,
    pub mutation_uuid: String,
    pub kind: DeleteKind,
    pub displaced_atom_uuid: Option<String>,
    /// The physical-log sequence is only a duplicate tie for legacy deletes.
    /// It must not outrank the original device's write time.
    pub cloud_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeleteKind {
    Normal,
    LegacyPhysical,
}

/// Version two groups barriers by the exact `mk:` partition, including its
/// scope and escaped hash. The suffix encodes the complete final key, byte for
/// byte, so distinct ranges and scopes cannot share a barrier.
#[must_use]
pub(crate) fn delete_barrier_key(mk_key: &[u8]) -> String {
    use std::fmt::Write;
    let partition_end = mk_key
        .iter()
        .position(|byte| *byte == 0)
        .map_or(mk_key.len(), |at| at + 1);
    let scope = std::str::from_utf8(mk_key)
        .ok()
        .and_then(|key| key.find("mk:").map(|at| &key[..at]))
        .unwrap_or("");
    let mut key = format!("{scope}rdel:v2:");
    for byte in &mk_key[..partition_end] {
        write!(&mut key, "{byte:02x}").expect("write to String cannot fail");
    }
    write!(&mut key, "\0{}:", mk_key.len()).expect("write to String cannot fail");
    for byte in mk_key {
        write!(&mut key, "{byte:02x}").expect("write to String cannot fail");
    }
    key
}

impl DeleteBarrier {
    /// The common order is source time, counter, writer, mutation, and kind.
    /// Delete wins an exact tie with Put. Displaced atom is history metadata.
    pub(crate) fn order_key(&self) -> (u64, u64, &str, &str, u8) {
        (
            self.written_at,
            self.logical_counter,
            &self.device_id,
            &self.mutation_uuid,
            1,
        )
    }

    pub(crate) fn blocks_tip(&self, tip: &AtomEntry) -> bool {
        self.order_key() >= tip_order_key(tip)
    }

    pub(crate) fn blocks_resident_tip(&self, tip: &crate::resident::ResidentTip) -> bool {
        let writer = if tip.device_id.is_empty() {
            tip.writer_pubkey.as_str()
        } else {
            tip.device_id.as_str()
        };
        self.order_key()
            >= (
                tip.written_at,
                tip.logical_counter,
                writer,
                tip.mutation_uuid.as_str(),
                0,
            )
    }

    pub(crate) fn is_newer_than(&self, other: &Self) -> bool {
        self.order_key() > other.order_key()
            || (self.order_key() == other.order_key()
                && self.cloud_sequence.unwrap_or(0) > other.cloud_sequence.unwrap_or(0))
    }

    pub(crate) fn matches_key(&self, mk_key: &[u8]) -> bool {
        self.mk_key.as_bytes() == mk_key
    }
}

pub(crate) fn tip_order_key(tip: &AtomEntry) -> (u64, u64, &str, &str, u8) {
    (
        tip.written_at,
        tip.logical_counter,
        tip.lww_device(),
        &tip.mutation_uuid,
        0,
    )
}
