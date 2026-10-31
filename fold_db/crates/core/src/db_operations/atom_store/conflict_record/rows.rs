//! The rows that one recorded conflict writes. Every row is small: the
//! history row and the conflict row hold atom ids and orders, never an atom body.

use super::TipConflict;
use crate::atom::{
    molecule_key_codec, AtomEntry, MutationEvent, MutationEventKind, SourceMutationOrder,
    SuppressedByTip, TipConflictKind,
};
use crate::schema::types::field::build_storage_key;
use crate::SyncConflict;
use chrono::{DateTime, Utc};

impl TipConflict {
    /// Origin time of the incoming write, in the signed range of a history key.
    fn incoming_nanos(&self) -> i64 {
        i64::try_from(self.incoming().written_at).unwrap_or(i64::MAX)
    }

    /// `history\0{molecule}:{ts}:conflict:{digest}`. The digest makes the key
    /// idempotent. The `:conflict:` tag keeps the row apart from the
    /// `:suppressed:` rows of the Delete barrier.
    pub(super) fn history_key(&self, storage_prefix: Option<&str>) -> String {
        let base = format!(
            "{}:conflict:{}",
            molecule_key_codec::history_event_key(&self.molecule_uuid, self.incoming_nanos()),
            self.digest().hex()
        );
        build_storage_key(storage_prefix, &base)
    }

    /// The conflict id `{molecule}:{stamp}`. The stamp is 20 digits, as the
    /// bounded conflict reader requires. It is the first 64 bits of the digest.
    pub(super) fn conflict_id(&self) -> String {
        format!("{}:{:020}", self.molecule_uuid, self.digest().stamp())
    }

    /// The `conflict\0{id}` key of the conflict row.
    pub(super) fn conflict_key(&self, storage_prefix: Option<&str>) -> String {
        build_storage_key(
            storage_prefix,
            &lastdb_model::kind_partition::anchored("conflict", &self.conflict_id()),
        )
    }

    /// A replaced tip is a `Transition` event, like the legacy event. A
    /// dropped write is a `SuppressedPut` event with the tip that beat it.
    pub(super) fn event(&self) -> MutationEvent {
        let timestamp = DateTime::<Utc>::from_timestamp_nanos(self.incoming_nanos());
        let mut event = MutationEvent {
            molecule_uuid: self.molecule_uuid.clone(),
            timestamp,
            field_key: self.field_key.clone(),
            old_atom_uuid: None,
            new_atom_uuid: self.loser.atom_uuid.clone(),
            kind: MutationEventKind::SuppressedPut,
            version: 0,
            is_conflict: true,
            conflict_loser_atom: None,
            writer_pubkey: String::new(),
            signature: String::new(),
            provenance: None,
            source_order: None,
            suppressed_by_delete: None,
            suppressed_by_tip: None,
        };
        match self.kind {
            TipConflictKind::Replaced => {
                event.old_atom_uuid = Some(self.loser.atom_uuid.clone());
                event.new_atom_uuid = self.winner.atom_uuid.clone();
                event.kind = MutationEventKind::Transition;
                event.conflict_loser_atom = Some(self.loser.atom_uuid.clone());
            }
            TipConflictKind::Dropped => {
                event.source_order = Some(source_order(&self.loser));
                event.suppressed_by_tip = Some(suppressed_by(&self.winner));
            }
        }
        event
    }

    /// The row that the conflict API reads. For a dropped write the winner is
    /// the tip and the loser is the dropped write.
    pub(super) fn sync_conflict(&self, detected_at: DateTime<Utc>) -> SyncConflict {
        SyncConflict {
            id: self.conflict_id(),
            molecule_uuid: self.molecule_uuid.clone(),
            conflict_key: crate::atom::MergeConflict::display_key(&self.field_key),
            winner_atom: self.winner.atom_uuid.clone(),
            loser_atom: self.loser.atom_uuid.clone(),
            winner_written_at: self.winner.written_at,
            loser_written_at: self.loser.written_at,
            detected_at,
            resolved: false,
        }
    }
}

fn source_order(entry: &AtomEntry) -> SourceMutationOrder {
    SourceMutationOrder {
        written_at: entry.written_at,
        logical_counter: entry.logical_counter,
        device_id: entry.lww_device().to_string(),
        mutation_uuid: entry.mutation_uuid.clone(),
        origin_device: entry.origin_device.clone(),
    }
}

fn suppressed_by(entry: &AtomEntry) -> SuppressedByTip {
    SuppressedByTip {
        atom_uuid: entry.atom_uuid.clone(),
        written_at: entry.written_at,
        logical_counter: entry.logical_counter,
        device_id: entry.lww_device().to_string(),
        mutation_uuid: entry.mutation_uuid.clone(),
        origin_device: entry.origin_device.clone(),
    }
}

/// True when `key` is a history row that a recorder wrote as a counted record:
/// `history\0{molecule}:{ts}:conflict:{digest}` from the tip-conflict recorder
/// or `history\0{molecule}:{ts}:suppressed:{digest}` from a Put that lost to a
/// Delete. Each such row holds one reference to every atom it names. A history
/// row of another shape holds none.
pub(crate) fn is_counted_record_key(key: &str) -> bool {
    let Some(rest) = lastdb_model::kind_partition::rest_of(key, "history") else {
        return false;
    };
    let mut parts = rest.rsplitn(3, ':');
    let (_digest, tag) = (parts.next(), parts.next());
    matches!(tag, Some("conflict" | "suppressed" | "archive")) && parts.next().is_some()
}
