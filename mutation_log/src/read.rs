use crate::{
    auth::can_read, DatabaseKind, LogError, LogState, NamespaceKind, NamespacePolicy, PrincipalId,
    Snapshot, Stamp, StoredEntry, Writer, LIST_PAGE_LIMIT,
};

/// Head a reader is allowed to see.
///
/// `cursor` is the last delivery sequence scanned, including hidden entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedHead {
    /// Last delivery sequence scanned, including entries this reader cannot see.
    pub cursor: u64,
    /// Stamp of the last visible entry.
    pub stamp: Stamp,
}

/// Still-stored entries a device needs after its cursor expires.
///
/// The caller installs `snapshot` when `reset` is set, then applies `entries`.
/// `next` is the scan cursor to ack after both are applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovery {
    /// Snapshot the caller uses to replace its state.
    pub snapshot: Snapshot,
    /// Visible entries still stored and not yet acked by this device.
    pub entries: Vec<StoredEntry>,
    /// Scan cursor covering `entries`, the snapshot, and hidden entries.
    pub next: u64,
    /// Whether this device has not yet been given `snapshot`.
    pub reset: bool,
}

/// One page of entries the reader may see, in delivery order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListPage {
    /// Visible entries, oldest first.
    pub entries: Vec<StoredEntry>,
    /// Last delivery sequence scanned, including entries this reader cannot see.
    ///
    /// The next call passes this value as `after`, so hidden entries are covered.
    /// Unchanged when nothing after `after` was scanned.
    pub next: u64,
}

/// Entries `reader` may see. Hidden entries are omitted, not redacted in place.
///
/// A share receiver gets entries whose writer key is the publisher key and whose
/// namespace is official. A pending submission is returned only to its author
/// key and to the publisher key. An entry from another database is omitted.
/// An app-private namespace on a shareable database is omitted for every reader.
pub fn filter_readable<'a>(
    entries: &'a [StoredEntry],
    reader: &PrincipalId,
    database: &LogState,
    policy: &impl NamespacePolicy,
) -> Vec<&'a StoredEntry> {
    entries
        .iter()
        .filter(|stored| entry_visible(database, stored, reader, policy))
        .collect()
}

/// Merge order. Delivery order stays in [`StoredEntry::delivery_seq`].
///
/// Equal stamps break ties by delivery sequence so the order is total.
#[must_use]
pub fn merge_order(entries: &[StoredEntry]) -> Vec<&StoredEntry> {
    let mut ordered: Vec<&StoredEntry> = entries.iter().collect();
    ordered.sort_by(|left, right| {
        left.entry
            .stamp()
            .cmp(&right.entry.stamp())
            .then(left.delivery_seq.cmp(&right.delivery_seq))
    });
    ordered
}

/// Whether one stored entry is visible to `reader` in `database`.
#[must_use]
pub fn entry_visible(
    database: &LogState,
    stored: &StoredEntry,
    reader: &PrincipalId,
    policy: &impl NamespacePolicy,
) -> bool {
    if stored.entry.database_id != database.database_id {
        return false;
    }
    if !can_read(reader, database, &stored.entry, policy) {
        return false;
    }
    shareable_namespace_allowed(database, stored, policy)
}

fn shareable_namespace_allowed(
    database: &LogState,
    stored: &StoredEntry,
    policy: &impl NamespacePolicy,
) -> bool {
    if !matches!(database.kind, DatabaseKind::Shareable { .. }) {
        return true;
    }
    policy.classify(&stored.entry.namespace) == NamespaceKind::Official
}

impl LogState {
    /// Last visible entry, with the cursor set to the last delivery scanned.
    pub fn head_for(
        &self,
        reader: &PrincipalId,
        policy: &impl NamespacePolicy,
    ) -> Result<Option<ScopedHead>, LogError> {
        if !self.reader_allowed(reader) {
            return Err(LogError::Unauthorized);
        }
        Ok(scoped_head(self, reader, policy))
    }

    /// Entries still stored for a device whose cursor is below the trim floor.
    ///
    /// `entries` includes every visible record this device has not acked, both
    /// the ones at or below the snapshot and the ones after it. `reset` is
    /// set until the device has been given the snapshot. `next` does not move
    /// past an entry this page did not scan.
    pub fn recover(
        &self,
        reader: &Writer,
        now_ms: u64,
        policy: &impl NamespacePolicy,
    ) -> Result<Recovery, LogError> {
        if !self.reader_allowed(&reader.key) {
            return Err(LogError::Unauthorized);
        }
        let floor = self.effective_floor(now_ms);
        let snapshot = self.snapshot.clone().unwrap_or(Snapshot {
            through_delivery_seq: 0,
            at_ms: 0,
        });
        let device_ack = self.acks.get(reader).copied().unwrap_or(0);
        let page = recover_page(self, &reader.key, device_ack, policy);
        let next = page
            .next
            .max(snapshot.through_delivery_seq)
            .max(device_ack)
            .max(floor);
        Ok(Recovery {
            reset: device_ack < snapshot.through_delivery_seq,
            snapshot,
            entries: page.entries,
            next,
        })
    }

    /// List visible entries after `after`. A cursor below the trim floor expires.
    pub fn list(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        now_ms: u64,
        policy: &impl NamespacePolicy,
    ) -> Result<ListPage, LogError> {
        if limit == 0 || limit > LIST_PAGE_LIMIT {
            return Err(LogError::InvalidPage);
        }
        if !self.reader_allowed(reader) {
            return Err(LogError::Unauthorized);
        }
        let floor = self.effective_floor(now_ms);
        if after < floor {
            return Err(LogError::CursorExpired {
                snapshot_through: self
                    .snapshot
                    .as_ref()
                    .map_or(0, |cut| cut.through_delivery_seq),
            });
        }
        Ok(page(self, reader, after, limit, policy))
    }
}

fn scoped_head(
    state: &LogState,
    reader: &PrincipalId,
    policy: &impl NamespacePolicy,
) -> Option<ScopedHead> {
    let mut cursor = 0;
    let mut stamp = None;
    for stored in &state.entries {
        cursor = stored.delivery_seq;
        if entry_visible(state, stored, reader, policy) {
            stamp = Some(stored.entry.stamp());
        }
    }
    stamp.map(|stamp| ScopedHead { cursor, stamp })
}

fn recover_page(
    state: &LogState,
    reader: &PrincipalId,
    device_ack: u64,
    policy: &impl NamespacePolicy,
) -> ListPage {
    let mut entries = Vec::new();
    let mut next = device_ack;
    for stored in &state.entries {
        if stored.delivery_seq <= device_ack {
            continue;
        }
        next = stored.delivery_seq;
        if !entry_visible(state, stored, reader, policy) {
            continue;
        }
        entries.push(stored.clone());
        if entries.len() == LIST_PAGE_LIMIT {
            break;
        }
    }
    ListPage { entries, next }
}

fn page(
    state: &LogState,
    reader: &PrincipalId,
    after: u64,
    limit: usize,
    policy: &impl NamespacePolicy,
) -> ListPage {
    let mut entries = Vec::new();
    let mut next = after;
    for stored in &state.entries {
        if stored.delivery_seq <= after {
            continue;
        }
        next = stored.delivery_seq;
        if !entry_visible(state, stored, reader, policy) {
            continue;
        }
        entries.push(stored.clone());
        if entries.len() == limit {
            break;
        }
    }
    ListPage { entries, next }
}
