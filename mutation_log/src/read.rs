use crate::{
    auth::can_read, DatabaseKind, LogError, LogState, NamespaceKind, NamespacePolicy, PrincipalId,
    SnapshotRef, Stamp, StoredEntry, Writer, LIST_PAGE_LIMIT,
};

/// Head a reader is allowed to see.
///
/// `cursor` is the log head's delivery sequence. `stamp` is the last entry
/// this reader can see, and it is absent when every stored entry is hidden.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedHead {
    /// Log head delivery sequence. Hidden entries do not move it backward.
    pub cursor: u64,
    /// Stamp of the last visible entry. Absent when this reader can see none.
    pub stamp: Option<Stamp>,
}

/// Snapshot the caller downloads before applying the still-stored suffix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reset {
    /// Shell pointer to the snapshot bytes.
    pub snapshot_ref: SnapshotRef,
    /// Last delivery sequence the snapshot includes.
    pub through: u64,
    /// When the shell cut the snapshot.
    pub at_ms: u64,
}

/// Still-stored entries a device needs after its position falls below the floor.
///
/// The caller downloads `reset` when it is set, applies `entries`, persists
/// that, then acks `next`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovery {
    /// Snapshot to install when the caller's position is below the trim floor.
    pub reset: Option<Reset>,
    /// Visible entries still stored after the caller's position.
    pub entries: Vec<StoredEntry>,
    /// Scan cursor to ack after the reset and `entries` are applied.
    pub next: u64,
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
        left.merge_stamp()
            .cmp(&right.merge_stamp())
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

    /// Entries still stored after the lesser of the local cursor and the stored ack.
    ///
    /// A reinstall that reuses a device id can have a local cursor behind the
    /// ack this log stored. Delivery starts at that lesser position. A start
    /// below the trim floor returns a reset naming the snapshot ref.
    pub fn recover(
        &self,
        reader: &Writer,
        local_cursor: u64,
        now_ms: u64,
        policy: &impl NamespacePolicy,
    ) -> Result<Recovery, LogError> {
        if !self.reader_allowed(&reader.key) {
            return Err(LogError::Unauthorized);
        }
        let floor = self.effective_floor(now_ms);
        let server_ack = self.acks.get(reader).copied().unwrap_or(0);
        let start = local_cursor.min(server_ack);
        let page = recover_page(self, &reader.key, start, policy);
        Ok(Recovery {
            reset: reset_for(self, start, floor)?,
            next: recovery_cursor(&page, floor),
            entries: page.entries,
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
        self.list_after_floor(reader, after, limit, self.effective_floor(now_ms), policy)
    }

    /// List after the caller's position using the last persisted trim boundary.
    pub fn list_retained(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        policy: &impl NamespacePolicy,
    ) -> Result<ListPage, LogError> {
        self.list_after_floor(reader, after, limit, self.trimmed_through, policy)
    }

    fn list_after_floor(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        floor: u64,
        policy: &impl NamespacePolicy,
    ) -> Result<ListPage, LogError> {
        if limit == 0 || limit > LIST_PAGE_LIMIT {
            return Err(LogError::InvalidPage);
        }
        if !self.reader_allowed(reader) {
            return Err(LogError::Unauthorized);
        }
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
    let cursor = state.head.as_ref().map(|head| head.delivery_seq)?;
    let mut stamp = None;
    for stored in &state.entries {
        if entry_visible(state, stored, reader, policy) {
            stamp = Some(stored.merge_stamp());
        }
    }
    Some(ScopedHead { cursor, stamp })
}

fn reset_for(state: &LogState, start: u64, floor: u64) -> Result<Option<Reset>, LogError> {
    if start >= floor {
        return Ok(None);
    }
    let cut = state.snapshot.as_ref().ok_or(LogError::SnapshotRequired)?;
    Ok(Some(Reset {
        snapshot_ref: cut.snapshot_ref.clone(),
        through: cut.through_delivery_seq,
        at_ms: cut.at_ms,
    }))
}

fn recovery_cursor(page: &ListPage, floor: u64) -> u64 {
    if page.entries.len() == LIST_PAGE_LIMIT {
        page.next
    } else {
        page.next.max(floor)
    }
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
