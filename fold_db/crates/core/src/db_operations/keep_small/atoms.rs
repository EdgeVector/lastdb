//! Atom-level meter updates: puts, replaces, deletes and pending-atom bookkeeping.

use super::*;

impl KeepSmallMeters {
    /// Record a stored atom and the logical blob bytes named by its pointer.
    pub fn record_atom_put_with_blob(
        &self,
        schema: &str,
        atom_uuid: &str,
        logical_bytes: u64,
        blob_reference_bytes: u64,
        contribution_complete: bool,
    ) {
        self.atom_bytes.fetch_add(logical_bytes, Ordering::Relaxed);
        self.atom_count.fetch_add(1, Ordering::Relaxed);
        self.remember_atom_contribution(
            schema,
            atom_uuid,
            logical_bytes,
            blob_reference_bytes,
            contribution_complete,
            true,
        );
        self.with_schema(schema, |meter| {
            meter.live_bytes = meter.live_bytes.saturating_add(logical_bytes);
            meter.atom_count = meter.atom_count.saturating_add(1);
            Self::add_appended(meter, logical_bytes);
        });
    }

    /// Refresh a reused immutable atom's tip contribution without counting a
    /// body put, appended bytes, or a new live atom.
    pub(crate) fn remember_atom_contribution(
        &self,
        schema: &str,
        atom_uuid: &str,
        logical_bytes: u64,
        blob_reference_bytes: u64,
        contribution_complete: bool,
        body_put: bool,
    ) {
        if let Ok(mut pending) = self.pending_atoms.lock() {
            pending.insert(
                atom_uuid.to_string(),
                (
                    schema.to_string(),
                    logical_bytes,
                    blob_reference_bytes,
                    contribution_complete,
                    body_put,
                ),
            );
        }
        if !body_put {
            self.mark_reused_atom_aggregate_incomplete(schema);
        }
        if !contribution_complete {
            self.molecule_counters_complete
                .store(false, Ordering::Relaxed);
        }
    }

    /// After [`Self::record_atom_put`] of a replacement body, subtract the
    /// *superseded* live size so live becomes `previous - old + new`.
    ///
    /// The new atom is already in live (and in today's append). Subtracting
    /// the old size (not the new one) is what lets a growing rewrite raise
    /// live and still show high churn (`appended / live`).
    pub fn replace_live_atom(&self, new_uuid: &str, superseded_bytes: u64) {
        let Some((schema, body_put)) = self.pending_atoms.lock().ok().and_then(|pending| {
            pending
                .get(new_uuid)
                .map(|(schema, _, _, _, body_put)| (schema.clone(), *body_put))
        }) else {
            return;
        };
        if !body_put {
            return;
        }
        self.sub_atom_bytes(superseded_bytes);
        let count = self.atom_count.load(Ordering::Relaxed);
        if count > 0 {
            self.atom_count.fetch_sub(1, Ordering::Relaxed);
        }
        self.with_schema(&schema, |meter| {
            meter.live_bytes = meter.live_bytes.saturating_sub(superseded_bytes);
            meter.atom_count = meter.atom_count.saturating_sub(1);
        });
    }

    pub(super) fn sub_atom_bytes(&self, bytes: u64) {
        let cur = self.atom_bytes.load(Ordering::Relaxed);
        self.atom_bytes.fetch_sub(bytes.min(cur), Ordering::Relaxed);
    }

    /// Owning schema of a just-stored atom, without consuming it.
    ///
    /// The tip-accounting path uses this to bind a molecule to its schema.
    #[must_use]
    pub fn pending_atom_schema(&self, atom_uuid: &str) -> Option<String> {
        self.pending_atoms
            .lock()
            .ok()
            .and_then(|m| m.get(atom_uuid).map(|(schema, _, _, _, _)| schema.clone()))
    }

    #[must_use]
    pub fn pending_atom_contribution(&self, atom_uuid: &str) -> Option<(u64, u64, bool)> {
        self.pending_atoms.lock().ok().and_then(|m| {
            m.get(atom_uuid)
                .map(|(_, atom_bytes, blob_bytes, complete, _)| {
                    (*atom_bytes, *blob_bytes, *complete)
                })
        })
    }

    /// Logical contribution of an atom that the current commit wrote or
    /// reused. A commit lists every atom its tips name, so tip accounting
    /// finds each one here.
    #[must_use]
    pub fn known_atom_contribution(&self, atom_uuid: &str) -> Option<(u64, u64)> {
        let (atom, blob, complete) = self.pending_atom_contribution(atom_uuid)?;
        complete.then_some((atom, blob))
    }

    /// Decrement live atom bytes when a **live-head** atom is deleted.
    ///
    /// Only call this for the current tip body. A superseded atom already
    /// left the live gauge on [`Self::replace_live_atom`]; charging it again
    /// would under-report.
    pub fn record_atom_delete(&self, schema: Option<&str>, logical_bytes: u64) {
        self.sub_atom_bytes(logical_bytes);
        let count = self.atom_count.load(Ordering::Relaxed);
        if count > 0 {
            self.atom_count.fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(schema) = schema.filter(|name| !name.is_empty()) {
            self.with_schema(schema, |meter| {
                meter.live_bytes = meter.live_bytes.saturating_sub(logical_bytes);
                meter.atom_count = meter.atom_count.saturating_sub(1);
            });
        } else {
            self.mark_incomplete("erase_atom_source_schema_unknown");
        }
    }

    /// Drop a just-stored atom's pending size so a later replace cannot
    /// subtract it after the body is gone.
    pub fn forget_pending_atom(&self, atom_uuid: &str) {
        let _ = self.take_pending(atom_uuid);
    }

    /// Drop the pending entries of one commit once its tip accounting ran.
    ///
    /// A commit's bodies include every atom its tips point at, written or
    /// reused, so tip accounting reads them from this commit's entries. Later
    /// commits learn old atom sizes from the atom counters. Without this, the
    /// map kept one entry per atom ever stored: measured 2026-10-20, about
    /// two allocations per update that never left, and each burst of writes
    /// held more allocator pages.
    pub(crate) fn settle_pending_atoms<'a>(&self, atom_uuids: impl IntoIterator<Item = &'a str>) {
        if let Ok(mut pending) = self.pending_atoms.lock() {
            for atom_uuid in atom_uuids {
                pending.remove(atom_uuid);
            }
        }
    }
}
