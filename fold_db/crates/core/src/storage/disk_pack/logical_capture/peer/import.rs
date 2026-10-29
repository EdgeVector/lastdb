//! A verified token can complete only the exact private batch that admitted it.

use super::*;

pub(crate) struct PreparedLogicalImport {
    runtime: Arc<PackRuntime>,
    pending: Arc<transaction::PendingBatch>,
    before_roster: Roster,
    before_peer: PeerPosition,
    after_roster: Roster,
    receipt: ImportReceipt,
    events: Vec<CaptureEvent>,
}

impl PreparedLogicalImport {
    /// Duplicate events never reach the logical data writer.
    pub(crate) fn events(&self) -> &[CaptureEvent] {
        &self.events
    }
}

impl PackRuntime {
    pub(crate) async fn begin_logical_import(
        self: &Arc<Self>,
        mut events: Vec<CaptureEvent>,
    ) -> StorageResult<PreparedLogicalImport> {
        let own_origin = enabled_origin(self)?;
        let pending = transaction::PendingBatch::current(self)
            .ok_or_else(|| invalid("logical peer import needs the data's native transaction"))?;
        let origin = verify_page(&events)?;
        if origin == own_origin {
            return Err(invalid(
                "logical peer import refuses this device's own origin",
            ));
        }
        let roster = state::roster(self).await?;
        let peer = state::peer(self, &roster, &origin)
            .await?
            .ok_or_else(|| invalid("logical peer origin needs explicit owner enrollment"))?;
        let duplicates = duplicate_prefix(self, &peer, &events).await?;
        let fresh = events.len() - duplicates;
        let mut after_roster = roster.clone();
        // Reserve the global metadata budget before the core stages any data.
        after_roster.reserve_receipts(fresh)?;
        if let Some(first) = events.get(duplicates) {
            if peer.position.sequence.checked_add(1) != Some(first.sequence)
                || first.previous_sha256 != peer.position.sha256
            {
                return Err(invalid("logical peer import has a gap or a fork"));
            }
        }
        drop(events.drain(..duplicates));
        let receipt = ImportReceipt {
            position: events
                .last()
                .map_or_else(|| peer.position.clone(), CaptureEvent::position),
            imported_events: fresh,
            duplicate_events: duplicates,
            imported_outcomes: events.iter().map(|event| event.outcomes.len()).sum(),
            receipt_count: after_roster.receipt_count,
        };
        Ok(PreparedLogicalImport {
            runtime: Arc::clone(self),
            pending,
            before_roster: roster,
            before_peer: peer,
            after_roster,
            receipt,
            events,
        })
    }

    pub(crate) async fn stage_logical_import(
        self: &Arc<Self>,
        prepared: PreparedLogicalImport,
    ) -> StorageResult<ImportReceipt> {
        let pending = transaction::PendingBatch::current(self)
            .ok_or_else(|| invalid("logical peer import lost its native transaction"))?;
        if !Arc::ptr_eq(self, &prepared.runtime) || !Arc::ptr_eq(&pending, &prepared.pending) {
            return Err(invalid(
                "logical peer import token belongs to another batch or home",
            ));
        }
        let roster = state::roster(self).await?;
        let origin = &prepared.before_peer.position.origin;
        let peer = state::peer(self, &roster, origin).await?;
        if roster != prepared.before_roster || peer.as_ref() != Some(&prepared.before_peer) {
            return Err(invalid(
                "logical peer import position changed after admission",
            ));
        }
        if prepared.events.is_empty() {
            return Ok(prepared.receipt);
        }
        let mut operations = Vec::with_capacity(prepared.events.len() + 2);
        for event in &prepared.events {
            operations.push(operation(
                receipt_key(origin, event.sequence),
                unhex::<32>(&event.sha256)?.to_vec(),
            ));
        }
        operations.push(operation(roster_key(), encode(&prepared.after_roster)?));
        let peer = PeerPosition {
            anchor: prepared.before_peer.anchor,
            position: prepared.receipt.position.clone(),
        };
        operations.push(operation(peer_key(origin), encode(&peer)?));
        pending.stage(operations)?;
        // The original signed outcomes and this cursor share one native commit.
        // Do not create a local authored event for imported data.
        transaction::mark_logical_capture(self)?;
        Ok(prepared.receipt)
    }
}

fn verify_page(events: &[CaptureEvent]) -> StorageResult<String> {
    if events.is_empty() || events.len() > PAGE_EVENTS {
        return Err(StorageError::MutationLimit {
            detail: "logical peer import needs 1..=4 events".into(),
        });
    }
    bounded::serialized_size_with_limit(events, MAX_EVENT_BYTES)?;
    let origin = &events[0].origin;
    for event in events {
        event.verify()?;
        if &event.origin != origin {
            return Err(invalid("logical peer import page needs one origin"));
        }
    }
    for pair in events.windows(2) {
        if pair[0].sequence.checked_add(1) != Some(pair[1].sequence)
            || pair[1].previous_sha256 != pair[0].sha256
        {
            return Err(invalid("logical peer import page has a gap or a fork"));
        }
    }
    Ok(origin.clone())
}

async fn duplicate_prefix(
    runtime: &Arc<PackRuntime>,
    peer: &PeerPosition,
    events: &[CaptureEvent],
) -> StorageResult<usize> {
    let duplicates = events
        .iter()
        .take_while(|event| event.sequence <= peer.position.sequence)
        .count();
    let keys = events[..duplicates]
        .iter()
        .map(|event| receipt_key(&event.origin, event.sequence))
        .collect();
    let receipts = state::values(runtime, keys).await?;
    for (event, bytes) in events[..duplicates].iter().zip(receipts) {
        let expected = unhex::<32>(&event.sha256)?;
        if bytes.as_deref() != Some(expected.as_slice()) {
            return Err(invalid(
                "logical peer duplicate differs from its durable digest receipt",
            ));
        }
    }
    Ok(duplicates)
}
