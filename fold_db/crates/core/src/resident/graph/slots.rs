//! Persist slots, turns and revisions.

use super::*;

impl ResidentGraph {
    /// Full slot identity for this graph's storage prefix.
    pub fn slot_id(&self, molecule_uuid: &str, hash: &str, range: &str) -> ResidentSlotId {
        ResidentSlotId {
            storage_prefix: self.storage_prefix.clone(),
            molecule_uuid: molecule_uuid.to_string(),
            disk_hash: hash.to_string(),
            disk_range: range.to_string(),
        }
    }

    /// Control state for one slot. Missing entry is [`ResidentSlotState::Absent`].
    pub fn slot_control(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> ResidentSlotControl {
        let k = tip_map_key(molecule_uuid, hash, range);
        self.slots
            .read()
            .expect("resident slot lock")
            .get(&k)
            .copied()
            .unwrap_or(ResidentSlotControl {
                state: ResidentSlotState::Absent,
                resident_revision: 0,
                durable_revision: 0,
            })
    }

    pub fn slot_resident_revision(&self, molecule_uuid: &str, hash: &str, range: &str) -> u64 {
        self.slot_control(molecule_uuid, hash, range)
            .resident_revision
    }

    /// Wait until every ticket's captured durable revision is current.
    ///
    /// A ticket target can exceed its predecessor by more than one. One
    /// envelope can apply several resident changes to the same physical slot.
    pub async fn wait_for_persist_turn(
        &self,
        tickets: &[PersistSlotRevision],
    ) -> Result<(), String> {
        let turns = Self::persist_turns(tickets)?;
        loop {
            // Register before the state check so a completion between the
            // check and await cannot lose its notification.
            let notified = self.persist_turn_changed.notified();
            let mut waiting = false;
            {
                let slots = self.slots.read().expect("resident slot lock");
                for (key, (predecessor, target)) in &turns {
                    let Some(control) = slots.get(key) else {
                        return Err(format!("persist turn names an absent slot {key:?}"));
                    };
                    if control.resident_revision < *target {
                        return Err(format!(
                            "persist turn target {target} exceeds resident revision {} for slot {key:?}",
                            control.resident_revision
                        ));
                    }
                    if control.durable_revision > *predecessor {
                        return Err(format!(
                            "persist turn predecessor {predecessor} already passed at durable revision {} for slot {key:?}",
                            control.durable_revision
                        ));
                    }
                    waiting |= control.durable_revision < *predecessor;
                }
            }
            if !waiting {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Advance every slot only from the ticket's exact predecessor revision.
    ///
    /// The check and all updates use one write lock. A rejected ticket set
    /// cannot advance a subset of its slots.
    pub(crate) fn complete_persist_turn(
        &self,
        tickets: &[PersistSlotRevision],
    ) -> Result<(), String> {
        let turns = Self::persist_turns(tickets)?;
        let mut slots = self.slots.write().expect("resident slot lock");
        for (key, (predecessor, target)) in &turns {
            let Some(control) = slots.get(key) else {
                return Err(format!("persist turn names an absent slot {key:?}"));
            };
            if control.resident_revision < *target {
                return Err(format!(
                    "persist turn target {target} exceeds resident revision {} for slot {key:?}",
                    control.resident_revision
                ));
            }
            if control.durable_revision != *predecessor {
                return Err(format!(
                    "persist turn requires durable revision {predecessor}, found {} for slot {key:?}",
                    control.durable_revision
                ));
            }
        }
        for (key, (_, target)) in &turns {
            slots
                .get_mut(key)
                .expect("persist turn slot validated under the same lock")
                .durable_revision = *target;
        }
        drop(slots);
        if !turns.is_empty() {
            self.persist_turn_changed.notify_waiters();
        }
        Ok(())
    }

    pub(super) fn persist_turns(
        tickets: &[PersistSlotRevision],
    ) -> Result<HashMap<String, (u64, u64)>, String> {
        let mut turns = HashMap::with_capacity(tickets.len());
        for ticket in tickets {
            if ticket.resident_revision < ticket.durable_revision {
                return Err(format!(
                    "persist turn target {} is below predecessor {} for slot ({:?}, {:?}, {:?})",
                    ticket.resident_revision,
                    ticket.durable_revision,
                    ticket.molecule_uuid,
                    ticket.disk_hash,
                    ticket.disk_range
                ));
            }
            let key = tip_map_key(&ticket.molecule_uuid, &ticket.disk_hash, &ticket.disk_range);
            let revisions = (ticket.durable_revision, ticket.resident_revision);
            if let Some(existing) = turns.insert(key, revisions) {
                if existing != revisions {
                    return Err(format!(
                        "persist turn has conflicting revisions for slot ({:?}, {:?}, {:?})",
                        ticket.molecule_uuid, ticket.disk_hash, ticket.disk_range
                    ));
                }
            }
        }
        Ok(turns)
    }

    pub(super) fn bump_resident_revision(&self, molecule_uuid: &str, hash: &str, range: &str) {
        let k = tip_map_key(molecule_uuid, hash, range);
        let mut slots = self.slots.write().expect("resident slot lock");
        self.invalidate_partition(molecule_uuid, hash);
        let control = slots.entry(k).or_default();
        control.resident_revision = control.resident_revision.saturating_add(1);
        control.state = ResidentSlotState::Ready;
    }

    /// Register one cold-read leader and capture its revision in one lock cut.
    ///
    /// Purge and eviction use the same flight-to-slot lock order. They cannot
    /// invalidate a slot between leader admission and this revision snapshot.
    pub(crate) fn join_rehydrate_flight_with_snapshot(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> (String, Arc<RehydrateFlightCell>, bool, Option<(u64, u64)>) {
        let key = tip_map_key(molecule_uuid, hash, range);
        let mut flights = self
            .rehydrate_flights
            .lock()
            .expect("resident rehydrate flight lock");
        if let Some(existing) = flights.get(&key) {
            return (key, existing.clone(), false, None);
        }
        let cell = Arc::new(RehydrateFlightCell::new());
        flights.insert(key.clone(), cell.clone());
        let mut slots = self.slots.write().expect("resident slot lock");
        let control = slots.entry(key.clone()).or_default();
        if control.state == ResidentSlotState::Absent {
            control.state = ResidentSlotState::RehydrateFlight;
        }
        let observed = (control.resident_revision, control.durable_revision);
        (key, cell, true, Some(observed))
    }

    pub(crate) fn clear_rehydrate_flight(&self, key: &str, cell: &Arc<RehydrateFlightCell>) {
        let readers = self.slot_readers.lock().expect("resident slot reader lock");
        let mut flights = self
            .rehydrate_flights
            .lock()
            .expect("resident rehydrate flight lock");
        if flights
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, cell))
        {
            flights.remove(key);
            let mut slots = self.slots.write().expect("resident slot lock");
            if !readers.contains_key(key)
                && slots
                    .get(key)
                    .is_some_and(|control| control.state != ResidentSlotState::Ready)
            {
                slots.remove(key);
            }
        }
    }
}
