//! Persist slots, turns and revisions.

use super::*;

impl ResidentGraph {
    /// Per-key controls retained by active tips, persist turns, or observers.
    pub fn revision_slot_count(&self) -> usize {
        self.slots.read().expect("resident slot lock").len()
    }

    pub fn revision_observer_count(&self) -> usize {
        self.slot_readers
            .lock()
            .expect("resident slot reader lock")
            .values()
            .sum()
    }

    pub(super) fn next_revision(&self) -> u64 {
        self.next_slot_revision
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |revision| revision.checked_add(1),
            )
            .expect("resident slot revision identity exhausted")
    }

    /// Call only with the observer registry locked and no observer for `key`.
    pub(super) fn reclaim_idle_slot(
        &self,
        slots: &mut HashMap<String, ResidentSlotControl>,
        key: &str,
    ) {
        if slots.get(key).is_some_and(|control| {
            control.state == ResidentSlotState::Absent
                && control.resident_revision == control.durable_revision
        }) {
            slots.remove(key);
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

    pub(super) fn bump_resident_revision(
        &self,
        slots: &mut HashMap<String, ResidentSlotControl>,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) {
        let k = tip_map_key(molecule_uuid, hash, range);
        self.invalidate_partition(molecule_uuid, hash);
        let control = slots.entry(k).or_default();
        control.resident_revision = self.next_revision();
        control.state = ResidentSlotState::Ready;
    }
}
