use super::*;

pub(crate) fn apply_durable_through(store: &LastStore, set: &Mutex<LogicalResidentSet>) {
    let seal = DurabilityToken::new(store.durable_through().as_u64());
    crate::read_receipts::lock(set).apply_durable_through(seal);
    publish_loader_measurements(store, set);
}

/// Count one point read the set served from memory.
pub(in super::super) fn note_point_hit(guard: &LogicalResidentSet) {
    laststore::record_logical_warm_hits(1);
    if let Some(metrics) = guard.metrics() {
        metrics.record_point_hit();
    }
}

/// Run one loader call and record it: `misses` point misses, one load, and
/// its wall-clock cost. The set lock is not held while `load` runs.
pub(in super::super) fn timed_load<T>(
    set: &Mutex<LogicalResidentSet>,
    misses: u64,
    load: impl FnOnce() -> T,
) -> T {
    laststore::record_logical_warm_misses(misses);
    let metrics = crate::read_receipts::lock(set).metrics().cloned();
    let started = std::time::Instant::now();
    let out = load();
    if let Some(metrics) = metrics {
        for _ in 0..misses {
            metrics.record_point_miss();
        }
        metrics.record_loader_load(started.elapsed());
    }
    out
}

/// Publish pin occupancy. Call after a pin opens, a pin is reaped, and flush.
pub(super) fn publish_loader_measurements(store: &LastStore, set: &Mutex<LogicalResidentSet>) {
    let metrics = crate::read_receipts::lock(set).metrics().cloned();
    let Some(metrics) = metrics else {
        return;
    };
    metrics.set_loader_measurements(store.loader_pin_bytes(), store.loader_groups_open_now());
}

/// Hit the warm set with plaintext already admitted (encrypting layer).
pub(crate) fn lookup_plaintext(set: &Mutex<LogicalResidentSet>, key: &[u8]) -> ResidentLookup {
    let id = LastStoreKvStore::encode_key(key);
    let mut guard = crate::read_receipts::lock(set);
    if let Some(coords) = parse_tip_coords(&id) {
        if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range) {
            return ResidentLookup::Absent;
        }
        if let Some(body) = guard
            .tip_body(coords.molecule, &coords.hash, &coords.range)
            .map(ToOwned::to_owned)
        {
            guard.hold_tip(coords.molecule, &coords.hash, &coords.range);
            guard.release_tip(coords.molecule, &coords.hash, &coords.range);
            note_point_hit(&guard);
            return ResidentLookup::Hit(body);
        }
        return ResidentLookup::Miss;
    }
    if let Some(atom) = parse_atom_id(&id) {
        if let Some(body) = guard.atom_body(&atom).map(ToOwned::to_owned) {
            guard.touch(crate::resident::ResidentKey::Atom(atom));
            note_point_hit(&guard);
            return ResidentLookup::Hit(body);
        }
    }
    ResidentLookup::Miss
}

/// Digest of the bytes one append stored. The encrypting layer passes the
/// digest of its sealed bytes so that only the put that wrote them admits
/// its plaintext.
pub(crate) fn body_digest(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Admit a body that a read decrypted. A read never replaces a resident
/// body: a write admitted that body, and it is at least as new as the read.
pub(crate) fn admit_plaintext(set: &Mutex<LogicalResidentSet>, key: &[u8], value: &[u8]) {
    let id = LastStoreKvStore::encode_key(key);
    let mut guard = crate::read_receipts::lock(set);
    if let Some(coords) = parse_tip_coords(&id) {
        if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range)
            || guard
                .tip_body(coords.molecule, &coords.hash, &coords.range)
                .is_some()
        {
            return;
        }
    }
    if let Some(atom) = parse_atom_id(&id) {
        if guard.atom_body(&atom).is_some() {
            return;
        }
    }
    store_warm_body(&mut guard, &id, value, None);
}

/// Admit the plaintext of a put whose stored bytes have digest
/// `stored_digest`. Only the newest append for the key admits; an older put
/// that finishes last stores nothing.
pub(crate) fn admit_written_plaintext(
    set: &Mutex<LogicalResidentSet>,
    key: &[u8],
    stored_digest: u64,
    value: &[u8],
) {
    let id = LastStoreKvStore::encode_key(key);
    let mut guard = crate::read_receipts::lock(set);
    let Some(token) = guard.take_write_token_for(key, stored_digest) else {
        return;
    };
    if superseded_tip(&guard, &id, token) {
        return;
    }
    store_warm_body(&mut guard, &id, value, Some(token));
}
