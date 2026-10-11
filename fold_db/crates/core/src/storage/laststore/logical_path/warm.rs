use super::*;

pub(super) enum WarmLookup {
    Miss,
    Absent,
    Hit(Vec<u8>),
}

pub(super) enum WarmExists {
    Miss,
    No,
    Yes,
}

pub(super) fn warm_lookup(
    guard: &mut LogicalResidentSet,
    collection: &str,
    key: &[u8],
) -> WarmLookup {
    let id = LastStoreKvStore::encode_key(key);
    let known = |guard: &mut LogicalResidentSet| match record_hit(guard, collection, &id) {
        Some(Some(body)) => WarmLookup::Hit(body),
        Some(None) => WarmLookup::Absent,
        None => WarmLookup::Miss,
    };
    if let Some(coords) = parse_tip_coords(&id) {
        if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range) {
            return WarmLookup::Absent;
        }
        if let Some(body) = guard
            .tip_body(coords.molecule, &coords.hash, &coords.range)
            .map(ToOwned::to_owned)
        {
            guard.hold_tip(coords.molecule, &coords.hash, &coords.range);
            guard.release_tip(coords.molecule, &coords.hash, &coords.range);
            note_point_hit(guard);
            return WarmLookup::Hit(body);
        }
        return known(guard);
    }
    if let Some(atom) = parse_atom_id(&id) {
        if let Some(body) = guard.atom_body(&atom).map(ToOwned::to_owned) {
            guard.touch(crate::resident::ResidentKey::Atom(atom));
            note_point_hit(guard);
            return WarmLookup::Hit(body);
        }
        return known(guard);
    }
    if is_molecule_header_key(&id) {
        return known(guard);
    }
    if super::super::absence_hints::is_absence_hint_id(&id) && guard.absent_hint(collection, &id) {
        note_point_hit(guard);
        return WarmLookup::Absent;
    }
    WarmLookup::Miss
}

pub(super) fn warm_exists(guard: &LogicalResidentSet, collection: &str, key: &[u8]) -> WarmExists {
    let id = LastStoreKvStore::encode_key(key);
    let known = || match guard.has_fetched_record(collection, &id) {
        Some(true) => WarmExists::Yes,
        Some(false) => WarmExists::No,
        None => WarmExists::Miss,
    };
    if let Some(coords) = parse_tip_coords(&id) {
        if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range) {
            return WarmExists::No;
        }
        if guard
            .tip(coords.molecule, &coords.hash, &coords.range)
            .is_some()
        {
            return WarmExists::Yes;
        }
        return known();
    }
    if let Some(atom) = parse_atom_id(&id) {
        if guard.atom_body(&atom).is_some() {
            return WarmExists::Yes;
        }
        return known();
    }
    if is_molecule_header_key(&id) {
        return known();
    }
    WarmExists::Miss
}

pub(super) fn store_warm_body(
    guard: &mut LogicalResidentSet,
    id: &str,
    value: &[u8],
    token: Option<DurabilityToken>,
) {
    if let Some(coords) = parse_tip_coords(id) {
        if let Ok(tip) = tip_from_body(value, coords.scope.as_deref()) {
            guard.store_tip_body(coords.molecule, &coords.hash, &coords.range, value.to_vec());
            if guard
                .tip(coords.molecule, &coords.hash, &coords.range)
                .is_some()
            {
                guard.hold_tip(coords.molecule, &coords.hash, &coords.range);
            } else {
                guard.admit_tip(
                    coords.molecule,
                    coords.hash.clone(),
                    coords.range.clone(),
                    tip,
                );
            }
            if let Some(token) = token {
                guard.mark_dirty(coords.molecule, &coords.hash, &coords.range, token);
            }
            guard.release_tip(coords.molecule, &coords.hash, &coords.range);
        }
    }
    if let Some(atom) = parse_atom_id(id) {
        guard.admit_atom_body(atom, value.to_vec());
    }
}

pub(super) fn admit_loaded_body(set: &Mutex<LogicalResidentSet>, id: &str, body: &[u8]) {
    if let Some(coords) = parse_tip_coords(id) {
        admit_loaded_tip(set, &coords, body);
        return;
    }
    if let Some(atom) = parse_atom_id(id) {
        if admit_raw_body(body) {
            let mut guard = set.lock().expect("poison");
            if guard.atom_body(&atom).is_some() {
                return;
            }
            guard.admit_atom_body(atom, body.to_vec());
        }
    }
}

/// Copy one returned key into the warm set. Ciphertext and a body that is
/// not a tip stay out. The caller still receives those stored bytes.
pub(super) fn admit_returned_tip(
    set: &mut LogicalResidentSet,
    coords: &HashCoords,
    range: &str,
    body: &[u8],
) {
    if !admit_raw_body(body) {
        return;
    }
    let Ok(tip) = tip_from_body(body, coords.scope.as_deref()) else {
        return;
    };
    set.store_tip_body(coords.molecule, &coords.hash, range, body.to_vec());
    if set.tip(coords.molecule, &coords.hash, range).is_some() {
        set.hold_tip(coords.molecule, &coords.hash, range);
    } else {
        set.admit_tip(coords.molecule, coords.hash.clone(), range.to_string(), tip);
    }
    set.release_tip(coords.molecule, &coords.hash, range);
}

pub(super) fn admit_loaded_tip(set: &Mutex<LogicalResidentSet>, coords: &TipCoords, body: &[u8]) {
    if !admit_raw_body(body) {
        return;
    }
    let mut guard = set.lock().expect("poison");
    if guard
        .tip_body(coords.molecule, &coords.hash, &coords.range)
        .is_some()
    {
        return;
    }
    if let Ok(tip) = tip_from_body(body, coords.scope.as_deref()) {
        guard.store_tip_body(coords.molecule, &coords.hash, &coords.range, body.to_vec());
        if guard
            .tip(coords.molecule, &coords.hash, &coords.range)
            .is_some()
        {
            guard.hold_tip(coords.molecule, &coords.hash, &coords.range);
        } else {
            guard.admit_tip(
                coords.molecule,
                coords.hash.clone(),
                coords.range.clone(),
                tip,
            );
        }
        guard.release_tip(coords.molecule, &coords.hash, &coords.range);
    }
}

pub(super) fn admit_raw_body(body: &[u8]) -> bool {
    !is_sealed_at_rest(body)
}
