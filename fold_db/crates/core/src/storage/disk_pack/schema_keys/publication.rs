//! Raw stored writes maintain candidate completeness at the logical boundary.
//! Encrypted tip/barrier bytes do not establish visibility; query resolution
//! checks the selected candidates through the ordinary current-tip reader.

use super::*;
use crate::atom::molecule_key_codec::{
    decode_hash_range, molecule_record_key_from_generation_delete_key,
    molecule_record_key_from_generation_key, molecule_record_prefix,
};
use crate::resident::core_keys::KeyVisibility;
use crate::storage::disk_pack::native_routing;

impl NativeWarmSet {
    /// The caller holds the view write lock and publishes the corresponding
    /// values/root before it releases that lock. Optional key capacity never
    /// rejects an otherwise valid accepted write.
    pub(in crate::storage::disk_pack) fn publish_schema_key_changes<'a>(
        &self,
        changes: impl IntoIterator<Item = (&'a str, &'a str, bool)>,
    ) {
        let entries = self
            .schema_keys
            .entries
            .lock()
            .expect("key publication entries");
        if entries.is_empty() {
            return;
        }
        for (collection, encoded, deleted) in changes {
            let Ok(raw) = native_routing::decode(encoded) else {
                continue;
            };
            let Ok(raw) = std::str::from_utf8(&raw) else {
                continue;
            };
            if native_routing::collection_for_key(raw.as_bytes()) != collection {
                continue;
            }
            let Some((id, bare, structural)) = membership_coordinate(raw) else {
                continue;
            };
            let Some(entry) = entries.get(&id) else {
                continue;
            };
            let mut state = entry.state.lock().expect("key publication state");
            if structural {
                self.invalidate_key_entry(entry, &mut state, Phase::Unloaded, "membership_control");
                continue;
            }
            let Some(candidate) = candidate_key(&entry.id.1, bare) else {
                self.invalidate_key_entry(
                    entry,
                    &mut state,
                    Phase::Unloaded,
                    "invalid_membership_key",
                );
                continue;
            };
            let candidate = candidate.as_ref();
            let phase = *entry.phase.borrow();
            // Without an active generation, its rows cannot expose a key.
            // A sparse delete marker must not re-add a physically deleted tip.
            // Publishing a generation pointer invalidates this domain above.
            if phase == Phase::Ready
                && state.has_generation == Some(false)
                && (bare.starts_with("mgr:v1:") || bare.starts_with("mgd:v1:"))
            {
                continue;
            }
            match phase {
                Phase::Loading => self.retain_key_change(entry, &mut state, candidate),
                Phase::Ready if deleted && bare.starts_with("mk:") => {
                    self.remove_key_candidate(entry, &mut state, candidate);
                }
                Phase::Ready => self.insert_key_candidate(entry, &mut state, candidate),
                Phase::Unloaded | Phase::DiskOnly => {}
            }
        }
    }

    fn retain_key_change(&self, entry: &Entry, state: &mut EntryState, key: &str) {
        if state.changes.contains_key(key) {
            return;
        }
        let Some(bytes) = key.len().checked_add(256) else {
            self.invalidate_key_entry(entry, state, Phase::DiskOnly, "load_change_capacity");
            return;
        };
        match self.schema_keys.budget.reserve_temporary(bytes) {
            Ok(charge) => {
                state.changes.insert(key.to_owned(), charge);
            }
            Err(_) => {
                self.invalidate_key_entry(entry, state, Phase::DiskOnly, "load_change_capacity");
            }
        }
    }

    fn insert_key_candidate(&self, entry: &Entry, state: &mut EntryState, key: &str) {
        let Some((hash, range)) = decode_hash_range(key, &entry.id.1) else {
            self.invalidate_key_entry(entry, state, Phase::Unloaded, "invalid_membership_key");
            return;
        };
        let Some(domain) = state.domain.as_ref() else {
            self.invalidate_key_entry(entry, state, Phase::Unloaded, "missing_key_domain");
            return;
        };
        if !supported_coordinate(domain, &hash, &range) {
            self.invalidate_key_entry(entry, state, Phase::DiskOnly, "mixed_membership_encoding");
            return;
        }
        // The body is encrypted. Include this coordinate as a candidate even
        // when its new metadata can hide it; readers check that metadata.
        if domain
            .insert_batch(&[(hash, range, KeyVisibility::default())])
            .is_err()
        {
            self.invalidate_key_entry(entry, state, Phase::DiskOnly, "ready_key_capacity");
        }
    }

    fn remove_key_candidate(&self, entry: &Entry, state: &mut EntryState, key: &str) {
        match state.has_generation {
            // A retained generation can expose the deleted live coordinate.
            // Exact tip resolution decides whether that candidate is visible.
            Some(true) => return,
            None => {
                self.invalidate_key_entry(entry, state, Phase::Unloaded, "physical_tip_delete");
                return;
            }
            Some(false) => {}
        }
        let Some((hash, range)) = decode_hash_range(key, &entry.id.1) else {
            self.invalidate_key_entry(entry, state, Phase::Unloaded, "invalid_membership_key");
            return;
        };
        let Some(domain) = state.domain.as_ref() else {
            self.invalidate_key_entry(entry, state, Phase::Unloaded, "missing_key_domain");
            return;
        };
        if domain.remove(&hash, &range).is_err() {
            self.invalidate_key_entry(entry, state, Phase::DiskOnly, "ready_key_capacity");
        }
    }

    fn invalidate_key_entry(
        &self,
        entry: &Entry,
        state: &mut EntryState,
        phase: Phase,
        reason: &'static str,
    ) {
        let current_phase = *entry.phase.borrow();
        if state.domain.is_none()
            && state.changes.is_empty()
            && current_phase == phase
            && state.reason.as_deref() == Some(reason)
        {
            return;
        }
        if let Some(domain) = state.domain.take() {
            let _ = domain.retire(reason);
        }
        state.changes.clear();
        state.has_generation = None;
        state.reason = Some(reason.to_owned());
        self.core
            .lock()
            .expect("key publication detach")
            .detach_keys(entry.id.0.as_deref(), &entry.id.1);
        entry.phase.send_replace(phase);
    }
}

/// A stored hash is empty or one BlindV1 token: 16 HMAC bytes in 22 URL-safe
/// base64 characters.
pub(super) fn supported_coordinate(domain: &CoreKeyDomain, hash: &str, range: &str) -> bool {
    let hash_supported = hash.is_empty()
        || (hash.len() == 22
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')));
    hash_supported && domain.accepts_stored_range(range)
}

/// A generation edit can hide or expose a logical coordinate. Keep the same
/// candidate either way, and let exact current-tip resolution decide visibility.
fn candidate_key<'a>(molecule: &str, bare: &'a str) -> Option<std::borrow::Cow<'a, str>> {
    if bare.starts_with("mk:") {
        return Some(std::borrow::Cow::Borrowed(bare));
    }
    if bare.starts_with("mgd:v1:") {
        return molecule_record_key_from_generation_delete_key(molecule, bare)
            .map(std::borrow::Cow::Owned);
    }
    let prefix = format!("mgr:v1:{molecule}:");
    let (generation, _) = bare.strip_prefix(&prefix)?.split_once(':')?;
    molecule_record_key_from_generation_key(molecule, generation, bare).map(std::borrow::Cow::Owned)
}

/// Scope boundaries are matched before a supported molecule family. This
/// keeps scoped and personal molecules separate, including short share scopes.
fn membership_coordinate(raw: &str) -> Option<(DomainId, &str, bool)> {
    let families = [
        ("mgp:v1:", true),
        ("mgr:v1:", false),
        ("mgd:v1:", false),
        ("mk:", false),
    ];
    let (at, family, structural) = families
        .iter()
        .filter_map(|(family, structural)| {
            raw.match_indices(family)
                .find(|(at, _)| *at == 0 || raw.as_bytes()[at - 1] == b':')
                .map(|(at, _)| (at, *family, *structural))
        })
        .min_by_key(|(at, _, _)| *at)?;
    let scope = raw[..at]
        .strip_suffix(':')
        .filter(|scope| !scope.is_empty());
    let bare = &raw[at..];
    let molecule = bare[family.len()..].split([':', '\0']).next()?;
    if molecule.is_empty()
        || (family == "mk:" && !bare.starts_with(&molecule_record_prefix(molecule)))
    {
        return None;
    }
    Some((
        (scope.map(str::to_owned), molecule.to_owned()),
        bare,
        structural,
    ))
}
