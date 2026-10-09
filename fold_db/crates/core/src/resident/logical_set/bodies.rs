//! Atom and tip bodies plus pending write tokens.

use super::*;

impl LogicalResidentSet {
    pub fn admit_atom_body(&mut self, id: AtomId, body: Vec<u8>) {
        self.store_atom_body(id, body);
        self.purge_over_cap();
    }

    pub(super) fn store_atom_body(&mut self, id: AtomId, body: Vec<u8>) {
        self.touch(ResidentKey::Atom(id.clone()));
        self.atom_bodies.insert(id, body);
    }

    pub fn atom_body(&self, id: &AtomId) -> Option<&[u8]> {
        self.atom_bodies.get(id).map(Vec::as_slice)
    }

    /// Drop a stored atom body. A product delete uses this so a later get
    /// does not serve the overlay after the pin records the delete.
    pub fn drop_atom_body(&mut self, id: &AtomId) {
        self.atom_bodies.remove(id);
        self.forget_key(&ResidentKey::Atom(id.clone()));
        self.publish_occupancy();
    }

    /// Store the raw tip JSON (or ciphertext) for a resident molecule key.
    pub fn store_tip_body(&mut self, molecule: MoleculeId, hash: &str, range: &str, body: Vec<u8>) {
        self.tip_bodies.insert(tip_key(molecule, hash, range), body);
    }

    /// Raw tip bytes for a resident molecule key.
    pub fn tip_body(&self, molecule: MoleculeId, hash: &str, range: &str) -> Option<&[u8]> {
        self.tip_bodies
            .get(&tip_key(molecule, hash, range))
            .map(Vec::as_slice)
    }

    /// Record the loader token for `key` so a later admit can mark dirty.
    /// `body_digest` names the stored bytes of this append.
    ///
    /// Returns true when `token` is the newest token for `key` after this call.
    /// A concurrent older put must not store its body when this returns false.
    pub fn record_write_token(
        &mut self,
        key: Vec<u8>,
        token: DurabilityToken,
        body_digest: u64,
    ) -> bool {
        match self.pending_write_tokens.get(&key) {
            Some(existing) if existing.token > token => false,
            _ => {
                self.pending_write_tokens
                    .insert(key, PendingWrite { token, body_digest });
                true
            }
        }
    }

    /// Take the pending token for `key` only when the newest append stored
    /// the bytes named by `body_digest`. An older put gets `None` and leaves
    /// the newer put's entry in place.
    pub fn take_write_token_for(
        &mut self,
        key: &[u8],
        body_digest: u64,
    ) -> Option<DurabilityToken> {
        match self.pending_write_tokens.get(key) {
            Some(pending) if pending.body_digest == body_digest => {
                let token = pending.token;
                self.pending_write_tokens.remove(key);
                Some(token)
            }
            _ => None,
        }
    }

    /// Newest recorded write token for `key`.
    pub fn newest_write_token(&self, key: &[u8]) -> Option<DurabilityToken> {
        self.pending_write_tokens
            .get(key)
            .map(|pending| pending.token)
    }

    /// Drop the pending write token for `key`. A delete must not leave a
    /// token for a later put to consume.
    pub fn clear_write_tokens(&mut self, key: &[u8]) {
        self.pending_write_tokens.remove(key);
    }
}
