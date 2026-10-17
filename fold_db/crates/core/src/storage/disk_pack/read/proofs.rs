//! Bounded proof holds expire with the captured call, never with global eviction.

use super::*;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct HeldWitnesses {
    entries: BTreeMap<u64, Arc<validity::ValidatedWitness>>,
    bytes: usize,
}

impl ReadContext {
    pub(in crate::storage::disk_pack) fn validated(
        &self,
        token: u64,
    ) -> Option<Arc<validity::ValidatedWitness>> {
        let mut held = self
            .witnesses
            .lock()
            .expect("native held dependency proofs");
        if let Some(proof) = held.entries.get(&token) {
            return Some(Arc::clone(proof));
        }
        let proof = self
            .validity
            .lock()
            .expect("native dependency validity")
            .validate(token, self.version.revision)?;
        let bytes = held.bytes.checked_add(proof.bytes())?;
        if held.entries.len() >= 10_000 || bytes > 16 * 1024 * 1024 {
            return None;
        }
        held.bytes = bytes;
        held.entries.insert(token, Arc::clone(&proof));
        Some(proof)
    }

    /// Complete preflight checks hold every source proof before their copies.
    pub(in crate::storage::disk_pack) fn inherit_validated(&self) {
        let held = self
            .witnesses
            .lock()
            .expect("native held dependency proofs");
        let mut dependencies = self.dependencies.lock().expect("native read dependencies");
        for (token, proof) in &held.entries {
            proof.inherit(*token, &mut dependencies);
        }
    }
}
