//! Merkle tree utility for source-molecule sets.
//!
//! This is the leaf-and-root primitive behind `Provenance::Derived`'s
//! `sources_merkle_root`. Leaves are `MoleculeRef::canonical_bytes()`
//! outputs; the root pins the set of source molecules that flowed into a
//! derived molecule without inlining the full list.
//!
//! **Canonical forever.** Hash function is SHA-256. Odd internal layers
//! duplicate the last node Bitcoin-style. The final root commits to the
//! leaf count via `SHA-256(leaf_count_u64_be || inner_root)` — without
//! that wrap, `[A, B, C]` and `[A, B, C, C]` produce the same inner root
//! (CVE-2012-2459), letting an attacker substitute a duplicate-tail
//! source list past `LineageIndex::verify_merkle_consistency`. Changing
//! any of these choices changes the content address of every derived
//! molecule — gated by bumping `Provenance::Derived::encoding_version`.

use sha2::{Digest, Sha256};

/// Hash a single byte slice with SHA-256 into a fixed 32-byte array.
fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Hash the concatenation of two 32-byte nodes.
fn hash_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// Wrap an inner Merkle root with the leaf count so two leaf sets of
/// different size cannot share a root.
///
/// Closes CVE-2012-2459 in our Bitcoin-style inner tree: `[A, B, C]`
/// duplicates the last leaf at layer 0, producing the same internal
/// hashes — and therefore the same inner root — as `[A, B, C, C]`. The
/// count prefix makes those two leaf sets resolve to different final
/// roots (count 3 vs 4), so `verify_merkle_consistency` rejects a
/// duplicate-tail substitution against a root committed to the honest
/// (deduped) source list.
fn wrap_with_count(inner: &[u8; 32], leaf_count: usize) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((leaf_count as u64).to_be_bytes());
    hasher.update(inner);
    hasher.finalize().into()
}

/// Inner Bitcoin-style Merkle root over `leaves`, without the leaf-count
/// wrap. Empty input collapses to `sha256("")`. Odd layers duplicate the
/// last node before pairing. The final external root from [`merkle_root`]
/// always wraps this in [`wrap_with_count`] so a different leaf count
/// produces a different root even when the inner hashes collide.
fn inner_merkle_root(leaves: &[Vec<u8>]) -> [u8; 32] {
    if leaves.is_empty() {
        return sha256(b"");
    }

    let mut layer: Vec<[u8; 32]> = leaves.iter().map(|leaf| sha256(leaf)).collect();

    while layer.len() > 1 {
        if !layer.len().is_multiple_of(2) {
            let last = *layer.last().expect("non-empty layer");
            layer.push(last);
        }
        layer = layer
            .chunks_exact(2)
            .map(|pair| hash_pair(&pair[0], &pair[1]))
            .collect();
    }

    layer[0]
}

/// Build a Merkle root over `leaves`.
///
/// The returned root is `SHA-256(leaf_count_u64_be || inner_root)` where
/// `inner_root` is a Bitcoin-style tree (odd layers duplicate the last
/// node). The leaf-count prefix is what makes the root injective in the
/// leaf count, defeating the duplicate-last-leaf collision documented at
/// [`wrap_with_count`].
///
/// - Empty input returns `SHA-256(0u64_be || sha256(""))`. The "no
///   sources" sentinel still has a well-defined, stable root.
///
/// The wrap + inner shape is pinned forever by the known-vector tests; a
/// change breaks every previously-stored `sources_merkle_root`.
#[must_use]
pub fn merkle_root(leaves: &[Vec<u8>]) -> [u8; 32] {
    wrap_with_count(&inner_merkle_root(leaves), leaves.len())
}
