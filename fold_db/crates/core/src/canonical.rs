//! Canonical byte framing for hashing and signing.
//!
//! # Why this module exists
//!
//! Every place in `fold_db` that assembles bytes to feed a SHA-256 digest, an
//! Ed25519 signature, or a Merkle leaf must do so *unambiguously*: two
//! semantically-different field tuples must never produce the same bytes.
//! Otherwise one signature verifies two distinct messages, or two distinct
//! Merkle leaves collide.
//!
//! The naive approach — joining variable-length fields with a single `0x00`
//! separator — is **not** injective when a field can itself contain `0x00`.
//! Many of our fields can: `atom_uuid` and `molecule_uuid` are peer-supplied
//! on the import / sync-merge path, range/hash `key`s are user-supplied, and
//! `share_e2e_secret` is random bytes. An attacker could shift a `0x00` byte
//! across a field boundary so that, e.g.
//!
//! ```text
//! A: { atom_uuid: "B",     key: "\x00k" }
//! B: { atom_uuid: "B\x00", key: "k"     }
//! ```
//!
//! serialize identically under a `0x00`-separator scheme. This is the
//! "NUL-shift collision". It was discovered and patched independently, site by
//! site, across PRs #408 (share rules), #409 (`MoleculeHashRange`), #422
//! (`hash_input_snapshot`), #438 (`MoleculeRange`), #442 (`MoleculeHash`),
//! #566 (`MoleculeRef`), and #573 (`Molecule`) — each one re-deriving the same
//! fix and re-implementing the same private helper. This module is the *one*
//! home for that discipline so the next signing/hashing site inherits it for
//! free, and the injectivity property is enforced by a test
//! ([`tests::framing_is_injective_over_adversarial_corpus`]) rather than by
//! reviewer vigilance.
//!
//! # The framing
//!
//! Each variable-length field is written as a **4-byte big-endian `u32` length
//! prefix** followed by its raw bytes. Fixed-width integers (`u64` versions /
//! timestamps) are written directly as big-endian bytes with no prefix — their
//! width is known statically, so they are self-delimiting in a fixed schema.
//!
//! Because every variable field carries its own length, the byte stream is
//! *uniquely decodable* given the field schema: there is exactly one way to
//! parse it back into its fields. Unique decodability ⇒ injectivity ⇒ no
//! cross-message collision. (Decode is exercised by the round-trip test.)
//!
//! Callers are still responsible for choosing a *fixed field order* per
//! message type and for not making two different message types share a layout;
//! this module guarantees only that a given ordered tuple of fields maps to a
//! unique byte string.

use sha2::Sha256;

/// A sink that canonical framing can be written into.
///
/// Implemented for `Vec<u8>` (build-then-hash/sign) and [`Sha256`]
/// (stream-into-the-digest, avoiding a large intermediate `Vec`). Both produce
/// byte-identical framing.
pub trait ByteSink {
    /// Append raw bytes to the sink.
    fn put(&mut self, bytes: &[u8]);
}

impl ByteSink for Vec<u8> {
    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

impl ByteSink for Sha256 {
    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        use sha2::Digest;
        self.update(bytes);
    }
}

/// Write `field` into `sink` preceded by its length as a 4-byte big-endian
/// `u32`.
///
/// This is the single source of the length-prefix framing. The `u32` cap is
/// `4 GiB`; no legitimate canonical field comes anywhere near it, and the
/// conversion panics rather than silently truncating (a truncated length would
/// reintroduce the very ambiguity this framing exists to remove).
#[inline]
pub fn push_field<S: ByteSink>(sink: &mut S, field: &[u8]) {
    let len: u32 = field
        .len()
        .try_into()
        .expect("canonical field exceeds 4 GiB — not possible for any legitimate input");
    sink.put(&len.to_be_bytes());
    sink.put(field);
}

/// Ergonomic builder for the common "build a `Vec<u8>` then hash/sign it" case.
///
/// Field order is the caller's responsibility; this type only guarantees that
/// each `field` is length-prefixed and each fixed-width integer is appended
/// big-endian, matching [`push_field`].
///
/// ```ignore
/// let bytes = CanonicalWriter::new()
///     .field(molecule_uuid.as_bytes())
///     .field(atom_uuid.as_bytes())
///     .u64(version)
///     .u64(written_at)
///     .finish();
/// ```
#[derive(Debug, Default, Clone)]
pub struct CanonicalWriter {
    buf: Vec<u8>,
}

impl CanonicalWriter {
    /// A new, empty writer.
    #[must_use]
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// A new writer with a pre-sized backing buffer (an optimization hint
    /// only — does not affect output bytes).
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    /// Append a length-prefixed variable-length field.
    #[must_use]
    pub fn field(mut self, field: &[u8]) -> Self {
        push_field(&mut self.buf, field);
        self
    }

    /// Append a `u64` as 8 big-endian bytes (no length prefix — fixed width).
    #[must_use]
    pub fn u64(mut self, value: u64) -> Self {
        self.buf.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Consume the writer and return the assembled canonical bytes.
    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}
