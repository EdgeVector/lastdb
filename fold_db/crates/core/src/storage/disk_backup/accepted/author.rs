//! The author of the entries that a capturing store puts into its spool.
//!
//! A store that captures its writes makes each one, in the transaction that
//! commits it, the next signed entry of one device stream. The entry is final
//! from then on: the spool holds the bytes the relay will store, and sending
//! them changes nothing. The store does not know the signed log. The host
//! gives it this author, which holds the keys and builds the entry.

use serde::{Deserialize, Serialize};

/// One entry of the device stream that this store signs: its place and its hash.
/// The next entry names it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalEntry {
    /// Sequence number. The first entry of a stream is 1.
    pub seq: u64,
    /// Hash of the entry, 64 lowercase hex characters.
    pub hash: String,
    /// Order value of the entry.
    pub order: u64,
}

/// A signed entry: the bytes the relay stores, and the hash the next entry names.
pub struct SignedLocal {
    pub entry: Vec<u8>,
    pub hash: String,
}

/// Seals and signs the captured writes of one store as the entries of one device stream.
pub trait CaptureAuthor: Send + Sync {
    /// The id of the device stream, lowercase hex.
    fn device(&self) -> &str;

    /// Seal the bytes of a file for the entry at `seq`.
    fn seal_file(&self, seq: u64, plain: &[u8]) -> Result<Vec<u8>, String>;

    /// Seal `body` and sign it as the entry after `previous` (the first entry
    /// when there is none), with the order value `order`. `file_sha256` is the
    /// hash of the sealed file the entry names. `note` is the write's
    /// `log_note`: bytes for the author that the store does not read.
    fn sign(
        &self,
        previous: Option<&LocalEntry>,
        order: u64,
        body: &[u8],
        file_sha256: Option<&str>,
        note: Option<&[u8]>,
    ) -> Result<SignedLocal, String>;
}
