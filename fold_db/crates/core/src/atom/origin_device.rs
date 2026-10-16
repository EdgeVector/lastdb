//! The bound on the sync device id that a tip carries.
//!
//! The id of a peer comes from its log entry. A peer is not trusted to bound
//! it. A long or odd id must still name one device, and it must still differ
//! from the id of this node, because an unknown id is not recorded and an
//! "in doubt" pair must be. So the node maps such an id to a short token.

use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::fmt::Write as _;

/// Longest `origin_device` that a tip keeps as it is, in bytes.
pub const MAX_ORIGIN_DEVICE_BYTES: usize = 128;

/// Every token starts with this prefix, and no valid id does.
const TOKEN_PREFIX: &str = "h:";

/// A device id as a tip may carry it.
///
/// A valid id is kept as it is: not empty, at most [`MAX_ORIGIN_DEVICE_BYTES`]
/// bytes, no control character, and no `h:` prefix. Any other id (an empty
/// one, a long one, one with a control character, one that looks like a token)
/// becomes `"h:"` and the first 32 hex characters of the SHA-256 of its bytes.
/// The token is 34 bytes. It is stable, so the id still names one device. Two
/// ids that differ map to two tokens, and no token equals a valid id, so a
/// peer cannot take the identity of this node by the choice of its id.
#[must_use]
pub fn bounded_origin_device(raw: &str) -> Cow<'_, str> {
    let valid = !raw.is_empty()
        && raw.len() <= MAX_ORIGIN_DEVICE_BYTES
        && !raw.starts_with(TOKEN_PREFIX)
        && !raw.chars().any(char::is_control);
    if valid {
        return Cow::Borrowed(raw);
    }
    let digest = Sha256::digest(raw.as_bytes());
    let mut token = String::from(TOKEN_PREFIX);
    for byte in &digest[..16] {
        let _ = write!(token, "{byte:02x}");
    }
    Cow::Owned(token)
}
