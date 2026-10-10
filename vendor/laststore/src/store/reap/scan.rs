//! Run the matcher over the live keys of one loaded group.

use super::matcher::Matcher;
use super::{group_label, ReapError};
use crate::store::{LastStore, Loc, Shard};
use sha2::{Digest, Sha256};

/// Bytes of one record that are not the key and not the value: the op byte,
/// the 2-byte key length, and the 4-byte value length.
const RECORD_FRAMING_BYTES: u64 = 7;

/// What a scan of one group found.
pub(super) struct GroupScan {
    /// Live keys in the group.
    pub keys: u64,
    /// Keys that a rule matches.
    pub matched_keys: u64,
    /// Sum of key length and value length of the matched keys.
    pub matched_bytes: u64,
    /// The matched keys. Filled only by a [`ScanDepth::Digest`] scan.
    pub matched: Vec<String>,
    /// Digest of every kept key and value, in key order. Filled only by a
    /// [`ScanDepth::Digest`] scan.
    pub kept_digest: [u8; 32],
}

/// How much work a scan does.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ScanDepth {
    /// Count the matches. Read no value.
    Count,
    /// Collect the matched keys. Read every kept value into the digest.
    Digest,
}

/// Refuse a group that this reap cannot rewrite.
pub(super) fn refuse_non_plain_group(
    sh: &Shard,
    collection: &str,
    shard: u16,
    group: Option<u32>,
) -> Result<(), ReapError> {
    if sh.uses_sorted_index() || sh.data_key.is_some() {
        return Err(ReapError::Refused(format!(
            "{} is not a plain legacy group (sorted index or data key)",
            group_label(collection, shard, group)
        )));
    }
    Ok(())
}

/// Key bytes plus value bytes of a record, from its location.
fn pair_bytes(loc: Loc) -> Result<u64, ReapError> {
    match loc {
        Loc::Legacy { len, .. } => Ok(len.saturating_sub(RECORD_FRAMING_BYTES)),
        _ => Err(ReapError::Refused(
            "a key is not in a plain legacy segment".to_string(),
        )),
    }
}

fn digest_pair(hasher: &mut Sha256, key: &str, body: &[u8]) {
    hasher.update((key.len() as u64).to_le_bytes());
    hasher.update(key.as_bytes());
    hasher.update((body.len() as u64).to_le_bytes());
    hasher.update(body);
}

/// Scan every live key of a loaded group.
///
/// The matcher sees the raw key bytes. A `Digest` scan also reads every kept
/// value, so the caller can prove that the rewrite kept those bytes.
pub(super) fn scan_group(
    sh: &Shard,
    matcher: &Matcher,
    depth: ScanDepth,
) -> Result<GroupScan, ReapError> {
    let mut scan = GroupScan {
        keys: 0,
        matched_keys: 0,
        matched_bytes: 0,
        matched: Vec::new(),
        kept_digest: [0u8; 32],
    };
    let mut hasher = Sha256::new();
    let mut failure: Option<ReapError> = None;
    sh.visit_keys("", None, |key, loc| {
        scan.keys += 1;
        if matcher.matches(key.as_bytes()) {
            match pair_bytes(loc) {
                Ok(bytes) => scan.matched_bytes = scan.matched_bytes.saturating_add(bytes),
                Err(error) => {
                    failure = Some(error);
                    return false;
                }
            }
            scan.matched_keys += 1;
            if depth == ScanDepth::Digest {
                scan.matched.push(key.to_string());
            }
            return true;
        }
        if depth == ScanDepth::Digest {
            match LastStore::read_at_uncached(sh, loc) {
                Ok(body) => digest_pair(&mut hasher, key, &body),
                Err(error) => {
                    failure = Some(error.into());
                    return false;
                }
            }
        }
        true
    })?;
    if let Some(error) = failure {
        return Err(error);
    }
    scan.kept_digest = hasher.finalize().into();
    Ok(scan)
}
