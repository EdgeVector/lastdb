//! Scope-relative backup key namespaces and classifiers, client side.
//!
//! The storage Lambda strips `{scope}/` before the client sees a listed key,
//! so everything here is scope-relative: `backup/chunks/{sha256}`,
//! `backup/v2/objects/{class}/{instance_id}` and so on.
//!
//! Hand-kept mirror of `exemem_common::storage` (constants and
//! `classify_backup_v2_key`). Deliberately duplicated rather than imported:
//! that crate lives in the lambda-only, workspace-excluded
//! `exemem_service/lambdas/*` tree, and `fold_db` core must not depend on
//! it. Keep the two in sync by hand; the tests below pin the shapes.
//!
//! # Why a second namespace
//!
//! The cloud-owned GC plan (P2, PR-A) reserves `backup/v2/` beside the v1
//! `backup/chunks/` and `backup/manifests/` prefixes. The format epoch is
//! part of the key on purpose (P0 `PhysicalKey`: format epoch is physical
//! identity), so a v2 object can never collide with a v1 content-addressed
//! chunk. Nothing on the client mints a v2 key yet. What PR-A must
//! guarantee is the negative space: every v1 listing parser and the v1
//! orphan GC treat a v2 key as a backup object that is **never** a v1
//! chunk digest — never seeded into the presence cache, never selected as
//! an orphan, never dispatched as a `legacy_backup_chunk` receipt — while
//! an inventory still counts it as backup storage.
//!
//! The four contract items behind the v2 format (opaque reference
//! metadata, one head authority, unique physical instances, restore-base
//! retirement) are not yet approved. This module carries no wire behaviour.

/// v1 sealed chunk objects: `backup/chunks/{sha256}`.
pub(crate) const BACKUP_CHUNKS_PREFIX: &str = "backup/chunks/";
/// v1 plaintext manifests: `backup/manifests/{sha256}`.
pub(crate) const BACKUP_MANIFESTS_PREFIX: &str = "backup/manifests/";
/// v1 server-side CAS pointer.
pub(crate) const BACKUP_LATEST_KEY: &str = "backup/latest";
/// Root of the v2 namespace. One `list_objects` on this prefix reaches the
/// whole namespace; the Lambda routes it to R2 like every other backup key.
pub(crate) const BACKUP_V2_PREFIX: &str = "backup/v2/";
/// `backup/v2/objects/{class}/{instance_id}` — final, server-minted instance.
pub(crate) const BACKUP_V2_OBJECTS_PREFIX: &str = "backup/v2/objects/";
/// `backup/v2/uploads/{attempt_id}` — disposable upload destination.
pub(crate) const BACKUP_V2_UPLOADS_PREFIX: &str = "backup/v2/uploads/";
/// `backup/v2/pages/{root_id}/{page:06}` — descriptor page.
pub(crate) const BACKUP_V2_PAGES_PREFIX: &str = "backup/v2/pages/";
/// `backup/v2/manifests/{root_id}` — encrypted manifest.
pub(crate) const BACKUP_V2_MANIFESTS_PREFIX: &str = "backup/v2/manifests/";
/// Width of the zero-padded page index segment.
const BACKUP_V2_PAGE_INDEX_WIDTH: usize = 6;

/// Every backup key shape a listing can return, scope-relative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackupKeyClass<'a> {
    /// `backup/chunks/{sha256}` with an exact 64-hex leaf. The only class
    /// the v1 presence cache and the v1 orphan GC may act on.
    V1Chunk(&'a str),
    /// Under `backup/chunks/` but not a 64-hex leaf (nested key, junk).
    /// v1 size accounting includes it; v1 presence and GC ignore it.
    V1ChunkStray(&'a str),
    /// `backup/manifests/{remainder}`.
    V1Manifest(&'a str),
    /// `backup/latest`.
    V1Latest,
    /// `backup/v2/objects/{class}/{instance_id}`.
    V2Object {
        class: &'a str,
        instance_id: &'a str,
    },
    /// `backup/v2/uploads/{attempt_id}`.
    V2Upload { attempt_id: &'a str },
    /// `backup/v2/pages/{root_id}/{page:06}`.
    V2Page { root_id: &'a str, page: &'a str },
    /// `backup/v2/manifests/{root_id}`.
    V2Manifest { root_id: &'a str },
    /// Under `backup/v2/` with an unknown sub-prefix or a malformed shape.
    /// Still a backup object, never a v1 chunk digest; surfaced as its own
    /// inventory category rather than folded into another one.
    V2Other(&'a str),
    /// Not a backup key.
    Other,
}

impl BackupKeyClass<'_> {
    /// Inventory category label, or `None` when the key is not a backup key.
    /// v1 labels are byte-identical to the ones `prefix_inventory` printed
    /// before the v2 namespace existed.
    pub(crate) fn category(self) -> Option<&'static str> {
        Some(match self {
            Self::V1Chunk(_) | Self::V1ChunkStray(_) => "backup/chunks",
            Self::V1Manifest(_) => "backup/manifests",
            Self::V1Latest => "backup/latest",
            Self::V2Object { .. } => "backup/v2/objects",
            Self::V2Upload { .. } => "backup/v2/uploads",
            Self::V2Page { .. } => "backup/v2/pages",
            Self::V2Manifest { .. } => "backup/v2/manifests",
            Self::V2Other(_) => "backup/v2/other",
            Self::Other => return None,
        })
    }
}

/// Exactly 64 ASCII hex digits.
pub(crate) fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn segment_ok(segment: &str) -> bool {
    !segment.is_empty() && segment != "." && segment != ".." && !segment.contains('/')
}

/// Classify one scope-relative key.
pub(crate) fn classify_backup_key(key: &str) -> BackupKeyClass<'_> {
    if let Some(rest) = key.strip_prefix(BACKUP_CHUNKS_PREFIX) {
        return if is_sha256_hex(rest) {
            BackupKeyClass::V1Chunk(rest)
        } else {
            BackupKeyClass::V1ChunkStray(rest)
        };
    }
    if let Some(rest) = key.strip_prefix(BACKUP_MANIFESTS_PREFIX) {
        return BackupKeyClass::V1Manifest(rest);
    }
    if key == BACKUP_LATEST_KEY {
        return BackupKeyClass::V1Latest;
    }
    let Some(v2_rest) = key.strip_prefix(BACKUP_V2_PREFIX) else {
        return BackupKeyClass::Other;
    };
    if let Some(rest) = key.strip_prefix(BACKUP_V2_OBJECTS_PREFIX) {
        if let Some((class, instance_id)) = rest.split_once('/') {
            if segment_ok(class) && segment_ok(instance_id) {
                return BackupKeyClass::V2Object { class, instance_id };
            }
        }
    } else if let Some(attempt_id) = key.strip_prefix(BACKUP_V2_UPLOADS_PREFIX) {
        if segment_ok(attempt_id) {
            return BackupKeyClass::V2Upload { attempt_id };
        }
    } else if let Some(rest) = key.strip_prefix(BACKUP_V2_PAGES_PREFIX) {
        if let Some((root_id, page)) = rest.split_once('/') {
            if segment_ok(root_id)
                && page.len() == BACKUP_V2_PAGE_INDEX_WIDTH
                && page.bytes().all(|b| b.is_ascii_digit())
            {
                return BackupKeyClass::V2Page { root_id, page };
            }
        }
    } else if let Some(root_id) = key.strip_prefix(BACKUP_V2_MANIFESTS_PREFIX) {
        if segment_ok(root_id) {
            return BackupKeyClass::V2Manifest { root_id };
        }
    }
    BackupKeyClass::V2Other(v2_rest)
}

/// The remainder after `backup/chunks/`, any shape. This is the exact
/// `strip_prefix("backup/chunks/")` the v1 size and footprint accounting
/// always used; it stays byte-identical so billed bytes do not move. A v2
/// key never has this prefix, so it is never returned here.
pub(crate) fn v1_chunk_remainder(key: &str) -> Option<&str> {
    key.strip_prefix(BACKUP_CHUNKS_PREFIX)
}

/// The v1 chunk digest, only for an exact 64-hex leaf under
/// `backup/chunks/`. Manifests, nested keys, junk and every v2 key yield
/// `None`. This is the only function the presence cache and the orphan
/// selector may take a digest from.
pub(crate) fn v1_chunk_sha(key: &str) -> Option<&str> {
    match classify_backup_key(key) {
        BackupKeyClass::V1Chunk(sha) => Some(sha),
        _ => None,
    }
}
