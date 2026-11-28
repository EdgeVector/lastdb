//! Relay-side access gating from the grant index (see `docs/designs/relay-protocol.md`).

use mutation_log::{DatabaseId, Hex32, NodeKey, Role, SignedEntry};
use serde_json::Value;

use crate::error::{ApiError, ApiResult};
use crate::grant_index::{Birth, GrantIndex};
use crate::grant_verify::{verify_birth, verify_publisher_grant};

/// Verified side effects to fold into the index once the batch is stored.
pub(crate) struct AppendPlan {
    birth: Option<Birth>,
    grants: Vec<SignedEntry>,
}

impl AppendPlan {
    pub(crate) fn commit(self, index: &mut GrantIndex, db: DatabaseId) {
        if let Some(birth) = self.birth {
            index.install_birth(db, birth);
        }
        for grant in &self.grants {
            index.apply_grant(&db, grant);
        }
    }
}

/// Parse a lowercase-hex id from a request or header.
pub(crate) fn parse_id(s: &str, field: &str) -> ApiResult<Hex32> {
    Hex32::from_hex(&s.to_ascii_lowercase()).map_err(|_| ApiError::invalid(field))
}

/// Decode opaque entry JSON into signed entries (needed to read kind/stream).
pub(crate) fn parse_entries(entries: &[Value]) -> ApiResult<Vec<SignedEntry>> {
    entries
        .iter()
        .map(|v| serde_json::from_value(v.clone()).map_err(|_| ApiError::invalid("entries")))
        .collect()
}

/// Gate `log.append`: publisher always; members by current role and entry kind.
pub(crate) fn authorize_append(
    index: &GrantIndex,
    db: &DatabaseId,
    caller: &NodeKey,
    entries: &[SignedEntry],
) -> ApiResult<AppendPlan> {
    if entries.is_empty() {
        return Err(ApiError::invalid("entries"));
    }
    let birth_in_batch = batch_birth(db, entries)?;
    let publisher = resolve_publisher(index, db, caller, birth_in_batch)?;
    let is_publisher = *caller == publisher;
    let mut grants = Vec::new();
    for entry in entries {
        if is_publisher {
            if matches!(entry, SignedEntry::Grant { .. }) {
                verify_publisher_grant(db, &publisher, entry).map_err(field_error)?;
                grants.push(entry.clone());
            }
        } else {
            authorize_member_entry(index, db, caller, entry)?;
        }
    }
    let birth = index
        .birth(db)
        .is_none()
        .then_some(birth_in_batch)
        .flatten();
    Ok(AppendPlan { birth, grants })
}

/// Gate `blob.put`: publisher or a current `write` grant.
pub(crate) fn authorize_blob_put(
    index: &GrantIndex,
    db: &DatabaseId,
    caller: &NodeKey,
) -> ApiResult<()> {
    let birth = index
        .birth(db)
        .ok_or_else(|| ApiError::forbidden("publisher"))?;
    if birth.owner == *caller || index.role_of(db, caller) == Some(Role::Write) {
        Ok(())
    } else {
        Err(ApiError::forbidden("role"))
    }
}

/// Gate `blob.get`: publisher, any current grant, or active `grant#any`.
pub(crate) fn authorize_read(
    index: &GrantIndex,
    db: &DatabaseId,
    caller: &NodeKey,
) -> ApiResult<()> {
    let birth = index
        .birth(db)
        .ok_or_else(|| ApiError::forbidden("publisher"))?;
    if birth.owner == *caller || index.role_of(db, caller).is_some() || index.any_read(db) {
        Ok(())
    } else {
        Err(ApiError::forbidden("role"))
    }
}

/// Gate `log.read` and `log.streams`. Active grants match [`authorize_read`],
/// narrowed by [`member_stream_visible`]. A revoked caller may only read a single
/// publisher device stream (AllDevices → 403), matching exemem #960 so the signed
/// revoke entry stays reachable.
pub(crate) fn authorize_log_read(
    index: &GrantIndex,
    db: &DatabaseId,
    caller: &NodeKey,
    writer_key: &str,
    all_devices: bool,
) -> ApiResult<()> {
    let birth = index
        .birth(db)
        .ok_or_else(|| ApiError::forbidden("publisher"))?;
    if birth.owner == *caller {
        return Ok(());
    }
    if index.role_of(db, caller).is_some() || index.any_read(db) {
        return if member_stream_visible(index, db, &birth, caller, writer_key) {
            Ok(())
        } else {
            Err(ApiError::forbidden("writer_key"))
        };
    }
    if index.is_revoked(db, caller) {
        if all_devices {
            return Err(ApiError::forbidden("role"));
        }
        if writer_key == birth.owner.to_hex() {
            return Ok(());
        }
    }
    Err(ApiError::forbidden("role"))
}

/// Whether a reader other than the publisher may read the streams of `writer_key`.
///
/// The publisher's streams are for every reader, and a member's own streams are
/// for that member. Another member's streams are for the publisher alone, unless
/// that member ever held `write`: its direct entries count under a lease, so
/// every reader needs them. A member that only held `submit` signs nothing but
/// proposals, and a proposal is the owner's to decide before anyone else reads it.
fn member_stream_visible(
    index: &GrantIndex,
    db: &DatabaseId,
    birth: &Birth,
    caller: &NodeKey,
    writer_key: &str,
) -> bool {
    let Ok(writer) = NodeKey::from_hex(&writer_key.to_ascii_lowercase()) else {
        return false;
    };
    writer == birth.owner || writer == *caller || index.ever_held_write(db, &writer)
}

fn authorize_member_entry(
    index: &GrantIndex,
    db: &DatabaseId,
    caller: &NodeKey,
    entry: &SignedEntry,
) -> ApiResult<()> {
    if entry.header().node_key != *caller {
        return Err(ApiError::forbidden("entries.node_key"));
    }
    let role = index.role_of(db, caller);
    // The relay is a mailbox. A member holding submit or write may deposit
    // data on its own stream, proposals and direct entries alike: the nodes
    // decide what counts, and a direct entry no lease covers is stored and
    // ignored. Bundles are for a member holding write. Lease entries and the
    // other kinds are the publisher's alone.
    let allowed = match entry {
        SignedEntry::Data { .. } => matches!(role, Some(Role::Write | Role::Submit)),
        SignedEntry::Bundle { .. } => role == Some(Role::Write),
        _ => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(ApiError::forbidden("role"))
    }
}

fn resolve_publisher(
    index: &GrantIndex,
    db: &DatabaseId,
    caller: &NodeKey,
    batch_birth: Option<Birth>,
) -> ApiResult<NodeKey> {
    match (index.birth(db), batch_birth) {
        (Some(known), Some(batch)) if known != batch => Err(ApiError::forbidden("entries.birth")),
        (Some(known), _) => Ok(known.owner),
        (None, Some(batch)) if batch.owner == *caller => Ok(batch.owner),
        (None, _) => Err(ApiError::forbidden("publisher")),
    }
}

fn batch_birth(db: &DatabaseId, entries: &[SignedEntry]) -> ApiResult<Option<Birth>> {
    let mut found: Option<Birth> = None;
    for entry in entries {
        if !matches!(entry, SignedEntry::Birth { .. }) {
            continue;
        }
        let birth = verify_birth(db, entry).map_err(field_error)?;
        if found.is_some_and(|f| f != birth) {
            return Err(ApiError::invalid("entries.birth"));
        }
        found = Some(birth);
    }
    Ok(found)
}

fn field_error(field: &'static str) -> ApiError {
    ApiError::invalid(field)
}
