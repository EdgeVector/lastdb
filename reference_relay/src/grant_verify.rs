//! Publisher-signature checks for the entries the relay folds into its grant index.

use mutation_log::{
    verify_birth_signature, verify_entry_signature, DatabaseId, EntryHash, Grantee, NodeKey, Role,
    SignedEntry,
};

use crate::grant_index::Birth;

/// Verify a `birth` entry: entry signature, owner == signer, genesis position, and db-key birth proof.
pub(crate) fn verify_birth(db: &DatabaseId, entry: &SignedEntry) -> Result<Birth, &'static str> {
    let SignedEntry::Birth { header, body, .. } = entry else {
        return Err("entries.kind");
    };
    if header.database_id != *db {
        return Err("entries.database_id");
    }
    if header.node_key != body.owner {
        return Err("entries.node_key");
    }
    if header.writer_seq != 1 {
        return Err("entries.writer_seq");
    }
    if header.prev_hash != EntryHash::ZERO {
        return Err("entries.prev_hash");
    }
    verify_entry_signature(entry).map_err(|_| "entries.signature")?;
    verify_birth_signature(*db, body.owner, body.db_salt, &body.birth_signature)
        .map_err(|_| "entries.birth_signature")?;
    Ok(Birth {
        owner: body.owner,
        db_salt: body.db_salt,
    })
}

/// Verify a `grant` entry is signed by the database publisher and carries a valid role.
pub(crate) fn verify_publisher_grant(
    db: &DatabaseId,
    publisher: &NodeKey,
    entry: &SignedEntry,
) -> Result<(), &'static str> {
    let SignedEntry::Grant { header, body, .. } = entry else {
        return Err("entries.kind");
    };
    if header.database_id != *db {
        return Err("entries.database_id");
    }
    if header.node_key != *publisher {
        return Err("entries.node_key");
    }
    if body.grantee == Grantee::Any && !matches!(body.role, Role::Read | Role::Revoked) {
        return Err("grants.any.role");
    }
    verify_entry_signature(entry).map_err(|_| "entries.signature")
}
