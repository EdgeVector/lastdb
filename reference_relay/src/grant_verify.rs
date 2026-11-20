//! Publisher-signature checks for the entries the relay folds into its grant index.

use mutation_log::{
    verify_birth_signature, verify_entry_signature, DatabaseId, NodeKey, SignedEntry,
};

use crate::grant_index::Birth;

/// Verify a `birth` entry: entry signature, owner == signer, and db-key birth proof.
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
    verify_entry_signature(entry).map_err(|_| "entries.signature")?;
    verify_birth_signature(*db, body.owner, body.db_salt, &body.birth_signature)
        .map_err(|_| "entries.birth_signature")?;
    Ok(Birth {
        owner: body.owner,
        db_salt: body.db_salt,
    })
}

/// Verify a `grant` entry is signed by the database publisher.
pub(crate) fn verify_publisher_grant(
    db: &DatabaseId,
    publisher: &NodeKey,
    entry: &SignedEntry,
) -> Result<(), &'static str> {
    if !matches!(entry, SignedEntry::Grant { .. }) {
        return Err("entries.kind");
    }
    let header = entry.header();
    if header.database_id != *db {
        return Err("entries.database_id");
    }
    if header.node_key != *publisher {
        return Err("entries.node_key");
    }
    verify_entry_signature(entry).map_err(|_| "entries.signature")
}
