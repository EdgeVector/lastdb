//! Self-access of a received database, folded from its signed grants.

use mutation_log::{NodeKey, Role, VerifiedHistory};
use serde_json::{json, Value};

use crate::host::Host;

/// `{ "role": "read" | "submit" | "write" }` for this node's key. Nothing when
/// no history is held yet or the key has no grant.
pub(crate) fn view(host: &Host, database_id: &str) -> Option<Value> {
    let node = NodeKey::from_bytes(host.keypair.public_key_bytes());
    let histories = super::histories_for(host);
    histories
        .peek_with(database_id, |history| role_of(history, node))
        .flatten()
        .map(|role| json!({ "role": role }))
}

/// Folded role for `node` in `history`: owner is Write; else the grant window;
/// else a public `any` role when present.
pub(crate) fn role_of(history: &VerifiedHistory, node: NodeKey) -> Option<Role> {
    if history.owner() == Some(node) {
        return Some(Role::Write);
    }
    let grants = history.grants();
    grants.role_of(&node).or_else(|| grants.any_role())
}
