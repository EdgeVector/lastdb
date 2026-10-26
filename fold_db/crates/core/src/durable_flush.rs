//! Legacy partition identifiers retained for serialized mutation receipts.
//!
//! Native requests do not write LastStore fanout groups; their receipt list
//! stays empty. Durability belongs to the native transaction queue.

use serde::Serialize;

/// One fanout slot the server already resolved. The client does not rehash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct TouchedGroupId {
    /// LastStore collection.
    pub collection: String,
    /// Shard index. Product homes use shard 0.
    pub shard: u16,
    /// Hash-group index inside that shard.
    pub group: u32,
}
