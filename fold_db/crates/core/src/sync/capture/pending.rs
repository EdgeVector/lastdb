//! Residual in-memory capture pending map.
//!
//! Store-diff C11 de-dupe is retired with cold watermark capture. The map is
//! no longer populated; overflow/heal helpers still clear it for a safe
//! no-op transition.

use std::collections::HashMap;

/// Namespace + raw key identity (legacy map key shape).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NsKey {
    pub namespace: String,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingExport {
    pub staging_seq: u64,
}

pub(crate) type PendingMap = HashMap<NsKey, PendingExport>;
