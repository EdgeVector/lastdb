//! Atom CRUD, mutation-event history, and schema-index listing.

use crate::atom::{atom_key_codec, atom_locator_codec, molecule_key_codec};
use crate::atom::{Atom, MutationEvent};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;

use super::helpers::schema_index_codec;
use super::{AtomStore, BlobRefEdge};

/// Report a failed batch write and turn it into the error the caller sees.
///
/// Two failures arrive here that need opposite treatment:
///
/// - A **full disk** is an operator condition. The request was well formed and
///   will succeed unchanged once space is freed, so it keeps its own
///   [`SchemaError::StorageFull`] identity (rendered as `507`, not `400`) and is
///   logged at WARN. At ERROR every failed write becomes its own Sentry issue:
///   one 8-hour disk-full episode raised 207 of them (issue `7620061902`),
///   burying the single fact an operator needed.
/// - **Capture-queue backpressure** is the same shape with a shorter fuse. The
///   bounded mutation-capture queue stayed full for its whole admission window,
///   so the write was refused before the local commit. Nothing is wrong with
///   the request and nothing needs an operator — the worker drains and the same
///   write succeeds. It keeps its own [`SchemaError::CaptureQueueFull`]
///   identity (rendered as a retryable `503`, not `400`) and is logged at WARN.
///   At ERROR it repeated the disk-full storm in a different lane: issue
///   `7699865707` took 123 error events in 13 hours from one node, 0 users.
///   The volume now lives in the `queue_rejections` capture counter instead.
/// - Anything else is a real backend fault and stays an ERROR.
///
/// `what` names the write, e.g. `"atoms"` or `"atom schema index"`.
fn batch_write_error(what: &str, error: crate::storage::StorageError) -> SchemaError {
    if error.is_storage_full() {
        // Not a code fault: say it once per write at WARN, and let the 507 tell
        // the caller to free space rather than re-check its payload.
        tracing::warn!("Cannot batch store {what} — {error}");
        return SchemaError::from(error);
    }
    if error.is_capture_queue_full() {
        // Not a code fault either, and it clears itself: say it once per write
        // at WARN and let the 503 tell the caller to retry unchanged.
        tracing::warn!("Cannot batch store {what} — {error}");
        return SchemaError::from(error);
    }
    tracing::error!("Failed to batch store {what}: {error}");
    SchemaError::InvalidData(format!("Failed to batch store {what}: {error}"))
}

mod events;
mod history;
mod maintenance;
mod read;
mod schema_index;
mod write;
