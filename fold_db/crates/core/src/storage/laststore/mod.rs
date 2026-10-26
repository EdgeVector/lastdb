//! Native disk helpers left over from the Last Store adapter.
//!
//! The Last Store–backed `KvStore` / `NamespacedStore` adapter is gone; every
//! home is a native disk home. What remains here is shared by the native
//! backend: current-key routing to physical collections, the native id
//! encoding, the plaintext-catalog guard, and the blocking-call bridge.
//!
//! ## Key encoding
//!
//! Trait keys are bytes; native ids are strings. UTF-8 keys that do not
//! start with `b64:` are stored as themselves (production molecule keys are
//! UTF-8). Other keys use `b64:` + standard base64.

mod key_routing;
pub(in crate::storage) mod native_routing;

use super::encrypting_namespaced_store::CATALOG_NAMESPACES;
use super::error::{StorageError, StorageResult};
use crate::mini_cutover::classify_main_key;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use key_routing::*;

const TIPS_COLLECTION: &str = "tips";

/// Catalog collections require direct JSON values. An at-rest envelope there is what made Tom's primary unbootable on 2026-08-16:
/// `TypedKvStore` parsed ciphertext as JSON (`expected value at line 1 column 1`)
/// and `get_all_schemas` aborted `open_existing_store`. Reject at the native
/// put so a legacy replay envelope fails closed instead of poisoning the catalog.
pub(in crate::storage) fn reject_enc_on_plaintext_catalog(
    collection: &str,
    value: &[u8],
) -> StorageResult<()> {
    // All at-rest envelopes are equally fatal on a plaintext catalog: the
    // 2026-08-16 brick was an `ENC:` body the catalog reader could not parse,
    // and `ENZ:` / binary `ENB:` bodies would fail identically.
    let enveloped = crate::crypto::is_sealed_at_rest(value);
    if enveloped && CATALOG_NAMESPACES.contains(&collection) {
        return Err(StorageError::InvalidOperation(format!(
            "refusing to persist ENC:/ENZ:/ENB: envelope in plaintext catalog \
             collection {collection:?} — schema/catalog rows must stay JSON so \
             boot can deserialize them"
        )));
    }
    Ok(())
}

/// Home of the blocking-call bridge and the native id encoding.
///
/// The name predates the native backend; callers reach these helpers through
/// it, so it stays until those call sites move.
pub(crate) struct LastStoreKvStore;

impl LastStoreKvStore {
    pub(in crate::storage) async fn run_blocking<T, F>(work: F) -> StorageResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> StorageResult<T> + Send + 'static,
    {
        let can_block = tokio::runtime::Handle::try_current().is_ok_and(|handle| {
            matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            )
        });
        let admit = crate::warm_admit::current_code();
        let disk_reads = crate::read_receipts::current();
        let queued = disk_reads.as_ref().map(|_| std::time::Instant::now());
        let observed_work = move || {
            crate::read_receipts::with_sync(disk_reads, || {
                if let Some(queued) = queued {
                    laststore::record_queue_wait(queued.elapsed());
                }
                laststore::with_admit_code(admit, || laststore::observe_read_activity(work))
            })
        };
        let (result, activity) = if can_block {
            tokio::task::block_in_place(observed_work)
        } else {
            tokio::task::spawn_blocking(observed_work).await?
        };
        // Restore attribution in the caller's task-local scope after the
        // synchronous storage call. A store-wide delta would charge peers.
        crate::request_phases::add_counter(
            crate::request_phases::RequestCounter::PartitionReadRejections,
            activity.partition_read_rejections,
        );
        crate::request_phases::add_counter(
            crate::request_phases::RequestCounter::AllGroupWalks,
            activity.all_group_walks,
        );
        result
    }

    pub(super) fn encode_key(key: &[u8]) -> String {
        if let Ok(key) = std::str::from_utf8(key) {
            if !key.starts_with("b64:") {
                return key.to_string();
            }
        }

        format!("b64:{}", STANDARD.encode(key))
    }

    pub(super) fn decode_key(id: &str) -> StorageResult<Vec<u8>> {
        let Some(encoded) = id.strip_prefix("b64:") else {
            return Ok(id.as_bytes().to_vec());
        };
        STANDARD
            .decode(encoded)
            .map_err(|e| StorageError::BackendError(format!("laststore key decode failed: {e}")))
    }
}
