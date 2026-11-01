//! Opt-in capture of certified final fields from the ordinary native request.

mod admission;
pub(super) use admission::require_plain_schema;
mod certify;
mod collector;
pub(super) use collector::collect;
mod records;

use super::MutationManager;
use crate::schema::types::Mutation;
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use std::collections::BTreeSet;
use std::sync::Arc;

pub(super) const MAX_MUTATIONS: usize = 128;
pub(super) const MAX_OUTCOMES: usize = 1024;
pub(super) const MAX_REQUEST_BYTES: usize = 256 * 1024;

tokio::task_local! {
    static ADMITTED: BTreeSet<String>;
}

pub(super) async fn with_admission<T>(
    identities: Option<BTreeSet<String>>,
    future: impl std::future::Future<Output = T>,
) -> T {
    match identities {
        Some(identities) => ADMITTED.scope(identities, future).await,
        None => future.await,
    }
}

pub(super) fn unsupported(reason: &str) -> SchemaError {
    SchemaError::InvalidData(format!("logical capture v1 does not support {reason}"))
}

impl MutationManager {
    fn logical_capture_runtime(&self) -> Option<Arc<PackRuntime>> {
        self.db_ops
            .namespaced_store()
            .disk_pack_runtime()
            .filter(|runtime| runtime.logical_capture_enabled())
    }

    pub(super) fn require_native_logical_admission(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        if self.logical_capture_runtime().is_none() {
            return Ok(());
        }
        let admitted = ADMITTED
            .try_with(|ids| {
                mutations.iter().all(|mutation| {
                    ids.contains(&mutation.uuid)
                        && mutation.author_clock_writer_id == self.signer.public_key_base64()
                        && mutation.imported_written_at.is_some()
                        && mutation.provenance.is_none()
                        && !mutation.aggregate_derived_internal
                        && mutation.aggregate_set.is_none()
                })
            })
            .unwrap_or(false);
        if !admitted {
            return Err(unsupported("internal or imported write paths"));
        }
        Ok(())
    }
}
