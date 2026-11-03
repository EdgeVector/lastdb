//! Write scopes.

use super::*;
use futures::{future::BoxFuture, FutureExt};

impl RecordStore {
    /// Run `work` inside one native write request scope. The scope holds the
    /// write gate from its first read to the accept of its one batch, so a
    /// read-modify-write inside it cannot interleave with another writer.
    pub(crate) fn in_write_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl std::future::Future<Output = Result<T, crate::schema::SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, crate::schema::SchemaError>> {
        crate::storage::disk_pack::transaction::scope(
            self.namespaced_store.disk_pack_runtime(),
            work,
        )
        .boxed()
    }

    /// Run `work` inside one native write request scope of a background job.
    /// It holds the write gate like [`Self::in_write_scope`], and it returns
    /// after the batch is durable. Unreserved background work shares the
    /// current collection window instead of sealing one for each batch.
    pub(crate) fn in_background_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl std::future::Future<Output = Result<T, crate::schema::SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, crate::schema::SchemaError>> {
        crate::storage::disk_pack::transaction::background_scope(
            self.namespaced_store.disk_pack_runtime(),
            work,
        )
        .boxed()
    }
}
