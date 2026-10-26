//! Bounded namespace removal is one complete named-key mutation.

use super::*;

impl PackRuntime {
    pub(super) async fn delete_namespace(self: &Arc<Self>, name: &str) -> StorageResult<bool> {
        if name.len() > 65531 || name == REGISTRY {
            return Err(StorageError::InvalidOperation(
                "invalid native namespace identity".into(),
            ));
        }
        transaction::scope(Some(Arc::clone(self)), async {
            let context = read::ReadContext::current(self).await?;
            let pending = transaction::PendingBatch::current(self);
            let registry_key = format!("{NAMESPACES}{name}");
            if self
                .raw_get(&context, REGISTRY, &registry_key, pending.as_ref())?
                .is_none()
            {
                return Ok(false);
            }
            let runtime = Arc::clone(self);
            let collection = name.to_string();
            let operations = super::run_blocking(move || {
                let (rows, complete) = runtime.raw_rows(
                    &context,
                    &collection,
                    b"",
                    b"\xff",
                    4095,
                    pending.as_ref(),
                )?;
                if !complete {
                    return Err(StorageError::WriteResourceLimit {
                        detail: "native namespace removal exceeds its complete 4095-key plan"
                            .into(),
                    });
                }
                let mut operations: Vec<_> = rows
                    .into_iter()
                    .map(|(key, _)| HomeMutation {
                        collection: collection.clone(),
                        key,
                        body: None,
                    })
                    .collect();
                operations.push(HomeMutation {
                    collection: REGISTRY.into(),
                    key: registry_key,
                    body: None,
                });
                Ok(operations)
            })
            .await?;
            self.mutate(operations).await?;
            Ok(true)
        })
        .await
    }
}
