//! Ordered schema and membership bodies captured outside logical mutations.

use super::*;
use crate::sync::log::{LogOp, LogicalChange};

impl MutationLogCaptureKvStore {
    pub(super) async fn capture_applyable_mixed(
        &self,
        mutations: Vec<KvMutation>,
    ) -> StorageResult<()> {
        let mut keys = Vec::new();
        let mut changes = Vec::new();
        let mut digests = Vec::new();
        for mutation in &mutations {
            let (key, value) = match mutation {
                KvMutation::Put { key, value } => (key, Some(value)),
                KvMutation::Delete { key } => (key, None),
            };
            if policy::capture_should_skip_key(&self.namespace, key) {
                continue;
            }
            keys.push(key.clone());
            if let Some(value) =
                value.filter(|_| !policy::leftover_capture_is_applyable(&self.namespace, key))
            {
                digests.push((key.clone(), leftover_body_digest(value)));
            } else {
                changes.push(LogicalChange {
                    namespace: self.namespace.clone(),
                    key: LogOp::encode_bytes(key),
                    value: value.map(|value| LogOp::encode_bytes(value)),
                });
            }
        }
        let reservation = if keys.is_empty() {
            None
        } else {
            self.router.reserve_capture().await?
        };
        self.inner.batch_mutate(mutations).await?;
        if let Some(reservation) = reservation {
            reservation.submit(CaptureJobKind::ApplyableMixed {
                namespace: self.namespace.clone(),
                keys,
                changes,
                digests,
            });
        }
        Ok(())
    }
}

pub(super) async fn process_catalog_capture(
    engine: &Arc<SyncEngine>,
    namespace: &str,
    keys: &[Vec<u8>],
    op: LogOp,
    digests: &[(Vec<u8>, [u8; 32])],
    operation: &'static str,
) {
    let stage_started = std::time::Instant::now();
    let marker = stage_marker(engine, namespace, keys).await;
    engine.capture_stage_us.fetch_add(
        elapsed_micros(stage_started),
        std::sync::atomic::Ordering::Relaxed,
    );
    let record_started = std::time::Instant::now();
    let result = async {
        let recorded = engine.record_op(op).await?;
        if !digests.is_empty() {
            engine.record_physical_digest(namespace, digests).await?;
        }
        Ok(recorded)
    }
    .await;
    engine.capture_record_us.fetch_add(
        elapsed_micros(record_started),
        std::sync::atomic::Ordering::Relaxed,
    );
    finish_capture(engine, namespace, marker, result, operation).await;
}

pub(super) async fn process_catalog_put(
    engine: &Arc<SyncEngine>,
    namespace: &str,
    items: &[(Vec<u8>, Vec<u8>)],
    digests: &[(Vec<u8>, [u8; 32])],
) {
    let keys: Vec<Vec<u8>> = items
        .iter()
        .map(|(key, _)| key.clone())
        .chain(digests.iter().map(|(key, _)| key.clone()))
        .collect();
    let op = if items.len() == 1 {
        LogOp::Put {
            namespace: namespace.to_string(),
            key: LogOp::encode_bytes(&items[0].0),
            value: LogOp::encode_bytes(&items[0].1),
        }
    } else {
        LogOp::BatchPut {
            namespace: namespace.to_string(),
            items: items
                .iter()
                .map(|(key, value)| (LogOp::encode_bytes(key), LogOp::encode_bytes(value)))
                .collect(),
        }
    };
    process_catalog_capture(engine, namespace, &keys, op, digests, "applyable_put").await;
}
