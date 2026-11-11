//! Bounded file ownership crosses into one blocking feeder, plus shared log append.
use super::{bounded, BackupTransport, BackupTransportError, Result};
use crate::cloud_backup_client::shared_log::AppendOutcome;
use cloud_backup_protocol::shared::{BatchReceipt, LogBatch, LogHead, APPEND_PATH};
use cloud_backup_protocol::{canonical_bytes, limits, Digest, ProtocolError};
use reqwest::{header, StatusCode};
use sha2::{Digest as _, Sha256};
use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit};

struct Stop {
    stopped: AtomicBool,
    changed: Notify,
}
pub(super) struct Cancel(Arc<Stop>);
impl Cancel {
    pub fn stop(&self) {
        self.0.stopped.store(true, Ordering::Release);
        self.0.changed.notify_one();
    }
}
impl Drop for Cancel {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(super) fn start(
    source: Box<dyn Read + Send>,
    bytes: u64,
    expected: Digest,
    permit: OwnedSemaphorePermit,
) -> (reqwest::Body, tokio::task::JoinHandle<Result<()>>, Cancel) {
    let (sender, receiver) = mpsc::channel::<io::Result<Vec<u8>>>(1);
    let cancel = Arc::new(Stop {
        stopped: AtomicBool::new(false),
        changed: Notify::new(),
    });
    let stopped = Arc::clone(&cancel);
    let span = tracing::Span::current();
    let task = tokio::task::spawn_blocking(move || {
        span.in_scope(|| {
            let _permit = permit;
            let result = produce(source, bytes, &expected, &sender, &stopped);
            if result.is_err() && !stopped.stopped.load(Ordering::Acquire) {
                let _ = send(
                    &sender,
                    Err(io::Error::other("native backup source failed")),
                    &stopped,
                );
            }
            result
        })
    });
    let stream = futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    });
    (reqwest::Body::wrap_stream(stream), task, Cancel(cancel))
}

fn produce(
    mut source: Box<dyn Read + Send>,
    bytes: u64,
    expected: &Digest,
    sender: &mpsc::Sender<io::Result<Vec<u8>>>,
    cancel: &Stop,
) -> Result<()> {
    let mut left = bytes;
    let mut hash = Sha256::new();
    while left > 0 {
        if cancel.stopped.load(Ordering::Acquire) {
            return Err(BackupTransportError::Transport);
        }
        let size = left.min(fold_db::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes) as usize;
        let mut bytes = vec![0u8; size];
        source
            .read_exact(&mut bytes)
            .map_err(|_| BackupTransportError::Source)?;
        hash.update(&bytes);
        left -= size as u64;
        send(sender, Ok(bytes), cancel)?;
    }
    let mut extra = [0u8; 1];
    if source
        .read(&mut extra)
        .map_err(|_| BackupTransportError::Source)?
        != 0
        || fold_db::hex::hex_lower(hash.finalize()) != expected.as_str()
    {
        return Err(BackupTransportError::Integrity("upload source"));
    }
    Ok(())
}

fn send(
    sender: &mpsc::Sender<io::Result<Vec<u8>>>,
    item: io::Result<Vec<u8>>,
    stop: &Stop,
) -> Result<()> {
    futures::executor::block_on(async {
        if stop.stopped.load(Ordering::Acquire) {
            return Err(BackupTransportError::Transport);
        }
        tokio::select! {
            _ = stop.changed.notified() => Err(BackupTransportError::Transport),
            result = sender.send(item) => result.map_err(|_| BackupTransportError::Transport),
        }
    })
}

impl BackupTransport {
    pub(crate) async fn post_shared_append(
        &self,
        batch: &LogBatch,
        headers: header::HeaderMap,
    ) -> Result<AppendOutcome> {
        let bytes = canonical_bytes(batch)?;
        if bytes.len() as u64 > limits::REQUEST_JSON_MAX_BYTES {
            return Err(BackupTransportError::Configuration("request bytes"));
        }
        let request = self
            .http
            .post(self.api_endpoint(APPEND_PATH.trim_start_matches('/')))
            .headers(headers)
            .header(header::CONTENT_TYPE, "application/json")
            .body(bytes);
        let request = self.apply_auth(request);
        tokio::time::timeout(self.timeout, async {
            let response = observability::propagation::inject_w3c(request)
                .send()
                .await
                .map_err(|_| BackupTransportError::Transport)?;
            read_reply(response, batch).await
        })
        .await
        .map_err(|_| BackupTransportError::Timeout)?
    }
}

async fn read_reply(response: reqwest::Response, batch: &LogBatch) -> Result<AppendOutcome> {
    let status = response.status();
    let reconcile = status == StatusCode::CONFLICT
        && response
            .headers()
            .get("x-lastdb-reconcile")
            .is_some_and(|value| value == "writer-head");
    let body = bounded(response, limits::REQUEST_JSON_MAX_BYTES).await?;
    if status == StatusCode::UNAUTHORIZED {
        return Err(BackupTransportError::Authentication);
    }
    if let Ok(error) = serde_json::from_slice::<ProtocolError>(&body) {
        return Err(BackupTransportError::Protocol(error));
    }
    if reconcile {
        let head: LogHead = serde_json::from_slice(&body)
            .map_err(|_| BackupTransportError::InvalidResponse("shared writer head"))?;
        head.validate()?;
        if head.sequence < batch.through {
            return Err(BackupTransportError::Integrity("shared writer head"));
        }
        return Ok(AppendOutcome::Reconcile(head));
    }
    if status != StatusCode::OK {
        return Err(BackupTransportError::Http(status.as_u16()));
    }
    let receipt: BatchReceipt = serde_json::from_slice(&body)
        .map_err(|_| BackupTransportError::InvalidResponse("shared batch receipt"))?;
    receipt
        .validate_for(batch)
        .map_err(|_| BackupTransportError::Integrity("shared batch receipt"))?;
    Ok(AppendOutcome::Accepted(receipt))
}
