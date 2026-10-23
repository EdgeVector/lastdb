//! Bounded file ownership crosses into one blocking feeder, never a whole-file Vec.
use super::*;
use sha2::{Digest as _, Sha256};
use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
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
    input: &cloud_backup_protocol::transfer::TransferInput,
    permit: OwnedSemaphorePermit,
) -> (reqwest::Body, tokio::task::JoinHandle<Result<()>>, Cancel) {
    let (sender, receiver) = mpsc::channel::<io::Result<Vec<u8>>>(1);
    let cancel = Arc::new(Stop {
        stopped: AtomicBool::new(false),
        changed: Notify::new(),
    });
    let stopped = Arc::clone(&cancel);
    let expected = input.object.clone();
    let span = tracing::Span::current();
    let task = tokio::task::spawn_blocking(move || {
        span.in_scope(|| {
            let _permit = permit;
            let result = produce(source, &expected, &sender, &stopped);
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
    object: &cloud_backup_protocol::ObjectIdentity,
    sender: &mpsc::Sender<io::Result<Vec<u8>>>,
    cancel: &Stop,
) -> Result<()> {
    let mut left = object.bytes.get();
    let mut hash = Sha256::new();
    while left > 0 {
        if cancel.stopped.load(Ordering::Acquire) {
            return Err(NativeBackupError::Transport);
        }
        let size = left.min(lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes) as usize;
        let mut bytes = vec![0u8; size];
        source
            .read_exact(&mut bytes)
            .map_err(|_| NativeBackupError::Source)?;
        hash.update(&bytes);
        left -= size as u64;
        send(sender, Ok(bytes), cancel)?;
    }
    let mut extra = [0u8; 1];
    if source
        .read(&mut extra)
        .map_err(|_| NativeBackupError::Source)?
        != 0
        || crate::hex::hex_lower(hash.finalize()) != object.sha256.as_str()
    {
        return Err(NativeBackupError::Integrity("upload source"));
    }
    Ok(())
}

fn send(
    sender: &mpsc::Sender<io::Result<Vec<u8>>>,
    item: io::Result<Vec<u8>>,
    stop: &Stop,
) -> Result<()> {
    // A provider can answer before it consumes the outgoing body. Cancellation
    // must wake a full channel even if that HTTP body is never polled again.
    futures::executor::block_on(async {
        if stop.stopped.load(Ordering::Acquire) {
            return Err(NativeBackupError::Transport);
        }
        tokio::select! {
            _ = stop.changed.notified() => Err(NativeBackupError::Transport),
            result = sender.send(item) => result.map_err(|_| NativeBackupError::Transport),
        }
    })
}
