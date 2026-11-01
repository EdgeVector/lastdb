use super::*;
use futures::StreamExt;
use sha2::{Digest as _, Sha256};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pub type ObjectStream = Pin<Box<dyn AsyncRead + Send>>;

impl BackupTransport {
    pub(crate) async fn open(
        &self,
        object: StoredObject<'_>,
        grant: TemporaryGrant<'_>,
    ) -> Result<ObjectStream> {
        let url = urls::signed(grant.url, false)?;
        let budget = self.transfer_timeout(object.bytes, grant.expires_ms)?;
        let deadline = tokio::time::Instant::now() + budget;
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.uploads).acquire_owned())
            .await
            .map_err(|_| BackupTransportError::Timeout)?
            .map_err(|_| BackupTransportError::Task)?;
        let response = tokio::time::timeout_at(deadline, self.http.get(url).send())
            .await
            .map_err(|_| BackupTransportError::Timeout)?
            .map_err(|_| BackupTransportError::Transport)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(BackupTransportError::Http(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|bytes| bytes != object.bytes)
        {
            return Err(BackupTransportError::Integrity("download length"));
        }
        let stream = response
            .bytes_stream()
            .map(|item| item.map_err(|_| io::Error::other("native object transport failed")));
        let input = tokio_util::io::StreamReader::new(stream);
        Ok(Box::pin(Bounded {
            input: Box::pin(input),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            left: object.bytes,
            hash: Sha256::new(),
            expected: object.sha256.clone(),
            finished: false,
            _permit: permit,
        }))
    }
}

struct Bounded {
    input: ObjectStream,
    deadline: Pin<Box<tokio::time::Sleep>>,
    left: u64,
    hash: Sha256,
    expected: cloud_backup_protocol::Digest,
    finished: bool,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl AsyncRead for Bounded {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 || this.finished {
            return Poll::Ready(Ok(()));
        }
        if this.deadline.as_mut().poll(cx).is_ready() {
            this.finished = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "native object deadline",
            )));
        }
        if this.left == 0 {
            return this.eof(cx);
        }
        let maximum = this
            .left
            .min(output.remaining() as u64)
            .min(lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes)
            as usize;
        let count = {
            let mut buffer = ReadBuf::new(output.initialize_unfilled_to(maximum));
            match this.input.as_mut().poll_read(cx, &mut buffer) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => {
                    this.finished = true;
                    return Poll::Ready(Err(io::Error::other("native object read failed")));
                }
                Poll::Ready(Ok(())) => {}
            }
            this.hash.update(buffer.filled());
            buffer.filled().len()
        };
        if count == 0 {
            this.finished = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "native object truncated",
            )));
        }
        this.left -= count as u64;
        output.advance(count);
        Poll::Ready(Ok(()))
    }
}
impl Bounded {
    fn eof(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut extra = [0u8; 1];
        let mut buffer = ReadBuf::new(&mut extra);
        match self.input.as_mut().poll_read(cx, &mut buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.finished = true;
                if result.is_err()
                    || !buffer.filled().is_empty()
                    || lastdb_model::hex::hex_lower(self.hash.clone().finalize())
                        != self.expected.as_str()
                {
                    Poll::Ready(Err(io::Error::other("native object bytes differ")))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
}
