//! Streaming `reqwest::Body` constructors for file and in-memory blob uploads. Each counts the
//! bytes the transport has taken, so a stalled upload can be told from a slow one.

use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Bytes read or sliced per chunk of an upload.
const CHUNK_BYTES: usize = 64 * 1024;

pub enum UploadSource {
    File {
        file: Arc<std::fs::File>,
        start: u64,
        size: u64,
    },
    Bytes(Arc<[u8]>),
}

impl UploadSource {
    pub fn body(&self, taken: Arc<AtomicU64>) -> reqwest::Body {
        match self {
            Self::File { file, start, size } => file_body(Arc::clone(file), *start, *size, taken),
            Self::Bytes(data) => bytes_body(Arc::clone(data), taken),
        }
    }
}

/// An upload body that reads `[start .. start + size]` via positional `read_at`
/// so concurrent or cancelled tasks never share kernel `f_pos`.
fn file_body(
    file: Arc<std::fs::File>,
    start: u64,
    size: u64,
    taken: Arc<AtomicU64>,
) -> reqwest::Body {
    let end = start.saturating_add(size);
    let chunks = futures::stream::unfold(start, move |offset| {
        let file = Arc::clone(&file);
        let taken = Arc::clone(&taken);
        async move {
            if offset >= end {
                return None;
            }
            let want = ((end - offset).min(CHUNK_BYTES as u64)) as usize;
            let read = tokio::task::spawn_blocking(move || read_at_chunk(&file, offset, want))
                .await
                .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())));
            match read {
                Ok(buf) if buf.is_empty() => None,
                Ok(buf) => {
                    let next = offset + buf.len() as u64;
                    taken.fetch_add(buf.len() as u64, Ordering::Relaxed);
                    Some((Ok::<Vec<u8>, std::io::Error>(buf), next))
                }
                Err(error) => Some((Err(error), end)),
            }
        }
    });
    reqwest::Body::wrap_stream(chunks)
}

fn bytes_body(data: Arc<[u8]>, taken: Arc<AtomicU64>) -> reqwest::Body {
    let end = data.len();
    let chunks = futures::stream::unfold(0usize, move |offset| {
        let data = Arc::clone(&data);
        let taken = Arc::clone(&taken);
        async move {
            if offset >= end {
                return None;
            }
            let next = (offset + CHUNK_BYTES).min(end);
            let buf = data[offset..next].to_vec();
            taken.fetch_add(buf.len() as u64, Ordering::Relaxed);
            Some((Ok::<Vec<u8>, std::io::Error>(buf), next))
        }
    });
    reqwest::Body::wrap_stream(chunks)
}

fn read_at_chunk(file: &std::fs::File, offset: u64, want: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; want];
    loop {
        match file.read_at(&mut buf, offset) {
            Ok(n) => {
                buf.truncate(n);
                return Ok(buf);
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}
