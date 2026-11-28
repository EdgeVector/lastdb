//! The presigned transfers of [`RelayClient`] through [`relay_transfer`], the node's own uploader,
//! so this proof measures what users run.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use reqwest::header::{HeaderName, HeaderValue};

use crate::relay::RelayClient;

impl RelayClient {
    /// The presigned PUT through [`relay_transfer::put_presigned`]: stall detection, a fresh
    /// connection and a bounded retry. Every try is printed to stderr and returned.
    pub(crate) async fn upload(
        &self,
        url: &str,
        headers: &BTreeMap<String, String>,
        body: Arc<[u8]>,
        size: u64,
    ) -> Result<Vec<relay_transfer::Attempt>, String> {
        let mut signed = Vec::with_capacity(headers.len());
        for (name, value) in headers {
            if name.eq_ignore_ascii_case("content-length") {
                continue;
            }
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| format!("presigned header name {name}: {e}"))?;
            let value = HeaderValue::from_str(value)
                .map_err(|e| format!("presigned header {name}: {e}"))?;
            signed.push((name, value));
        }
        let client = relay_transfer::upload_client()?;
        let source = relay_transfer::UploadSource::Bytes(body);
        let place = relay_transfer::place(url);
        let note = |tried: u32, attempt: &relay_transfer::Attempt| {
            eprintln!(
                "blob upload try {tried} to {place}: {} after {:.3}s with {} of {size} bytes sent; trying again",
                attempt.outcome, attempt.seconds, attempt.sent_bytes
            );
        };
        let uploaded = relay_transfer::put_presigned(&client, url, &signed, &source, size, &note)
            .await
            .map_err(|failed| format!("{} ({} tries)", failed.message, failed.attempts.len()))?;
        if !uploaded.status.is_success() {
            return Err(format!(
                "blob upload HTTP {} ({} tries)",
                uploaded.status,
                uploaded.attempts.len()
            ));
        }
        Ok(uploaded.attempts)
    }

    pub(crate) async fn download(
        &self,
        url: &str,
        headers: &BTreeMap<String, String>,
    ) -> Result<Bytes, String> {
        let client = relay_transfer::download_client()?;
        let mut last = String::new();
        for tried in 1..=relay_transfer::UPLOAD_TRIES {
            if tried > 1 {
                tokio::time::sleep(relay_transfer::upload_backoff(tried - 1)).await;
            }
            let mut req = client.get(url);
            for (name, value) in headers {
                req = req.header(name.as_str(), value.as_str());
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                    Ok(bytes) => return Ok(bytes),
                    Err(e) => {
                        last = format!("blob download body: {}", relay_transfer::describe(e, url));
                    }
                },
                Ok(resp) => {
                    let status = resp.status();
                    last = format!("blob download HTTP {status}");
                    if !relay_transfer::is_retryable_store_status(status) {
                        return Err(last);
                    }
                }
                Err(e) => last = format!("blob download: {}", relay_transfer::describe(e, url)),
            }
            eprintln!("blob download try {tried}: {last}");
        }
        Err(last)
    }
}

/// The stages of one `blob.put`: begin and commit are relay calls, put is the presigned upload.
#[derive(Debug, Default)]
pub struct BlobPutTiming {
    pub begin_seconds: f64,
    pub put_seconds: f64,
    pub commit_seconds: f64,
    /// Every try of the presigned upload; empty when the relay already held the blob.
    pub attempts: Vec<relay_transfer::Attempt>,
}
