//! Request signing, bounded response reading, and retry classification for [`super::RelayClient`].

use std::time::{Duration, Instant};

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::Engine as _;
use relay_protocol::{headers, Endpoint, ErrorAnswer};
use sha2::{Digest, Sha256};

use super::RelayClient;

/// Bounded retries with 250/500/1000 ms jittered waits (at most 1.75 s total).
const RETRIES: u32 = 3;
const RETRY_BASE: Duration = Duration::from_millis(250);
const FAST_TRANSPORT_LIMIT: Duration = Duration::from_secs(5);
/// Maximum JSON response payload read from `/relay/v1/*` routes (8 MiB above the 6 MiB wire cap).
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// A relay call that failed: the text a caller prints, and the relay's answer when it gave one
/// other than 200.
pub(in crate::log_sync) struct CallError {
    pub(in crate::log_sync) message: String,
    /// The HTTP status of the relay's answer, when it answered other than 200.
    pub(in crate::log_sync) status: Option<u16>,
    /// The body of that answer, when it is an [`ErrorAnswer`].
    pub(in crate::log_sync) answer: Option<ErrorAnswer>,
}

impl CallError {
    fn text(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
            answer: None,
        }
    }

    /// The relay's error answer, when it answered with `status`.
    pub(in crate::log_sync) fn refusal(&self, status: u16) -> Option<&ErrorAnswer> {
        self.answer.as_ref().filter(|_| self.status == Some(status))
    }
}

impl From<CallError> for String {
    fn from(error: CallError) -> Self {
        error.message
    }
}

impl RelayClient {
    /// Send `request` to the route of `E` and parse its 200 answer.
    pub(in crate::log_sync) async fn call<E: Endpoint>(
        &self,
        request: &E::Request,
    ) -> Result<E::Answer, String> {
        self.call_tracked::<E>(request)
            .await
            .map(|(answer, _)| answer)
            .map_err(String::from)
    }

    /// [`Self::call`], with whether the request was sent more than once, and the relay's error
    /// answer on a refusal.
    pub(in crate::log_sync) async fn call_tracked<E: Endpoint>(
        &self,
        request: &E::Request,
    ) -> Result<(E::Answer, bool), CallError> {
        let path = E::ROUTE.path();
        let database_id = E::database_id(request);
        let raw =
            serde_json::to_vec(request).map_err(|_| CallError::text("cannot encode relay body"))?;
        let url = format!("{}{path}", self.base);
        let mut retried = 0;
        loop {
            // Every try is signed again: the relay consumes the nonce of a request it refuses,
            // and it rejects a replayed nonce and a request issued too long ago.
            let headers = sign_headers(
                &self.signing_key,
                &self.node_key_hex,
                &self.device_id,
                database_id,
                path,
                &raw,
            )
            .map_err(CallError::text)?;
            let mut req = self.http.post(&url).body(raw.clone());
            for (name, value) in headers {
                req = req.header(name, value);
            }
            let started = Instant::now();
            let resp = match req.send().await {
                Ok(resp) => resp,
                Err(e) if retried < RETRIES && retryable_transport(&e, started.elapsed()) => {
                    tokio::time::sleep(retry_wait(retried)).await;
                    retried += 1;
                    continue;
                }
                Err(e) => {
                    return Err(CallError::text(format!(
                        "relay request failed: {}",
                        crate::log_sync::blob_transfer::describe(e, &url)
                    )));
                }
            };
            let status = resp.status();
            self.note_answer(status.is_success(), resp.headers());
            let text = match read_bounded_text(resp, path).await {
                Ok(Ok(text)) => text,
                Ok(Err(limit_err)) => return Err(CallError::text(limit_err)),
                Err(e) if retried < RETRIES && retryable_transport(&e, started.elapsed()) => {
                    tokio::time::sleep(retry_wait(retried)).await;
                    retried += 1;
                    continue;
                }
                Err(e) => {
                    return Err(CallError::text(format!(
                        "relay response body: {}",
                        crate::log_sync::blob_transfer::describe(e, &url)
                    )));
                }
            };
            if status.is_success() {
                let answer = serde_json::from_str::<E::Answer>(&text).map_err(|error| {
                    CallError::text(if error.is_data() {
                        format!("relay {path} returned an invalid answer")
                    } else {
                        "relay response is not JSON".to_owned()
                    })
                })?;
                return Ok((answer, retried > 0));
            }
            let answer = serde_json::from_str::<ErrorAnswer>(&text).ok();
            if retried < RETRIES && is_transient(status.as_u16(), answer.as_ref()) {
                tokio::time::sleep(retry_wait(retried)).await;
                retried += 1;
                continue;
            }
            return Err(CallError {
                message: format!("relay {path} HTTP {status}: {text}"),
                status: Some(status.as_u16()),
                answer,
            });
        }
    }
}

async fn read_bounded_text(
    mut resp: reqwest::Response,
    path: &str,
) -> Result<Result<String, String>, reqwest::Error> {
    let mut buf = Vec::new();
    while let Some(chunk) = resp.chunk().await? {
        if buf.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Ok(Err(format!(
                "relay {path} response exceeded {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8(buf).map_err(|_| "relay response is not UTF-8".into()))
}

fn retryable_transport(error: &reqwest::Error, elapsed: Duration) -> bool {
    !error.is_timeout()
        && elapsed < FAST_TRANSPORT_LIMIT
        && (error.is_connect() || error.is_request() || error.is_body())
}

/// Whether the same request can succeed a moment later: a gateway failure (502, 503, 504), or a
/// relay error answer that [`ErrorAnswer::is_transient`] calls transient with the status of its
/// code (`unavailable` 500, `rate_limited` 429 with a provider or failed-read field). A quota or
/// payment refusal is a 429 too, and it is final.
fn is_transient(status: u16, answer: Option<&ErrorAnswer>) -> bool {
    matches!(status, 502..=504)
        || answer.is_some_and(|answer| answer.code.status() == status && answer.is_transient())
}

/// The wait before retry number `retried` (0 first): 250 ms, doubled each time, cut to between
/// half and all of it.
fn retry_wait(retried: u32) -> Duration {
    use rand::Rng;
    let full = RETRY_BASE * 2u32.pow(retried);
    let low = full / 2;
    rand::thread_rng().gen_range(low..=full)
}

fn sign_headers(
    key: &SigningKey,
    node_key_hex: &str,
    device_id: &str,
    database_id: &str,
    path: &str,
    body: &[u8],
) -> Result<Vec<(&'static str, String)>, String> {
    let issued_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or_else(|_| "0".into(), |d| d.as_secs().to_string());
    let nonce = hex_lower(&rand_32());
    let body_hash = hex_lower(&Sha256::digest(body));
    let message = relay_protocol::signed_message(
        path,
        node_key_hex,
        database_id,
        &body_hash,
        &issued_at,
        &nonce,
    );
    let sig = sign(key, message.as_bytes());
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);
    Ok(vec![
        (headers::PROTOCOL, relay_protocol::PROTOCOL.into()),
        (headers::NODE_KEY, node_key_hex.to_owned()),
        (headers::DATABASE_ID, database_id.to_owned()),
        (headers::DEVICE_ID, device_id.to_owned()),
        (headers::ISSUED_AT, issued_at),
        (headers::NONCE, nonce),
        (headers::SIGNATURE, signature),
        ("content-type", "application/json".into()),
    ])
}

fn rand_32() -> [u8; 32] {
    use rand::RngCore;
    let mut out = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut out);
    out
}
