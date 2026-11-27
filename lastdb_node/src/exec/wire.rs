//! Query-string helpers for the routes that remain on the owner socket.

use base64::Engine as _;
use fold_db::schema::types::{Query, QueryCursor};
use serde_json::Value;

use super::query::QueryResponseFormat;
use super::reply::HostError;

pub(crate) struct QueryRequest {
    pub(in crate::exec) query: Query,
    pub(in crate::exec) limit: Option<usize>,
    pub(in crate::exec) offset: Option<usize>,
    pub(in crate::exec) cursor: Option<QueryCursor>,
    pub(in crate::exec) response_format: QueryResponseFormat,
}

pub(in crate::exec) fn decode_cursor_token(
    raw: Option<&str>,
    prefix: &str,
    err_msg: &'static str,
) -> Result<Option<QueryCursor>, HostError> {
    raw.map(|enc| {
        let bytes = enc
            .strip_prefix(prefix)
            .and_then(|s| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(s)
                    .ok()
            })
            .ok_or_else(|| HostError::new(400, err_msg))?;
        serde_json::from_slice(&bytes).map_err(|_| HostError::new(400, err_msg))
    })
    .transpose()
}

pub(in crate::exec) fn encode_cursor_token(
    next: &Value,
    prefix: &str,
    err_msg: &'static str,
) -> Result<Option<String>, HostError> {
    if next.is_null() {
        return Ok(None);
    }
    serde_json::to_vec(next)
        .map(|b| {
            let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b);
            Some(if prefix.is_empty() {
                token
            } else {
                format!("{prefix}{token}")
            })
        })
        .map_err(|_| HostError::internal(err_msg))
}

pub(in crate::exec) fn query_pairs(target: &str) -> impl Iterator<Item = (&str, String)> {
    target
        .split_once('?')
        .map_or("", |(_, query)| query)
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, raw) = pair.split_once('=').unwrap_or((pair, ""));
            (key, percent_decode(raw))
        })
}

pub(in crate::exec) fn query_flag(target: &str, key: &str) -> bool {
    query_value(target, key).is_some_and(|v| fold_db::env_flag::truthy(&v))
}

pub(in crate::exec) fn query_value(target: &str, key: &str) -> Option<String> {
    query_pairs(target).find_map(|(k, v)| (k == key).then_some(v))
}

pub(in crate::exec) fn percent_decode(raw: &str) -> String {
    let (b, mut out, mut i) = (raw.as_bytes(), Vec::with_capacity(raw.len()), 0);
    while i < b.len() {
        if b[i] == b'+' {
            out.push(b' ');
        } else if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (
                (b[i + 1] as char).to_digit(16),
                (b[i + 2] as char).to_digit(16),
            ) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
            out.push(b'%');
        } else {
            out.push(b[i]);
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub(in crate::exec) fn schema_name_from_target(target: &str) -> Option<String> {
    let end = target.find(['?', '#']).unwrap_or(target.len());
    let raw = target[..end].strip_prefix("/api/schema/")?;
    (!raw.is_empty() && !raw.contains('/')).then(|| percent_decode(raw))
}
