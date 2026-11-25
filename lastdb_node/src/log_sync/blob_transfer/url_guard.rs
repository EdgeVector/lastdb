//! Presigned URL and header validation before sending traffic to an untrusted URL.
//!
//! A relay returns presigned upload and download URLs and header maps for object storage.
//! Plain `http://` is permitted only when the configured relay itself is on loopback and the
//! presigned URL shares its exact `(scheme, host, port)` origin (local `reference_relay`).
//! Every other presigned URL must use `https://`, carry no userinfo or fragment, and not address
//! loopback, private, link-local, CGNAT, multicast, unspecified, or unique-local IP literals.
//! Relay-supplied headers are restricted to `content-type`, `x-amz-*`, and `x-bz-*`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use reqwest::header::{HeaderName, HeaderValue};
use serde_json::Value;

const MAX_PRESIGNED_HEADERS: usize = 32;
const MAX_HEADER_NAME_BYTES: usize = 128;
const MAX_HEADER_VALUE_BYTES: usize = 1024;

/// Reject a presigned upload or download URL that violates scheme, origin, or IP-range rules.
pub(in crate::log_sync) fn validate_presigned_url(
    relay_base: &str,
    presigned_url: &str,
) -> Result<(), String> {
    let parsed = reqwest::Url::parse(presigned_url)
        .map_err(|_| "relay returned an invalid presigned URL".to_owned())?;
    if !parsed.username().is_empty() || parsed.password().is_some() || parsed.fragment().is_some() {
        return Err("relay presigned URL carries credentials or a fragment".into());
    }
    let Some(raw_host) = parsed.host_str() else {
        return Err("relay presigned URL has no host".into());
    };
    if is_same_loopback_origin(relay_base, &parsed) {
        return Ok(());
    }
    if parsed.scheme() != "https" {
        return Err("relay presigned URL must use https".into());
    }
    let bare = strip_ipv6_brackets(raw_host);
    if is_localhost_domain(bare) {
        return Err("relay presigned URL targets localhost".into());
    }
    if let Ok(ip) = bare.parse::<IpAddr>() {
        if is_forbidden_ip(ip) {
            return Err("relay presigned URL targets a non-public IP address".into());
        }
    }
    Ok(())
}

/// Validate relay-supplied presigned request headers against the storage header allowlist.
/// For uploads, a relay-supplied `content-length` is ignored because the caller sets the exact
/// file byte length itself.
pub(in crate::log_sync) fn presigned_headers(
    headers: &Value,
    is_upload: bool,
) -> Result<Vec<(HeaderName, HeaderValue)>, String> {
    match headers {
        Value::Null => return Ok(Vec::new()),
        Value::Object(map) => {
            if map.len() > MAX_PRESIGNED_HEADERS || !map.values().all(Value::is_string) {
                return Err("relay returned invalid presigned headers".into());
            }
        }
        _ => return Err("relay returned invalid presigned headers".into()),
    }
    let mut out = Vec::new();
    for (name, value) in crate::log_sync::relay::header_pairs(headers) {
        if is_upload && name.eq_ignore_ascii_case("content-length") {
            continue;
        }
        if !is_allowed_header_name(&name) || value.len() > MAX_HEADER_VALUE_BYTES {
            return Err(format!(
                "relay returned disallowed presigned header '{name}'"
            ));
        }
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| "relay returned an invalid presigned header name".to_owned())?;
        let header_value = HeaderValue::from_str(&value)
            .map_err(|_| "relay returned an invalid presigned header value".to_owned())?;
        out.push((header_name, header_value));
    }
    Ok(out)
}

fn is_same_loopback_origin(relay_base: &str, parsed: &reqwest::Url) -> bool {
    let Ok(base) = reqwest::Url::parse(relay_base) else {
        return false;
    };
    if !matches!(base.scheme(), "http" | "https") || parsed.origin() != base.origin() {
        return false;
    }
    let Some(base_host) = base.host_str() else {
        return false;
    };
    let bare = strip_ipv6_brackets(base_host);
    is_localhost_domain(bare) || bare.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

fn strip_ipv6_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host)
}

fn is_localhost_domain(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost") || host.eq_ignore_ascii_case("localhost.")
}

fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_forbidden_ipv4(v4),
        IpAddr::V6(v6) => is_forbidden_ipv6(v6),
    }
}

fn is_forbidden_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    ip.is_loopback()
        || ip.is_unspecified()
        || octets[0] == 0
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        // RFC 6598 carrier-grade NAT (100.64.0.0/10).
        || (octets[0] == 100 && (octets[1] & 0xc0) == 64)
}

fn is_forbidden_ipv6(ip: Ipv6Addr) -> bool {
    let seg0 = ip.segments()[0];
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        // Link-local fe80::/10 and unique-local fc00::/7.
        || (seg0 & 0xffc0) == 0xfe80
        || (seg0 & 0xfe00) == 0xfc00
        || ip.to_ipv4().is_some_and(is_forbidden_ipv4)
}

fn is_allowed_header_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_HEADER_NAME_BYTES {
        return false;
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        return false;
    }
    let lower = name.to_ascii_lowercase();
    lower == "content-type" || lower.starts_with("x-amz-") || lower.starts_with("x-bz-")
}
