//! Byte formatting and current response parsing.

pub(crate) fn format_bytes(n: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let f = n as f64;
    if f >= GIB {
        format!("{:.2} GiB", f / GIB)
    } else if f >= MIB {
        format!("{:.1} MiB", f / MIB)
    } else if f >= KIB {
        format!("{:.1} KiB", f / KIB)
    } else {
        format!("{n} B")
    }
}

pub(crate) fn parse_json_response(
    response: &str,
    label: &str,
) -> Result<serde_json::Value, String> {
    let status_line = response.lines().next().unwrap_or("<empty response>");
    if !response.starts_with("HTTP/1.1 200 ") {
        let body = response
            .split_once("\r\n\r\n")
            .map_or("", |(_, b)| b.trim());
        return Err(format!("{label} returned {status_line}: {body}"));
    }
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| format!("{label} response had no body"))?;
    serde_json::from_str(body).map_err(|e| format!("invalid {label} JSON: {e}"))
}

pub(crate) fn parse_binary_response(response: &[u8], label: &str) -> Result<Vec<u8>, String> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| format!("{label} response had no headers"))?;
    let header = std::str::from_utf8(&response[..header_end])
        .map_err(|e| format!("invalid {label} response headers: {e}"))?;
    let status_line = header.lines().next().unwrap_or("<empty response>");
    if !status_line.starts_with("HTTP/1.1 200 ") {
        let body = String::from_utf8_lossy(&response[header_end + 4..]);
        return Err(format!("{label} returned {status_line}: {}", body.trim()));
    }
    Ok(response[header_end + 4..].to_vec())
}
