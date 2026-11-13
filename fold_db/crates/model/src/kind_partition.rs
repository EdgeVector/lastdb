//! Storage-key layouts and half-open prefix bounds.

#[must_use]
pub fn anchored(kind: &str, rest: &str) -> String {
    format!("{kind}\0{rest}")
}

#[must_use]
pub fn exact_prefix_bounds(prefix: &str) -> (String, String) {
    let mut bytes = prefix.as_bytes().to_vec();
    match bytes.last_mut() {
        Some(last) if *last < 0xff => *last += 1,
        Some(_) => bytes.push(0),
        None => {}
    }
    let end = String::from_utf8(bytes)
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
    (prefix.to_owned(), end)
}
