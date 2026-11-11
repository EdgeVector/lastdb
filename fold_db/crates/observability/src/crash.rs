//! Panic marker hook and numeric version sanitizer.

use std::io::Write;

/// Emit only a fixed marker, even when stderr is captured by a service host.
pub fn write_panic_marker() {
    let _ = writeln!(std::io::stderr().lock(), "LastDB panic");
}

/// Install fixed panic output so captured stderr cannot retain raw panic payloads.
pub fn install_private_panic_hook() {
    std::panic::set_hook(Box::new(|_| write_panic_marker()));
}

/// Drop build suffixes and reject arbitrary version text.
#[cfg(feature = "sentry")]
pub(crate) fn numeric_version(release: &str) -> Option<&str> {
    let version = release
        .strip_prefix('v')
        .unwrap_or(release)
        .split('-')
        .next()?;
    let mut parts = version.split('.');
    for _ in 0..3 {
        let part = parts.next()?;
        if part.is_empty() || part.len() > 6 || !part.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
    }
    parts.next().is_none().then_some(version)
}
