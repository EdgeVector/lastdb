//! Shared telemetry substrate for the LastDB workspace.
#![warn(missing_docs)]

pub mod error_layer;
pub mod init;

/// Panic marker hooks and release version sanitization.
pub mod crash {
    use std::io::Write;

    /// Emit only a fixed marker to stderr.
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
            if part.is_empty() || part.len() > 6 || !part.bytes().all(|byte| byte.is_ascii_digit())
            {
                return None;
            }
        }
        parts.next().is_none().then_some(version)
    }
}

/// Subscriber layer namespaces.
pub mod layers {
    /// Optional Sentry ERROR sink.
    pub mod error {
        pub use crate::error_layer::*;
    }
}

/// Outbound HTTP trace propagation hook.
pub mod propagation {
    /// Pass-through hook at outgoing HTTP call sites.
    pub fn inject_w3c<T>(builder: T) -> T {
        builder
    }
}

/// UTF-8-safe string truncation for log call sites.
pub mod truncate {
    /// Truncate `s` to at most `max_bytes`, snapping down to the nearest UTF-8 char boundary.
    pub fn truncate_on_char_boundary(s: &str, max_bytes: usize) -> &str {
        if s.len() <= max_bytes {
            return s;
        }
        let mut end = max_bytes;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    }
}

pub use init::{init_lambda, ObsGuard};

/// Errors raised by `init_lambda`.
#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    /// `init_lambda` was called more than once in the same process.
    #[error("observability already initialized")]
    AlreadyInitialized,
    /// Could not install the global tracing subscriber.
    #[error("failed to install tracing subscriber: {0}")]
    SubscriberInstall(String),
    /// Could not open or write to the configured sink.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
