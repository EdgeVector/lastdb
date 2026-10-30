//! Final allowlist for events that leave the machine.

use std::collections::BTreeMap;

use sentry::protocol::Event;

/// Rebuild the event instead of trying to enumerate every sensitive field.
/// Scope data, integrations, exceptions, and arbitrary log values cannot pass.
pub(super) fn event(source: &Event<'_>) -> Event<'static> {
    let category = match source.tags.get("crash_kind").map(String::as_str) {
        Some("panic") => "panic",
        Some("startup_failed") => "startup_failed",
        Some("unclean_exit") => "unclean_exit",
        _ => "application_error",
    };
    Event {
        level: source.level,
        platform: "native".into(),
        message: Some(format!("Application fault: {category}")),
        fingerprint: vec!["private_fault".into(), category.into()].into(),
        release: source
            .release
            .as_deref()
            .and_then(package_version)
            .map(Into::into),
        environment: source.environment.as_deref().and_then(|value| match value {
            "production" | "development" | "prod" | "dev" | "staging" => {
                Some(value.to_string().into())
            }
            _ => None,
        }),
        tags: BTreeMap::from([
            ("fault_category".to_string(), category.to_string()),
            ("os".to_string(), std::env::consts::OS.to_string()),
        ]),
        ..Default::default()
    }
}

/// Retain only a numeric package version, never operator-defined suffixes.
fn package_version(release: &str) -> Option<String> {
    crate::crash::numeric_version(release).map(str::to_string)
}
