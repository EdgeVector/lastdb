//! Wall-clock helpers and environment-variable scalar/flag parsers.

pub mod clock {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn since_epoch() -> Duration {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
    }

    pub fn unix_secs() -> u64 {
        since_epoch().as_secs()
    }

    pub fn unix_millis() -> u64 {
        u64::try_from(since_epoch().as_millis()).unwrap_or(u64::MAX)
    }

    pub fn unix_nanos() -> u64 {
        u64::try_from(since_epoch().as_nanos()).unwrap_or(u64::MAX)
    }

    pub fn unix_nanos_wide() -> u128 {
        since_epoch().as_nanos()
    }
}

pub mod env_flag {
    #[must_use]
    pub fn truthy(raw: &str) -> bool {
        matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    }

    #[must_use]
    pub fn var_truthy(name: &str) -> bool {
        std::env::var(name).ok().is_some_and(|v| truthy(&v))
    }

    #[must_use]
    pub fn var_parsed<T: std::str::FromStr>(name: &str) -> Option<T> {
        std::env::var(name).ok()?.trim().parse().ok()
    }

    #[must_use]
    pub fn var_or<T: std::str::FromStr>(name: &str, default: T) -> T {
        var_parsed(name).unwrap_or(default)
    }
}
