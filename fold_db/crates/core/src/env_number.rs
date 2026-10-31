//! One classification of a trimmed environment number.
//!
//! Missing covers an unset value and an empty value. The raw text stays so a
//! caller can log the trimmed input. A zero stays inside Value when the caller
//! treats zero as its own case.

/// The trimmed number, or the reason it is not a number.
pub(crate) enum EnvNumber<'a> {
    /// The variable is unset or empty after trim.
    Missing,
    /// The trimmed text parses as a `u64`. `raw` is that trimmed text.
    Value { raw: &'a str, value: u64 },
    /// The trimmed text does not parse as a `u64`.
    Invalid { raw: &'a str },
}

/// Classify `raw`. Empty and whitespace-only input are Missing.
pub(crate) fn parse(raw: Option<&str>) -> EnvNumber<'_> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return EnvNumber::Missing;
    };
    match raw.parse::<u64>() {
        Ok(value) => EnvNumber::Value { raw, value },
        Err(_) => EnvNumber::Invalid { raw },
    }
}
