//! Validation only: preserve the original JSON string and never infer a date type.

use chrono::{DateTime, Datelike, NaiveDate, Timelike};
use serde_json::Value;

pub(super) fn validate_date(value: &Value) -> Result<(), String> {
    let text = value
        .as_str()
        .ok_or_else(|| super::type_err("Date", value))?;
    if !date_shape(text.as_bytes()) || NaiveDate::parse_from_str(text, "%Y-%m-%d").is_err() {
        return Err("expected Date in YYYY-MM-DD form with a valid calendar date".into());
    }
    Ok(())
}

pub(super) fn validate_datetime(value: &Value) -> Result<(), String> {
    let text = value
        .as_str()
        .ok_or_else(|| super::type_err("DateTime", value))?;
    let invalid = || {
        "expected DateTime in RFC 3339 form with an offset, years 0000..9999, \
         at most nine fractional digits, and no leap second"
            .to_string()
    };
    if !datetime_shape(text.as_bytes()) {
        return Err(invalid());
    }
    let instant = DateTime::parse_from_rfc3339(text).map_err(|_| invalid())?;
    if instant.nanosecond() >= 1_000_000_000 || !(0..=9999).contains(&instant.to_utc().year()) {
        return Err(invalid());
    }
    Ok(())
}

fn date_shape(bytes: &[u8]) -> bool {
    bytes.len() == 10
        && bytes.iter().enumerate().all(|(i, byte)| match i {
            4 | 7 => *byte == b'-',
            _ => byte.is_ascii_digit(),
        })
}

fn datetime_shape(bytes: &[u8]) -> bool {
    if !(20..=35).contains(&bytes.len())
        || !date_shape(&bytes[..10])
        || !matches!(bytes[10], b'T' | b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
        || ![11, 12, 14, 15, 17, 18]
            .iter()
            .all(|i| bytes[*i].is_ascii_digit())
    {
        return false;
    }
    let mut offset = &bytes[19..];
    if offset.first() == Some(&b'.') {
        let digits = offset[1..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if !(1..=9).contains(&digits) {
            return false;
        }
        offset = &offset[1 + digits..];
    }
    matches!(offset, [b'Z' | b'z'])
        || (offset.len() == 6
            && matches!(offset[0], b'+' | b'-')
            && offset[3] == b':'
            && [1, 2, 4, 5].iter().all(|i| offset[*i].is_ascii_digit()))
}
