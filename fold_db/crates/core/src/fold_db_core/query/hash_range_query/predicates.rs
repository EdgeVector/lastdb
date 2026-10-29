//! Storage-free predicate matching and query-instant parsing.

use crate::schema::types::operations::FieldPredicate;
use chrono::{DateTime, Utc};
use serde_json::Value;

pub(super) fn field_predicate_matches(predicate: &FieldPredicate, value: Option<&Value>) -> bool {
    match predicate {
        FieldPredicate::Eq { value: target, .. } => value == Some(target),
        FieldPredicate::In { values, .. } => value.is_some_and(|v| values.iter().any(|t| t == v)),
        FieldPredicate::After { instant, .. } => {
            let Some(record_instant) = value.and_then(parse_query_instant) else {
                return false;
            };
            let Some(target_instant) = parse_query_instant(instant) else {
                return false;
            };
            record_instant >= target_instant
        }
        FieldPredicate::Before { instant, .. } => {
            let Some(record_instant) = value.and_then(parse_query_instant) else {
                return false;
            };
            let Some(target_instant) = parse_query_instant(instant) else {
                return false;
            };
            record_instant <= target_instant
        }
        FieldPredicate::Present { .. } => value.is_some_and(|v| !v.is_null()),
        FieldPredicate::Absent { .. } => value.is_none_or(Value::is_null),
    }
}

pub(super) fn parse_query_instant(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(s) = value.as_str() {
        return DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc));
    }
    if let Some(n) = value.as_i64() {
        return if (-10_000_000_000..=10_000_000_000).contains(&n) {
            DateTime::from_timestamp(n, 0)
        } else {
            DateTime::from_timestamp_millis(n)
        };
    }
    let n = value.as_f64()?;
    if !n.is_finite() {
        return None;
    }
    let seconds = if (-10_000_000_000.0..=10_000_000_000.0).contains(&n) {
        n
    } else {
        n / 1000.0
    };
    // Negative fractions belong to the preceding second with a positive
    // remainder. Normalize nanosecond rounding across the next whole second.
    let whole = seconds.floor();
    let mut secs = whole as i64;
    let mut nanos = ((seconds - whole) * 1_000_000_000.0).round() as u32;
    if nanos == 1_000_000_000 {
        secs = secs.checked_add(1)?;
        nanos = 0;
    }
    DateTime::from_timestamp(secs, nanos)
}
