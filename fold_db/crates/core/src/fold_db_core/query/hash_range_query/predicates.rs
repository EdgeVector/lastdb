//! Pure helpers: field-predicate matching, instant parsing and JSON value ordering.

use super::*;

pub(super) fn field_predicate_matches(
    predicate: &FieldPredicate,
    fields: &HashMap<String, Value>,
) -> bool {
    let value = fields.get(predicate.field_name());
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
        let seconds = if n.abs() > 10_000_000_000 {
            n.checked_div(1000)?
        } else {
            n
        };
        return DateTime::from_timestamp(seconds, 0);
    }
    let n = value.as_f64()?;
    if !n.is_finite() {
        return None;
    }
    let seconds = if n.abs() > 10_000_000_000.0 {
        n / 1000.0
    } else {
        n
    };
    let secs = seconds.trunc() as i64;
    let nanos = ((seconds.fract().abs()) * 1_000_000_000.0).round() as u32;
    DateTime::from_timestamp(secs, nanos)
}

pub(super) fn compare_json_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(Value::Number(a)), Some(Value::Number(b))) => a
            .as_f64()
            .partial_cmp(&b.as_f64())
            .unwrap_or(Ordering::Equal),
        (Some(Value::String(a)), Some(Value::String(b))) => a.cmp(b),
        (Some(Value::Bool(a)), Some(Value::Bool(b))) => a.cmp(b),
        (Some(a), Some(b)) => value_rank(a)
            .cmp(&value_rank(b))
            .then_with(|| a.to_string().cmp(&b.to_string())),
    }
}

pub(super) fn value_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}
