//! Total order over JSON values, shared by keyed reads in `fold_db` and the
//! host's sorted result rows so both sort identically.

use serde_json::Value;
use std::cmp::Ordering;

/// Missing values sort last; numbers, strings and bools compare by value;
/// mixed kinds compare by kind, then by their JSON text.
pub fn compare_json_values(a: Option<&Value>, b: Option<&Value>) -> Ordering {
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

fn value_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}
