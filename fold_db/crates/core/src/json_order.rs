//! Total order over JSON values, shared by keyed reads in `fold_db` and the
//! host's sorted result rows so both sort identically.

use serde_json::{Number, Value};
use std::cmp::Ordering;

/// Missing values sort last; numbers, strings and bools compare by value;
/// mixed kinds compare by kind, then by their JSON text.
pub fn compare_json_values(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(Value::Number(a)), Some(Value::Number(b))) => compare_numbers(a, b),
        (Some(Value::String(a)), Some(Value::String(b))) => a.cmp(b),
        (Some(Value::Bool(a)), Some(Value::Bool(b))) => a.cmp(b),
        (Some(a), Some(b)) => value_rank(a)
            .cmp(&value_rank(b))
            .then_with(|| a.to_string().cmp(&b.to_string())),
    }
}

#[derive(Clone, Copy)]
enum NumericValue {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

impl NumericValue {
    fn from_number(number: &Number) -> Self {
        if let Some(value) = number.as_i64() {
            Self::Signed(value)
        } else if let Some(value) = number.as_u64() {
            Self::Unsigned(value)
        } else {
            // serde_json's standard Number representation contains either a
            // signed integer, an unsigned integer, or a finite binary float.
            Self::Float(number.as_f64().expect("finite JSON numeric value"))
        }
    }
}

fn compare_numbers(a: &Number, b: &Number) -> Ordering {
    use NumericValue::{Float, Signed, Unsigned};

    match (NumericValue::from_number(a), NumericValue::from_number(b)) {
        (Signed(a), Signed(b)) => a.cmp(&b),
        (Unsigned(a), Unsigned(b)) => a.cmp(&b),
        (Signed(a), Unsigned(b)) => compare_signed_unsigned(a, b),
        (Unsigned(a), Signed(b)) => compare_signed_unsigned(b, a).reverse(),
        (Signed(a), Float(b)) => compare_signed_float(a, b),
        (Float(a), Signed(b)) => compare_signed_float(b, a).reverse(),
        (Unsigned(a), Float(b)) => compare_unsigned_float(a, b),
        (Float(a), Unsigned(b)) => compare_unsigned_float(b, a).reverse(),
        (Float(a), Float(b)) => a.partial_cmp(&b).expect("finite JSON numeric values"),
    }
}

/// Compare a stored JSON number with an API float threshold. NaN remains
/// unordered; infinite library thresholds retain their mathematical order.
pub(crate) fn compare_number_to_f64(number: &Number, float: f64) -> Option<Ordering> {
    if float.is_nan() {
        return None;
    }
    Some(match NumericValue::from_number(number) {
        NumericValue::Signed(integer) => compare_signed_float(integer, float),
        NumericValue::Unsigned(integer) => compare_unsigned_float(integer, float),
        NumericValue::Float(value) => value.partial_cmp(&float)?,
    })
}

fn compare_signed_unsigned(integer: i64, unsigned: u64) -> Ordering {
    if integer < 0 {
        Ordering::Less
    } else {
        (integer as u64).cmp(&unsigned)
    }
}

fn compare_signed_float(integer: i64, float: f64) -> Ordering {
    if integer < 0 {
        if float < 0.0 {
            compare_unsigned_float(integer.unsigned_abs(), -float).reverse()
        } else {
            Ordering::Less
        }
    } else {
        compare_unsigned_float(integer as u64, float)
    }
}

/// Compare the float's exact represented value without rounding the integer
/// to f64. Guard 2^64 before conversion; u64::MAX rounds up to that boundary.
fn compare_unsigned_float(integer: u64, float: f64) -> Ordering {
    if float < 0.0 {
        return Ordering::Greater;
    }
    if float >= 18_446_744_073_709_551_616.0 {
        return Ordering::Less;
    }
    // The guarded finite nonnegative float truncates to an exact u64. A
    // fractional remainder places it strictly above that integer. Floats at
    // 2^52 and above have no fractional bits, including every large boundary.
    let ordering = integer.cmp(&(float as u64));
    if ordering == Ordering::Equal && float.fract() > 0.0 {
        Ordering::Less
    } else {
        ordering
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
