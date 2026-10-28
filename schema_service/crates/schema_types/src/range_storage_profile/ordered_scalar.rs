use super::RangeStorageProfile;
use crate::field_value_type::temporal;
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};

// Inclusive Gregorian day ordinals for 0000-01-01 and 9999-12-31.
const FIRST_DAY: i32 = -365;
const LAST_DAY: i32 = 3_652_059;
// Unix nanoseconds at 0000-01-01T00:00:00Z and 10000-01-01T00:00:00Z.
const FIRST_NANOS: i128 = -62_167_219_200_000_000_000;
const END_NANOS: i128 = 253_402_300_800_000_000_000;
const NANOS_PER_SECOND: i128 = 1_000_000_000;

pub(super) fn encode(profile: RangeStorageProfile, api: &str) -> Result<String, String> {
    use RangeStorageProfile as P;
    match profile {
        P::StringUtf8V1 => Ok(api.to_owned()),
        P::IntegerI64V1 => {
            let value = api
                .parse::<i64>()
                .map_err(|_| "expected a signed 64-bit range key")?;
            let biased = (value as u64) ^ (1_u64 << 63);
            Ok(format!("{biased:016x}"))
        }
        P::DateDaysV1 => {
            let biased = temporal::date_day(api)? - FIRST_DAY;
            Ok(format!("{biased:06x}"))
        }
        P::DateTimeNanosV1 => {
            let biased = temporal::datetime_nanos(api)? - FIRST_NANOS;
            Ok(format!("{biased:018x}"))
        }
        P::BooleanV1 => match api {
            "false" => Ok("0".into()),
            "true" => Ok("1".into()),
            _ => Err("expected a Boolean range key".into()),
        },
    }
}

pub(super) fn decode(profile: RangeStorageProfile, stored: &str) -> Result<String, String> {
    use RangeStorageProfile as P;
    match profile {
        P::StringUtf8V1 => Ok(stored.to_owned()),
        P::IntegerI64V1 => {
            let value = hex(stored, 16)? as u64;
            Ok(((value ^ (1_u64 << 63)) as i64).to_string())
        }
        P::DateDaysV1 => {
            let biased = hex(stored, 6)?;
            if biased > (LAST_DAY - FIRST_DAY) as u128 {
                return Err("stored Date range is outside years 0000..9999".into());
            }
            NaiveDate::from_num_days_from_ce_opt(FIRST_DAY + biased as i32)
                .map(|date| date.format("%Y-%m-%d").to_string())
                .ok_or_else(|| "stored Date range is invalid".into())
        }
        P::DateTimeNanosV1 => decode_datetime(stored),
        P::BooleanV1 => match stored {
            "0" => Ok("false".into()),
            "1" => Ok("true".into()),
            _ => Err("stored Boolean range is invalid".into()),
        },
    }
}

fn decode_datetime(stored: &str) -> Result<String, String> {
    let biased = hex(stored, 18)?;
    if biased >= (END_NANOS - FIRST_NANOS) as u128 {
        return Err("stored DateTime range is outside UTC years 0000..9999".into());
    }
    let nanos = FIRST_NANOS + biased as i128;
    let seconds = nanos.div_euclid(NANOS_PER_SECOND) as i64;
    let fraction = nanos.rem_euclid(NANOS_PER_SECOND) as u32;
    DateTime::<Utc>::from_timestamp(seconds, fraction)
        .map(|instant| instant.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .ok_or_else(|| "stored DateTime range is invalid".into())
}

fn hex(text: &str, width: usize) -> Result<u128, String> {
    if text.len() != width
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("stored typed range has a noncanonical width or alphabet".into());
    }
    u128::from_str_radix(text, 16).map_err(|_| "stored typed range is invalid".into())
}
