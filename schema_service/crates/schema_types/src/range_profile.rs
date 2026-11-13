//! Range storage profiles, typed range comparisons, field hashes, and portable IDs.

use crate::{DeclarativeSchemaDefinition, FieldValueType};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[must_use]
pub fn compute_field_hash(
    name: &str,
    description: &str,
    field_type: &FieldValueType,
    version: u32,
) -> String {
    let type_json = serde_json::to_string(field_type).unwrap_or_else(|_| "\"Any\"".into());
    let quote = |s: &str| serde_json::to_string(s.trim()).unwrap_or_else(|_| "\"\"".into());
    let payload = format!(
        r#"{{"description":{},"name":{},"type":{type_json},"version":{version}}}"#,
        quote(description),
        quote(name),
    );
    app_identity_crypto::hex_lower(&Sha256::digest(payload.as_bytes()))
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct PortableSchemaId(String);

impl PortableSchemaId {
    pub fn from_catalog(
        catalog_id: &str,
        mut def: DeclarativeSchemaDefinition,
    ) -> Result<Self, String> {
        if !is_digest(catalog_id) {
            return Err("schema catalog ID must be a lowercase SHA-256".into());
        }
        if def.identity_hash.as_deref() != Some(catalog_id) {
            return Err("definition lacks the exact supported catalog identity".into());
        }
        if def.record_keys.is_none() {
            return Err("portable schemas require explicit record keys".into());
        }
        def.validate_definition()?;
        def.require_typed_fields()?;
        let ver = def.identity_hash_algo_version;
        def.compute_identity_hash();
        if def.identity_hash.as_deref() != Some(catalog_id) || def.identity_hash_algo_version != ver
        {
            return Err("schema catalog identity does not match its declaration".into());
        }
        def.normalize_wire();
        def.ensure_field_hashes();
        let val =
            serde_json::to_value(&def).map_err(|_| "cannot encode portable schema definition")?;
        let bytes = app_identity_crypto::canonicalize(&val)
            .map_err(|_| "cannot canonicalize portable schema definition")?;
        let mut digest = Sha256::new();
        digest.update(b"lastdb:portable-schema-definition:v1\0");
        digest.update(bytes);
        Ok(Self(app_identity_crypto::hex_lower(&digest.finalize())))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PortableSchemaId {
    type Error = String;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        if is_digest(&v) {
            Ok(Self(v))
        } else {
            Err("portable schema ID must be a lowercase SHA-256".into())
        }
    }
}

fn is_digest(v: &str) -> bool {
    v.len() == 64 && v.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

pub mod field_range_profile {
    use crate::{date_day, datetime_nanos, key_comparison::ordered_type, FieldValueType};
    use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
    use serde::{Deserialize, Serialize};

    const FIRST_DAY: i32 = -365;
    const LAST_DAY: i32 = 3_652_059;
    const FIRST_NANOS: i128 = -62_167_219_200_000_000_000;
    const END_NANOS: i128 = 253_402_300_800_000_000_000;
    const NANOS_PER_SEC: i128 = 1_000_000_000;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum RangeStorageProfile {
        StringUtf8V1,
        IntegerI64V1,
        DateDaysV1,
        DateTimeNanosV1,
        BooleanV1,
    }

    impl RangeStorageProfile {
        pub fn typed(field_type: &FieldValueType) -> Result<Self, String> {
            match ordered_type(field_type)? {
                FieldValueType::String => Ok(Self::StringUtf8V1),
                FieldValueType::Integer => Ok(Self::IntegerI64V1),
                FieldValueType::Date => Ok(Self::DateDaysV1),
                FieldValueType::DateTime => Ok(Self::DateTimeNanosV1),
                FieldValueType::Boolean => Ok(Self::BooleanV1),
                _ => Err("this field type has no typed storage profile".into()),
            }
        }

        pub fn encode_value(self, api: &str) -> Result<String, String> {
            match self {
                Self::StringUtf8V1 => Ok(api.to_owned()),
                Self::IntegerI64V1 => api
                    .parse::<i64>()
                    .map(|v| format!("{:016x}", (v as u64) ^ (1_u64 << 63)))
                    .map_err(|_| "expected a signed 64-bit range key".into()),
                Self::DateDaysV1 => Ok(format!("{:06x}", date_day(api)? - FIRST_DAY)),
                Self::DateTimeNanosV1 => Ok(format!("{:018x}", datetime_nanos(api)? - FIRST_NANOS)),
                Self::BooleanV1 => match api {
                    "false" => Ok("0".into()),
                    "true" => Ok("1".into()),
                    _ => Err("expected a Boolean range key".into()),
                },
            }
        }

        pub fn decode_value(self, stored: &str) -> Result<String, String> {
            match self {
                Self::StringUtf8V1 => Ok(stored.to_owned()),
                Self::IntegerI64V1 => {
                    Ok((((parse_hex(stored, 16)? as u64) ^ (1_u64 << 63)) as i64).to_string())
                }
                Self::DateDaysV1 => {
                    let b = parse_hex(stored, 6)?;
                    if b > (LAST_DAY - FIRST_DAY) as u128 {
                        return Err("stored Date range is outside years 0000..9999".into());
                    }
                    NaiveDate::from_num_days_from_ce_opt(FIRST_DAY + b as i32)
                        .map(|d| d.format("%Y-%m-%d").to_string())
                        .ok_or_else(|| "stored Date range is invalid".into())
                }
                Self::DateTimeNanosV1 => {
                    let b = parse_hex(stored, 18)?;
                    if b >= (END_NANOS - FIRST_NANOS) as u128 {
                        return Err("stored DateTime range is outside UTC years 0000..9999".into());
                    }
                    let n = FIRST_NANOS + b as i128;
                    DateTime::<Utc>::from_timestamp(
                        n.div_euclid(NANOS_PER_SEC) as i64,
                        n.rem_euclid(NANOS_PER_SEC) as u32,
                    )
                    .map(|i| i.to_rfc3339_opts(SecondsFormat::Nanos, true))
                    .ok_or_else(|| "stored DateTime range is invalid".into())
                }
                Self::BooleanV1 => match stored {
                    "0" => Ok("false".into()),
                    "1" => Ok("true".into()),
                    _ => Err("stored Boolean range is invalid".into()),
                },
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
    pub enum FieldRangeProfile {
        EmptyV1,
        TypedV1 { scalar: RangeStorageProfile },
    }

    impl FieldRangeProfile {
        pub fn for_field_type(range: Option<&FieldValueType>) -> Result<Self, String> {
            range.map_or(Ok(Self::EmptyV1), |ty| {
                RangeStorageProfile::typed(ty).map(|scalar| Self::TypedV1 { scalar })
            })
        }

        pub fn encode_value(self, api: &str) -> Result<String, String> {
            match self {
                Self::EmptyV1 => empty_slot(api),
                Self::TypedV1 { scalar } => scalar.encode_value(api),
            }
        }

        pub fn decode_value(self, stored: &str) -> Result<String, String> {
            match self {
                Self::EmptyV1 => empty_slot(stored),
                Self::TypedV1 { scalar } => scalar.decode_value(stored),
            }
        }

        pub fn require_text_prefix(self) -> Result<(), String> {
            match self {
                Self::EmptyV1 => Err("a field key without a range field has no text prefix".into()),
                Self::TypedV1 {
                    scalar: RangeStorageProfile::StringUtf8V1,
                } => Ok(()),
                Self::TypedV1 { .. } => {
                    Err("a text prefix requires a String range storage profile".into())
                }
            }
        }
    }

    fn empty_slot(v: &str) -> Result<String, String> {
        if v.is_empty() {
            Ok(String::new())
        } else {
            Err("a field key without a range field requires an empty range slot".into())
        }
    }

    fn parse_hex(text: &str, width: usize) -> Result<u128, String> {
        if text.len() != width || !text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err("stored typed range has a noncanonical width or alphabet".into());
        }
        u128::from_str_radix(text, 16).map_err(|_| "stored typed range is invalid".into())
    }
}

pub mod key_comparison {
    use crate::{date_day, datetime_nanos, FieldValueType};
    use serde::Serialize;
    use std::cmp::Ordering;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct RangeKeyComparison {
        pub field_type: FieldValueType,
    }

    impl RangeKeyComparison {
        pub fn typed(field_type: FieldValueType) -> Result<Self, String> {
            ordered_type(&field_type)?;
            Ok(Self { field_type })
        }

        pub fn compare(&self, left: &str, right: &str) -> Result<Ordering, String> {
            match ordered_type(&self.field_type)? {
                FieldValueType::String => Ok(left.cmp(right)),
                FieldValueType::Integer => Ok(parse_i64(left)?.cmp(&parse_i64(right)?)),
                FieldValueType::Date => Ok(date_day(left)?.cmp(&date_day(right)?)),
                FieldValueType::DateTime => Ok(datetime_nanos(left)?.cmp(&datetime_nanos(right)?)),
                FieldValueType::Boolean => Ok(parse_bool(left)?.cmp(&parse_bool(right)?)),
                _ => Err("this field type has no approved typed range comparison".into()),
            }
        }

        pub fn validate_value(&self, text: &str) -> Result<(), String> {
            self.compare(text, text).map(|_| ())
        }
    }

    fn parse_i64(t: &str) -> Result<i64, String> {
        t.parse()
            .map_err(|_| "expected a signed 64-bit range key".into())
    }

    fn parse_bool(t: &str) -> Result<bool, String> {
        t.parse().map_err(|_| "expected a Boolean range key".into())
    }

    pub(crate) fn ordered_type(field_type: &FieldValueType) -> Result<&FieldValueType, String> {
        if let FieldValueType::OneOf(variants) = field_type {
            let mut non_null = variants.iter().filter(|v| **v != FieldValueType::Null);
            let first = ordered_type(non_null.next().ok_or("a required key cannot be null")?)?;
            for v in non_null {
                if ordered_type(v)? != first {
                    return Err("a typed range requires one unambiguous scalar domain".into());
                }
            }
            return Ok(first);
        }
        if matches!(
            field_type,
            FieldValueType::String
                | FieldValueType::Integer
                | FieldValueType::Date
                | FieldValueType::DateTime
                | FieldValueType::Boolean
        ) {
            Ok(field_type)
        } else {
            Err("this field type has no approved typed range comparison".into())
        }
    }
}
