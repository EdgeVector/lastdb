//! Compound ranges use declared fields. Their text representation is explicit.

use crate::{AlternateKey, DeclarativeSchemaDefinition, FieldValueType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RangePart {
    pub field: String,
    #[serde(default)]
    pub format: PartFormat,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PartFormat {
    #[default]
    Text,
    PaddedText {
        width: u8,
    },
    ReverseTimeMillis,
}

pub(crate) fn validate(
    key: &AlternateKey,
    schema: &DeclarativeSchemaDefinition,
) -> Result<(), String> {
    if key.hash_field.is_empty() == key.hash_literal.is_none() {
        return Err("an alternate key needs exactly one hash field or literal".into());
    }
    if key.omit_empty_hash
        && (key.missing != crate::MissingAlternateKey::Omit
            || key.hash_literal.is_some()
            || schema.key_field_type(&key.hash_field) != FieldValueType::String)
    {
        return Err("omit_empty_hash requires an optional text hash field".into());
    }
    if key
        .hash_literal
        .as_ref()
        .is_some_and(|value| value.is_empty() || value.len() > 1024)
    {
        return Err("an alternate hash literal must contain 1 to 1024 bytes".into());
    }
    if key.range_parts.is_empty() {
        if key.range_separator.is_some() {
            return Err("a range separator requires compound range parts".into());
        }
        return Ok(());
    }
    if key.range_field.is_some() || !(2..=8).contains(&key.range_parts.len()) {
        return Err("a compound range needs 2 to 8 parts and no range field".into());
    }
    let separator = key
        .range_separator
        .as_deref()
        .ok_or("a compound range needs a separator")?;
    if separator.len() != 1 || !separator.as_bytes()[0].is_ascii_punctuation() {
        return Err("a compound separator must be one ASCII punctuation byte".into());
    }
    for part in &key.range_parts {
        let kind = schema.key_field_type(&part.field);
        match part.format {
            PartFormat::Text if kind == FieldValueType::String => {}
            PartFormat::PaddedText { width }
                if (1..=64).contains(&width) && kind == FieldValueType::String => {}
            PartFormat::ReverseTimeMillis
                if matches!(kind, FieldValueType::String | FieldValueType::DateTime) => {}
            _ => return Err("compound range format does not match its field type".into()),
        }
    }
    Ok(())
}

pub fn render(
    part: &RangePart,
    value: &serde_json::Value,
    separator: &str,
) -> Result<String, String> {
    let text = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|n| n.to_string()))
        .ok_or("compound range parts require text or integer values")?;
    if text.contains(separator) {
        return Err("a compound range value contains its reserved separator".into());
    }
    match part.format {
        PartFormat::Text => Ok(text),
        PartFormat::PaddedText { width } => Ok(format!(
            "{}{}",
            "0".repeat(usize::from(width).saturating_sub(text.encode_utf16().count())),
            text
        )),
        PartFormat::ReverseTimeMillis => {
            let millis = crate::rfc3339::parse_utc(&text)?.timestamp_millis();
            let ordered = (millis as u64) ^ (1 << 63);
            Ok(format!("{:020}", u64::MAX - ordered))
        }
    }
}
