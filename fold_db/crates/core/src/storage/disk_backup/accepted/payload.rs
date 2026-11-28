//! One payload definition for encoding captured changes and decoding them on another node.
//! Capture borrows the store mutation. Deserialization owns the decoded values.

use crate::schema::types::KeyValue;
use lastdb_model::record_value::FileMarker;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;

/// The plaintext of a captured store mutation, before sealing.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedBody<'a> {
    pub version: u16,
    pub mutation_id: Cow<'a, str>,
    pub kind: Cow<'a, str>,
    pub schema: Cow<'a, str>,
    pub action: Cow<'a, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Cow<'a, KeyValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Cow<'a, BTreeMap<String, Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternates: Option<Cow<'a, BTreeMap<String, Option<KeyValue>>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<FileSection<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline_file: Option<InlineSection<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition_id: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contribution: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSection<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sealed_sha256: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<Cow<'a, str>>,
    pub cleared: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InlineSection<'a> {
    pub cleared: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub marker: Option<Cow<'a, FileMarker>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Cow<'a, str>>,
}
