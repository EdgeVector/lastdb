//! Contributor write rules and the request a granted writer submits.
use cloud_backup_protocol::{Digest, Id};
use fold_db::schema::types::{KeyValue, MutationType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRule {
    pub schema: String,
    pub definition_id: Digest,
    pub action: MutationType,
}

impl WriteRule {
    pub(super) fn valid(&self) -> bool {
        matches!(
            self.action,
            MutationType::Create | MutationType::Replace | MutationType::Delete
        ) && self.schema.len() <= 256
            && self.schema.trim() == self.schema
            && !self.schema.chars().any(char::is_control)
            && self
                .schema
                .split_once('/')
                .is_some_and(|(app, name)| !app.is_empty() && !name.is_empty())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    pub mutation_id: Id,
    pub schema: String,
    pub definition_id: Digest,
    pub action: MutationType,
    pub key: KeyValue,
    pub record: BTreeMap<String, Value>,
    /// Explicit alternate addresses. Omitted optional addresses stay absent.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    /// Expected revision for replace and delete. Omitted on a create so an
    /// older publisher still decodes it. Ship the publisher before writers
    /// send this field: the payload denies unknown fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}
