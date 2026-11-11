//! The saved query stored in the source database.
//!
//! The schema is `lastdb-publish/SavedQuery`. That namespace is not in a
//! share spec, so the query record itself stays in the source database.

use serde_json::Value;

use super::definition::Definition;
use super::plan::LedgerEntry;
use super::policy::DeclareSchema;
use super::{label_ok, PublishError, MAX_STORED_TEXT};

pub const SAVED_QUERY_SCHEMA: &str = "lastdb-publish/SavedQuery";

const NAMESPACE: &str = "lastdb-publish";
const NAME: &str = "SavedQuery";

/// One saved query and the ledger of keys its last reconcile published.
#[derive(Clone, Debug, PartialEq)]
pub struct SavedQuery {
    pub id: String,
    pub app_id: String,
    pub destination: String,
    pub spec_name: String,
    pub spec_version: u32,
    pub definition_json: String,
    pub definition: Definition,
    pub ledger: Vec<LedgerEntry>,
    pub revision: Option<String>,
}

impl SavedQuery {
    pub fn create(
        id: &str,
        app_id: &str,
        destination: &str,
        spec_name: &str,
        spec_version: u32,
        definition_json: &str,
    ) -> Result<Self, PublishError> {
        if !label_ok(id) || !label_ok(app_id) || !label_ok(spec_name) || !label_ok(destination) {
            return Err(PublishError::new("saved query name is invalid"));
        }
        if spec_version < 1 {
            return Err(PublishError::new("unsupported database spec version"));
        }
        let definition = Definition::parse(definition_json.as_bytes())?;
        Ok(Self {
            id: id.to_owned(),
            app_id: app_id.to_owned(),
            destination: destination.to_owned(),
            spec_name: spec_name.to_owned(),
            spec_version,
            definition_json: definition_json.to_owned(),
            definition,
            ledger: Vec::new(),
            revision: None,
        })
    }

    /// Load a query row. An empty ledger is a query that has not reconciled.
    pub fn from_row(id: &str, row: &Value) -> Result<Self, PublishError> {
        let fields = row
            .get("fields")
            .ok_or(PublishError::new("saved query record is invalid"))?;
        let definition_json = required_text(fields, "definition")?;
        let definition = Definition::parse(definition_json.as_bytes())?;
        let ledger_text = required_text(fields, "ledger")?;
        let ledger = if ledger_text.is_empty() || ledger_text == "[]" {
            Vec::new()
        } else {
            serde_json::from_str(&ledger_text)
                .map_err(|_| PublishError::new("saved query ledger is invalid"))?
        };
        let spec_version = required_text(fields, "spec_version")?
            .parse()
            .map_err(|_| PublishError::new("saved query record is invalid"))?;
        let revision = row
            .get("revision")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        Ok(Self {
            id: id.to_owned(),
            app_id: required_text(fields, "app_id")?,
            destination: required_text(fields, "destination")?,
            spec_name: required_text(fields, "spec_name")?,
            spec_version,
            definition_json,
            definition,
            ledger,
            revision,
        })
    }

    /// Field object for a create or replace of this record. The ledger is
    /// written only after the share writes succeed.
    pub fn fields(&self) -> Result<Value, PublishError> {
        let ledger = serde_json::to_string(&self.ledger)
            .map_err(|_| PublishError::new("saved query ledger is invalid"))?;
        if self.definition_json.len() > MAX_STORED_TEXT || ledger.len() > MAX_STORED_TEXT {
            return Err(PublishError::new("saved query record is too large"));
        }
        Ok(serde_json::json!({
            "app_id": self.app_id,
            "destination": self.destination,
            "spec_name": self.spec_name,
            "spec_version": self.spec_version.to_string(),
            "definition": self.definition_json,
            "ledger": ledger,
        }))
    }

    pub fn same_target(&self, destination: &str, spec_name: &str, spec_version: u32) -> bool {
        self.destination == destination
            && self.spec_name == spec_name
            && self.spec_version == spec_version
    }
}

pub fn saved_query_declare() -> DeclareSchema {
    let fields = [
        "app_id",
        "destination",
        "spec_name",
        "spec_version",
        "definition",
        "ledger",
    ];
    let mut field_types = std::collections::BTreeMap::new();
    let mut field_descriptions = std::collections::BTreeMap::new();
    for field in fields {
        field_types.insert(field.to_owned(), "String".to_owned());
        field_descriptions.insert(field.to_owned(), format!("Saved query {field}."));
    }
    DeclareSchema {
        namespace: NAMESPACE.to_owned(),
        name: NAME.to_owned(),
        schema_type: "Hash".to_owned(),
        record_keys: serde_json::json!({"primary": {"kind": "hash"}, "alternates": {}}),
        fields: fields.map(str::to_owned).to_vec(),
        field_types,
        field_descriptions,
        files: false,
    }
}

fn required_text(fields: &Value, name: &str) -> Result<String, PublishError> {
    fields
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(PublishError::new("saved query record is invalid"))
}
