//! Keyed list of saved-query ids in one source database.
//!
//! `SavedQuery` is a hash schema. Listing its keys would read every record
//! of that schema. The catalog is one reserved key, so a wake reads that
//! key and then each named query. The key is not a legal query id.

use serde::Deserialize;
use serde_json::{json, Value};

use super::{label_ok, PublishError, MAX_STORED_TEXT};

/// Reserved hash key. `label_ok` rejects the dot, so `save` cannot use it.
pub const CATALOG_KEY: &str = ".catalog";

/// One source catalog stays inside the saved-query text cap.
pub const MAX_CATALOG_IDS: usize = 256;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogBody {
    ids: Vec<String>,
}

/// Ids registered by `save` or a manual reconcile, plus the record revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryCatalog {
    pub ids: Vec<String>,
    pub revision: Option<String>,
}

impl QueryCatalog {
    pub fn empty() -> Self {
        Self {
            ids: Vec::new(),
            revision: None,
        }
    }

    /// `true` when `id` was not already present.
    pub fn insert(&mut self, id: &str) -> Result<bool, PublishError> {
        if id == CATALOG_KEY || !label_ok(id) {
            return Err(PublishError::new("saved query name is invalid"));
        }
        if self.ids.iter().any(|existing| existing == id) {
            return Ok(false);
        }
        if self.ids.len() >= MAX_CATALOG_IDS {
            return Err(PublishError::new("saved query catalog is too large"));
        }
        self.ids.push(id.to_owned());
        self.ids.sort();
        Ok(true)
    }

    pub fn from_row(row: &Value) -> Result<Self, PublishError> {
        let fields = row
            .get("fields")
            .ok_or(PublishError::new("saved query catalog is invalid"))?;
        let definition = fields
            .get("definition")
            .and_then(Value::as_str)
            .ok_or(PublishError::new("saved query catalog is invalid"))?;
        let body: CatalogBody = serde_json::from_str(definition)
            .map_err(|_| PublishError::new("saved query catalog is invalid"))?;
        let mut ids = Vec::new();
        for id in body.ids {
            if id == CATALOG_KEY || !label_ok(&id) || ids.iter().any(|existing| existing == &id) {
                return Err(PublishError::new("saved query catalog is invalid"));
            }
            ids.push(id);
        }
        if ids.len() > MAX_CATALOG_IDS {
            return Err(PublishError::new("saved query catalog is too large"));
        }
        ids.sort();
        let revision = row
            .get("revision")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        Ok(Self { ids, revision })
    }

    pub fn fields(&self) -> Result<Value, PublishError> {
        let definition = serde_json::to_string(&json!({ "ids": self.ids }))
            .map_err(|_| PublishError::new("saved query catalog is invalid"))?;
        if definition.len() > MAX_STORED_TEXT {
            return Err(PublishError::new("saved query catalog is too large"));
        }
        Ok(json!({
            "app_id": "lastdb-publish",
            "destination": "catalog",
            "spec_name": "catalog",
            "spec_version": "1",
            "definition": definition,
            "ledger": "[]",
        }))
    }
}
