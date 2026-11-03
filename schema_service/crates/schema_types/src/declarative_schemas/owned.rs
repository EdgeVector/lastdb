//! Owned app declarations: the one normalization of an app's schema.
//!
//! The LastDB node runs this when an app declares a schema, and the Schema
//! Service catalog feed runs it on each `lastdb-app.json` schema entry of a
//! listed app. One implementation gives one identity hash for one
//! declaration, so the catalog and the node never disagree about a schema.

use serde_json::Value;

use super::DeclarativeSchemaDefinition;

/// Where an owned declaration resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedDeclaration {
    /// The owning app id: the trimmed namespace.
    pub namespace: String,
    /// The local schema id, `<namespace>/<local name>`.
    pub local_schema_id: String,
}

impl DeclarativeSchemaDefinition {
    /// Normalize a declaration owned by `namespace`: name `<namespace>/<name>`,
    /// `owner_app_id`, a default `descriptive_name`, and the identity hash.
    ///
    /// `namespace_label` names the request field in error text (`app_id` or
    /// `namespace`). The checks run in a fixed order and the error text is
    /// stable, because the node returns it to the declaring app.
    ///
    /// # Errors
    /// A bad namespace or name, an invalid owned schema or definition, a
    /// missing identity hash, an empty or blank field list, or an untyped field.
    pub fn normalize_owned_declaration(
        &mut self,
        namespace_label: &str,
        namespace: &str,
    ) -> Result<OwnedDeclaration, String> {
        let namespace = namespace.trim().to_string();
        if namespace.is_empty() || namespace.contains('/') {
            return Err(format!(
                "{namespace_label} must be non-empty and must not contain '/'"
            ));
        }
        let raw_name = self.name.trim().to_string();
        if raw_name.is_empty() {
            return Err("schema.name must be non-empty".into());
        }
        let local_name = match raw_name.split_once('/') {
            Some((owner, local)) if owner == namespace && !local.is_empty() => local.to_string(),
            Some((_owner, _local)) => {
                return Err(format!(
                    "schema.name namespace must match request.{namespace_label}"
                ))
            }
            None => raw_name,
        };
        let local_schema_id = format!("{namespace}/{local_name}");
        self.owner_app_id = Some(namespace.clone());
        if self
            .descriptive_name
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
        {
            let short = local_schema_id.rsplit('/').next().unwrap_or_default();
            self.descriptive_name = Some(short.to_string());
        }
        self.name.clone_from(&local_schema_id);
        self.validate_owned_schema()?;
        self.compute_identity_hash();
        self.validate_runtime_definition()?;
        if self.get_identity_hash().is_none() {
            return Err("schema identity hash missing after compute_identity_hash".into());
        }
        self.require_typed_fields()?;
        Ok(OwnedDeclaration {
            namespace,
            local_schema_id,
        })
    }

    /// Drop declared field hashes and field keys, so an owned schema computes
    /// them from its own declaration.
    pub fn reset_field_hashes(&mut self) {
        self.field_hashes.clear();
        self.field_key_ids = None;
    }

    /// The normalization every wire load applies: `hash` and `superseded_by`
    /// are not carried, transform metadata is derived again, and an
    /// `owner_app_id` without an identity hash gets one.
    pub fn normalize_wire(&mut self) {
        self.hash = None;
        self.superseded_by = None;
        self.regenerate_metadata();
        if self.identity_hash.is_none() && self.owner_app_id.is_some() {
            self.compute_identity_hash();
        }
    }

    /// The definition check a node runs when it loads a schema. The text
    /// matches the node's `Invalid data` error for the same failure.
    fn validate_runtime_definition(&mut self) -> Result<(), String> {
        self.validate_definition()
            .map_err(|error| format!("Invalid data: {error}"))?;
        self.regenerate_metadata();
        Ok(())
    }

    /// Every field is named and names its type
    /// (`decision-2026-11-02-field-types-no-any`).
    fn require_typed_fields(&self) -> Result<(), String> {
        let fields = self.fields.as_deref().unwrap_or_default();
        if fields.is_empty() || fields.iter().any(|f| f.trim().is_empty()) {
            return Err("schema.fields must be non-empty without blank names".into());
        }
        let untyped: Vec<&str> = fields
            .iter()
            .filter(|field| !self.field_types.contains_key(*field))
            .map(String::as_str)
            .collect();
        if untyped.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "every field needs a type in schema.field_types; these fields have none: {}",
                untyped.join(", ")
            ))
        }
    }
}

/// One `lastdb-app.json` `schemas` entry, parsed as the node's declare route
/// parses it. `retention` is node-local policy that never reaches the schema,
/// so it is dropped first, as `lastdb app check` does.
///
/// # Errors
/// The entry is not a schema declaration.
pub fn manifest_schema_definition(entry: &Value) -> Result<DeclarativeSchemaDefinition, String> {
    let mut entry = entry.clone();
    if let Some(object) = entry.as_object_mut() {
        object.remove("retention");
    }
    let mut schema = schema_from_value(entry).map_err(|error| error.to_string())?;
    schema.normalize_wire();
    Ok(schema)
}

/// Parse one schema definition from a JSON value.
///
/// Every caller in the Schema Service Lambda parses a definition from a
/// `Value` through this one function. A generic `from_value` call in each
/// crate compiles its own copy of the definition deserializer (about 48 KB),
/// and the Lambda zip has a 15 MiB bar, so this stays out of line.
///
/// # Errors
/// The value is not a schema definition.
#[inline(never)]
pub fn schema_from_value(value: Value) -> Result<DeclarativeSchemaDefinition, serde_json::Error> {
    serde_json::from_value(value)
}

/// The definition a fresh install of `app_id`'s declaration loads: the owned
/// normalization, then field hashes computed from the declaration alone.
///
/// # Errors
/// Any error of [`DeclarativeSchemaDefinition::normalize_owned_declaration`],
/// or an owned or definition check that fails after the field hashes.
pub fn owned_catalog_definition(
    app_id: &str,
    mut schema: DeclarativeSchemaDefinition,
) -> Result<DeclarativeSchemaDefinition, String> {
    schema.normalize_owned_declaration("app_id", app_id)?;
    schema.reset_field_hashes();
    schema.ensure_field_hashes();
    schema.validate_runtime_definition()?;
    schema.validate_owned_schema()?;
    Ok(schema)
}
