//! Source authority and the destination schema projection.

use std::collections::BTreeMap;

use database_policy::{
    admit_namespace, field_is_shareable, file_is_shareable, record_is_shareable,
    schema_is_shareable, schemas, NamespaceClass, SpecBinding,
};
use serde_json::{Map, Value};

use super::definition::{Definition, RangeSelect};
use super::PublishError;

/// Schema installed in the source database, as `GET /api/schema` returns it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawSchema {
    pub namespace: String,
    pub name: String,
    pub owner_app_id: String,
    pub schema_type: String,
    pub record_keys: Value,
    pub fields: Vec<String>,
    pub field_types: BTreeMap<String, String>,
    pub field_descriptions: BTreeMap<String, String>,
}

/// Declaration body for the share database. Fields are the shareable subset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclareSchema {
    pub namespace: String,
    pub name: String,
    pub schema_type: String,
    pub record_keys: Value,
    pub fields: Vec<String>,
    pub field_types: BTreeMap<String, String>,
    pub field_descriptions: BTreeMap<String, String>,
    /// `file_is_shareable` for this schema. Other file bytes stay in personal.
    pub files: bool,
}

impl RawSchema {
    /// Read one installed schema. `namespace` and `name` are the names the
    /// query used, which are the authority names even when the stored `name`
    /// is only the local half.
    pub fn from_installed(
        namespace: &str,
        name: &str,
        value: &Value,
    ) -> Result<Self, PublishError> {
        let owner_app_id = value
            .get("owner_app_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or(PublishError::new("source schema has no owner"))?
            .to_owned();
        let schema_type = value
            .get("schema_type")
            .and_then(Value::as_str)
            .ok_or(PublishError::new("source schema has no key type"))?
            .to_owned();
        let record_keys = value
            .get("record_keys")
            .cloned()
            .ok_or(PublishError::new("source schema has no record key"))?;
        let fields = string_list(value.get("fields"))?;
        let field_types = string_map(value.get("field_types"))?;
        let field_descriptions = string_map(value.get("field_descriptions"))?;
        Ok(Self {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            owner_app_id,
            schema_type,
            record_keys,
            fields,
            field_types,
            field_descriptions,
        })
    }
}

impl DeclareSchema {
    pub fn qualified(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    /// Body for `POST /api/schemas/declare`.
    pub fn body(&self) -> Value {
        serde_json::json!({
            "namespace": self.namespace,
            "schema": {
                "name": self.name,
                "descriptive_name": self.name,
                "schema_type": self.schema_type,
                "record_keys": self.record_keys,
                "fields": self.fields,
                "field_types": self.field_types,
                "field_descriptions": self.field_descriptions,
            },
        })
    }
}

/// Build the binding A stored. [`SpecBinding::admitted`] checks the catalog.
/// A missing spec never reaches this function.
pub fn destination_binding(name: &str, version: u32) -> Result<SpecBinding, PublishError> {
    SpecBinding::admitted(name, version).map_err(PublishError::from)
}

/// Check app ownership and publish the shareable field list for every schema
/// the query names. The result is the destination declaration, in query order.
pub fn authorize(
    app_id: &str,
    binding: &SpecBinding,
    installed: &[RawSchema],
    definition: &Definition,
) -> Result<Vec<DeclareSchema>, PublishError> {
    if !super::label_ok(app_id) {
        return Err(PublishError::new("saved query app id is invalid"));
    }
    let mut declared = Vec::new();
    for (namespace, name) in definition.schema_refs() {
        let source = installed
            .iter()
            .find(|schema| schema.namespace == namespace && schema.name == name)
            .ok_or(PublishError::new("source schema is not installed"))?;
        declared.push(project_schema(app_id, binding, source)?);
    }
    check_shapes(definition, &declared)?;
    check_links(definition, &declared)?;
    Ok(declared)
}

fn project_schema(
    app_id: &str,
    binding: &SpecBinding,
    source: &RawSchema,
) -> Result<DeclareSchema, PublishError> {
    if source.owner_app_id != app_id {
        return Err(PublishError::new("app does not own the source schema"));
    }
    if admit_namespace(Some(binding), &source.namespace)? != NamespaceClass::Official {
        return Err(PublishError::new("namespace is not shareable"));
    }
    if !schema_is_shareable(binding, &source.namespace, &source.name)? {
        return Err(PublishError::new("schema is not shareable"));
    }
    let fields = shareable_fields(binding, source)?;
    let files = file_is_shareable(binding, &source.namespace, &source.name)?;
    let names: Vec<&str> = fields.iter().map(String::as_str).collect();
    if !record_is_shareable(binding, &source.namespace, &source.name, &names, files)? {
        return Err(PublishError::new("record is not shareable"));
    }
    let mut field_types = BTreeMap::new();
    let mut field_descriptions = BTreeMap::new();
    for field in &fields {
        let field_type = source
            .field_types
            .get(field)
            .ok_or(PublishError::new("source schema is missing a field type"))?;
        field_types.insert(field.clone(), field_type.clone());
        let description = source
            .field_descriptions
            .get(field)
            .filter(|value| !value.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("Published {field}."));
        field_descriptions.insert(field.clone(), description);
    }
    Ok(DeclareSchema {
        namespace: source.namespace.clone(),
        name: source.name.clone(),
        schema_type: source.schema_type.clone(),
        record_keys: source.record_keys.clone(),
        fields,
        field_types,
        field_descriptions,
        files,
    })
}

fn shareable_fields(
    binding: &SpecBinding,
    source: &RawSchema,
) -> Result<Vec<String>, PublishError> {
    let listed = schemas(binding)?;
    let spec = listed
        .iter()
        .find(|item| item.namespace == source.namespace && item.name == source.name)
        .ok_or(PublishError::new("schema is not shareable"))?;
    let mut fields = Vec::new();
    for field in spec.fields {
        if !source.fields.iter().any(|item| item == field) {
            continue;
        }
        if field_is_shareable(binding, &source.namespace, &source.name, field)? {
            fields.push((*field).to_owned());
        }
    }
    if fields.is_empty() {
        return Err(PublishError::new("schema has no shareable field"));
    }
    Ok(fields)
}

fn check_shapes(definition: &Definition, declared: &[DeclareSchema]) -> Result<(), PublishError> {
    for read in &definition.reads {
        let kind = schema_kind(declared, &read.namespace, &read.schema)?;
        let point = matches!(read.range, RangeSelect::Point);
        let hash_schema = kind == "Hash";
        if hash_schema && !point {
            return Err(PublishError::new("a hash read takes no range"));
        }
        if !hash_schema && point {
            return Err(PublishError::new("a hash-range read needs a range or *"));
        }
        if kind != "Hash" && kind != "HashRange" {
            return Err(PublishError::new("saved query schema key is not supported"));
        }
    }
    Ok(())
}

fn check_links(definition: &Definition, declared: &[DeclareSchema]) -> Result<(), PublishError> {
    for link in &definition.links {
        let from = find_declared(declared, &link.from_namespace, &link.from_schema)?;
        if !from.fields.iter().any(|field| field == &link.field) {
            return Err(PublishError::new("saved query link field is not shareable"));
        }
        if schema_kind(declared, &link.namespace, &link.schema)? != "Hash" {
            return Err(PublishError::new(
                "saved query link target is not a hash schema",
            ));
        }
    }
    Ok(())
}

fn schema_kind<'a>(
    declared: &'a [DeclareSchema],
    namespace: &str,
    name: &str,
) -> Result<&'a str, PublishError> {
    Ok(find_declared(declared, namespace, name)?
        .schema_type
        .as_str())
}

fn find_declared<'a>(
    declared: &'a [DeclareSchema],
    namespace: &str,
    name: &str,
) -> Result<&'a DeclareSchema, PublishError> {
    declared
        .iter()
        .find(|schema| schema.namespace == namespace && schema.name == name)
        .ok_or(PublishError::new("source schema is not installed"))
}

fn string_list(value: Option<&Value>) -> Result<Vec<String>, PublishError> {
    let Some(Value::Array(items)) = value else {
        return Err(PublishError::new("source schema has no fields"));
    };
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or(PublishError::new("source schema has no fields"))
        })
        .collect()
}

fn string_map(value: Option<&Value>) -> Result<BTreeMap<String, String>, PublishError> {
    let Some(Value::Object(map)) = value else {
        return Ok(BTreeMap::new());
    };
    map_strings(map)
}

fn map_strings(map: &Map<String, Value>) -> Result<BTreeMap<String, String>, PublishError> {
    let mut out = BTreeMap::new();
    for (key, value) in map {
        let Some(text) = value.as_str() else {
            return Err(PublishError::new("source schema field type is invalid"));
        };
        out.insert(key.clone(), text.to_owned());
    }
    Ok(out)
}
