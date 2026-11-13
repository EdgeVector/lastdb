//! Declarative schema definition, catalog identity hashing, and wire codec.

use crate::{
    compute_field_hash, refuses_identity_downgrade, DeclarativeSchemaType, FieldValueType,
    IdentityRecompute, KeyConfig, OwnedDeclaration, IDENTITY_HASH_ALGO_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

const RECORD_KEYS_IDENTITY_HASH_ALGO_VERSION: u32 = 3;
const FIELD_DERIVED_IDENTITY_HASH_ALGO_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "Self")]
pub struct DeclarativeSchemaDefinition {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptive_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    pub schema_type: DeclarativeSchemaType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<KeyConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub alternate_keys: BTreeMap<String, crate::AlternateKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_keys: Option<crate::record_keys::RecordKeys>,
    #[serde(default, skip_serializing_if = "crate::alternate_keys::is_false")]
    pub file_required: bool,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_classifications: HashMap<String, Vec<String>>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_descriptions: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_interest_categories: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_types: HashMap<String, FieldValueType>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_hashes: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_versions: HashMap<String, u32>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_declarations: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_hash_algo_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_app_id: Option<String>,
}

impl DeclarativeSchemaDefinition {
    #[must_use]
    pub fn get_field_type(&self, field_name: &str) -> &FieldValueType {
        self.field_types
            .get(field_name)
            .unwrap_or(&FieldValueType::Any)
    }

    pub fn ensure_field_hashes(&mut self) {
        let mut seen = HashSet::new();
        let names: Vec<String> = self
            .fields
            .iter()
            .flatten()
            .chain(self.field_types.keys())
            .chain(self.field_descriptions.keys())
            .filter(|n| !self.field_hashes.contains_key(*n) && seen.insert((*n).clone()))
            .cloned()
            .collect();
        for name in names {
            let desc = self
                .field_descriptions
                .get(&name)
                .map_or("", String::as_str);
            let ver = *self.field_versions.entry(name.clone()).or_insert(1);
            let hash = compute_field_hash(&name, desc, self.get_field_type(&name), ver);
            self.field_hashes.insert(name, hash);
        }
    }

    pub fn validate_definition(&self) -> Result<(), String> {
        if self.file_required && self.record_keys.is_none() {
            return Err("file_required needs explicit record keys".into());
        }
        let keys = self
            .key
            .iter()
            .flat_map(|k| [k.hash_field.as_deref(), k.range_field.as_deref()])
            .flatten();
        self.fields
            .iter()
            .flatten()
            .chain(self.field_classifications.keys())
            .chain(self.field_descriptions.keys())
            .chain(self.field_interest_categories.keys())
            .chain(self.field_types.keys())
            .chain(self.field_hashes.keys())
            .chain(self.field_declarations.keys())
            .chain(self.field_versions.keys())
            .map(String::as_str)
            .chain(keys)
            .try_for_each(Self::validate_field_name)?;
        self.validate_key_field_types()?;
        if !self.alternate_keys.is_empty() || self.record_keys.is_some() {
            self.validate_owned_schema()?;
        }
        Ok(())
    }

    pub fn compute_identity_hash(&mut self) {
        let names: BTreeSet<&str> = self.fields.iter().flatten().map(String::as_str).collect();
        let names: Vec<&str> = names.into_iter().collect();
        let mut hasher = Sha256::new();
        if let Some(app_id) = self.owner_app_id.as_deref().filter(|s| !s.is_empty()) {
            hasher.update(format!("app:{app_id}:").as_bytes());
        }
        let label = self
            .descriptive_name
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.name);
        hasher.update(format!("{label}:{}", names.join(",")).as_bytes());
        if let Some(k) = &self.key {
            let (hf, rf) = (
                k.hash_field.as_deref().map_or("", str::trim),
                k.range_field.as_deref().map_or("", str::trim),
            );
            if !hf.is_empty() || !rf.is_empty() {
                hasher.update(format!(":key:{hf}:{rf}").as_bytes());
            }
        }
        let base = format!("{:x}", hasher.finalize());
        if let Some(keys) = &self.record_keys {
            let mut hash = Sha256::new();
            hash.update(b"lastdb:record-schema:v1\0");
            hash.update(base.as_bytes());
            hash.update(serde_json::to_vec(keys).expect("record key definitions serialize"));
            if self.file_required {
                hash.update(b"\0file-required");
            }
            self.identity_hash = Some(format!("{:x}", hash.finalize()));
            self.identity_hash_algo_version = Some(if self.file_required {
                IDENTITY_HASH_ALGO_VERSION
            } else {
                RECORD_KEYS_IDENTITY_HASH_ALGO_VERSION
            });
        } else {
            self.identity_hash = Some(base);
            self.identity_hash_algo_version = Some(FIELD_DERIVED_IDENTITY_HASH_ALGO_VERSION);
        }
    }

    #[must_use]
    pub fn identity_is_newer_than_binary(&self) -> bool {
        refuses_identity_downgrade(
            self.identity_hash.as_deref(),
            self.identity_hash_algo_version,
        )
        .is_some()
    }

    #[must_use = "a refused downgrade must be surfaced, not dropped"]
    pub fn recompute_identity_hash_unless_newer(&mut self) -> IdentityRecompute {
        if let Some(stored) = refuses_identity_downgrade(
            self.identity_hash.as_deref(),
            self.identity_hash_algo_version,
        ) {
            return IdentityRecompute::RefusedDowngrade {
                stored,
                binary: IDENTITY_HASH_ALGO_VERSION,
            };
        }
        self.compute_identity_hash();
        IdentityRecompute::Recomputed
    }

    #[must_use]
    pub fn get_identity_hash(&self) -> Option<&String> {
        self.identity_hash.as_ref()
    }

    pub fn normalize_owned_declaration(
        &mut self,
        namespace_label: &str,
        namespace: &str,
    ) -> Result<OwnedDeclaration, String> {
        let namespace = namespace.trim().to_owned();
        if namespace.is_empty() || namespace.contains('/') {
            return Err(format!(
                "{namespace_label} must be non-empty and must not contain '/'"
            ));
        }
        let raw_name = self.name.trim();
        if raw_name.is_empty() {
            return Err("schema.name must be non-empty".into());
        }
        let local_name = match raw_name.split_once('/') {
            Some((owner, local)) if owner == namespace && !local.is_empty() => local,
            Some(_) => {
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
            .is_none_or(|n| n.trim().is_empty())
        {
            self.descriptive_name = Some(local_name.to_owned());
        }
        self.name.clone_from(&local_schema_id);
        self.validate_owned_schema()?;
        self.compute_identity_hash();
        self.validate_definition()
            .map_err(|e| format!("Invalid data: {e}"))?;
        self.require_typed_fields()?;
        Ok(OwnedDeclaration {
            namespace,
            local_schema_id,
        })
    }

    pub fn normalize_wire(&mut self) {
        self.hash = None;
        self.superseded_by = None;
        if self.identity_hash.is_none() && self.owner_app_id.is_some() {
            self.compute_identity_hash();
        }
    }

    pub(crate) fn require_typed_fields(&self) -> Result<(), String> {
        let fields = self.fields.as_deref().unwrap_or_default();
        if (fields.is_empty() && self.record_keys.is_none())
            || fields.iter().any(|f| f.trim().is_empty())
        {
            return Err("schema.fields must be non-empty without blank names".into());
        }
        let untyped: Vec<&str> = fields
            .iter()
            .filter(|f| !self.field_types.contains_key(*f))
            .map(String::as_str)
            .collect();
        untyped.is_empty().then_some(()).ok_or_else(|| {
            format!(
                "every field needs a type in schema.field_types; these fields have none: {}",
                untyped.join(", ")
            )
        })
    }
}

impl PartialEq for DeclarativeSchemaDefinition {
    fn eq(&self, o: &Self) -> bool {
        self.name == o.name
            && self.descriptive_name == o.descriptive_name
            && self.purpose_statement == o.purpose_statement
            && self.schema_type == o.schema_type
            && self.key == o.key
            && self.alternate_keys == o.alternate_keys
            && self.record_keys == o.record_keys
            && self.file_required == o.file_required
            && self.fields == o.fields
            && self.hash == o.hash
            && self.field_classifications == o.field_classifications
            && self.field_descriptions == o.field_descriptions
            && self.field_interest_categories == o.field_interest_categories
            && self.field_types == o.field_types
            && self.field_hashes == o.field_hashes
            && self.field_declarations == o.field_declarations
            && self.field_versions == o.field_versions
            && self.identity_hash == o.identity_hash
            && self.superseded_by == o.superseded_by
            && self.owner_app_id == o.owner_app_id
    }
}

impl Serialize for DeclarativeSchemaDefinition {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for DeclarativeSchemaDefinition {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_definition(Map::<String, Value>::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

fn decode_definition(mut map: Map<String, Value>) -> Result<DeclarativeSchemaDefinition, String> {
    if let Some(bad) = ["field_key_id", "record_mapper"]
        .into_iter()
        .find(|n| map.contains_key(*n))
    {
        return Err(format!("schema-level '{bad}' is not supported"));
    }
    if let Some(raw) = map.remove("fields") {
        let names = match raw {
            Value::Null => None,
            Value::Array(arr) => Some(
                arr.into_iter()
                    .map(|v| {
                        v.as_str()
                            .map(str::to_owned)
                            .ok_or("Invalid fields array; expected strings")
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Value::Object(obj) => {
                let mut ns: Vec<String> = obj.into_iter().map(|(k, _)| k).collect();
                ns.sort();
                Some(ns)
            }
            _ => return Err("Invalid fields; expected array or object map".into()),
        };
        if let Some(ns) = names {
            map.insert(
                "fields".into(),
                Value::Array(ns.into_iter().map(Value::String).collect()),
            );
        }
    }
    let had_type = map.get("schema_type").is_some_and(|v| !v.is_null());
    if !had_type {
        map.insert("schema_type".into(), Value::String("Single".into()));
    }
    let mut s =
        DeclarativeSchemaDefinition::deserialize(Value::Object(map)).map_err(|e| e.to_string())?;
    if let Some(keys) = &s.record_keys {
        keys.validate()?;
        let kind = match keys.primary.kind {
            crate::record_keys::RecordKeyKind::Hash => DeclarativeSchemaType::Hash,
            crate::record_keys::RecordKeyKind::HashRange => DeclarativeSchemaType::HashRange,
        };
        if s.key.is_some() || !s.alternate_keys.is_empty() || (had_type && s.schema_type != kind) {
            return Err(
                "record_keys cannot be mixed with field-derived or conflicting key declarations"
                    .into(),
            );
        }
        s.schema_type = kind;
    } else if !had_type {
        let (h, r) = s.key.as_ref().map_or((false, false), |k| {
            (k.hash_field.is_some(), k.range_field.is_some())
        });
        s.schema_type = match (h, r) {
            (true, true) => DeclarativeSchemaType::HashRange,
            (true, false) => DeclarativeSchemaType::Hash,
            (false, true) => DeclarativeSchemaType::Range,
            (false, false) => DeclarativeSchemaType::Single,
        };
    }
    if s.identity_hash.is_none() && (s.owner_app_id.is_some() || s.file_required) {
        s.compute_identity_hash();
    }
    s.validate_definition()?;
    Ok(s)
}
