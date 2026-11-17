//! Declarative schema definition, catalog identity hashing, and wire codec.

use crate::{
    compute_field_hash, refuses_identity_downgrade, DeclarativeSchemaType, FieldValueType,
    IdentityRecompute::{self, Recomputed, RefusedDowngrade},
    OwnedDeclaration, IDENTITY_HASH_ALGO_VERSION as BIN_VER,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, HashSet};

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
    pub record_keys: Option<crate::record_keys::RecordKeys>,
    #[serde(default, skip_serializing_if = "crate::is_false")]
    pub file_required: bool,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
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
    pub owner_app_id: Option<String>,
}

impl DeclarativeSchemaDefinition {
    pub fn validate_field_name(name: &str) -> Result<(), String> {
        if matches!(name, "\u{1f}record" | "__record") {
            let msg = format!("field name {name:?} is reserved and is not supported");
            return Err(msg);
        }
        let bad = |c: char| c.is_whitespace() || c.is_control() || matches!(c, ',' | ':');
        if name.is_empty() || name.chars().any(bad) {
            return Err(format!("field name {name:?} must be nonempty and cannot contain whitespace, control characters, commas, or colons"));
        }
        Ok(())
    }

    #[must_use]
    pub fn get_field_type(&self, name: &str) -> &FieldValueType {
        self.field_types.get(name).unwrap_or(&FieldValueType::Any)
    }

    #[must_use]
    pub fn identity_is_newer_than_binary(&self) -> bool {
        let ver = self.identity_hash_algo_version;
        refuses_identity_downgrade(self.identity_hash.as_deref(), ver).is_some()
    }

    #[must_use = "a refused downgrade must be surfaced, not dropped"]
    pub fn recompute_identity_hash_unless_newer(&mut self) -> IdentityRecompute {
        let ver = self.identity_hash_algo_version;
        if let Some(stored) = refuses_identity_downgrade(self.identity_hash.as_deref(), ver) {
            return RefusedDowngrade {
                stored,
                binary: BIN_VER,
            };
        }
        self.compute_identity_hash();
        Recomputed
    }

    pub fn normalize_wire(&mut self) {
        if self.identity_hash.is_none() && self.owner_app_id.is_some() {
            self.compute_identity_hash();
        }
    }

    pub fn validate_owned_schema(&self) -> Result<(), String> {
        let Some(keys) = &self.record_keys else {
            return Err("owned schemas require explicit record_keys".into());
        };
        keys.validate()?;
        if self.schema_type != keys.primary.kind.schema_type() {
            let msg =
                "record_keys require one owned Hash/HashRange layout without field-derived keys";
            return Err(msg.into());
        }
        Ok(())
    }

    #[must_use]
    pub fn alternate_key_revision(&self) -> String {
        let bytes = self.record_keys.as_ref().map_or_else(
            || b"[true,{}]".to_vec(),
            |k| serde_json::to_vec(k).expect("record key declarations serialize"),
        );
        format!("{:x}", Sha256::digest(bytes))
    }

    pub fn ensure_field_hashes(&mut self) {
        let mut seen = HashSet::new();
        let all = self.fields.iter().flatten().chain(self.field_types.keys());
        let names: Vec<String> = all
            .chain(self.field_descriptions.keys())
            .filter(|n| !self.field_hashes.contains_key(*n) && seen.insert((*n).clone()))
            .cloned()
            .collect();
        for n in names {
            let d = self.field_descriptions.get(&n).map_or("", String::as_str);
            let ver = *self.field_versions.entry(n.clone()).or_insert(1);
            let hash = compute_field_hash(&n, d, self.get_field_type(&n), ver);
            self.field_hashes.insert(n, hash);
        }
    }

    pub fn validate_definition(&self) -> Result<(), String> {
        if self.file_required && self.record_keys.is_none() {
            return Err("file_required needs explicit record keys".into());
        }
        let maps = [
            &self.field_descriptions,
            &self.field_interest_categories,
            &self.field_hashes,
            &self.field_declarations,
        ];
        for f in self
            .fields
            .iter()
            .flatten()
            .chain(self.field_types.keys())
            .chain(self.field_classifications.keys())
            .chain(self.field_versions.keys())
            .chain(maps.into_iter().flat_map(HashMap::keys))
        {
            Self::validate_field_name(f)?;
        }
        if self.record_keys.is_some() {
            self.validate_owned_schema()?;
        }
        Ok(())
    }

    pub fn compute_identity_hash(&mut self) {
        let Some(keys) = &self.record_keys else {
            (self.identity_hash, self.identity_hash_algo_version) = (None, None);
            return;
        };
        let names = self
            .fields
            .iter()
            .flatten()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut hasher = Sha256::new();
        if let Some(app_id) = self.owner_app_id.as_deref().filter(|s| !s.is_empty()) {
            hasher.update(format!("app:{app_id}:").as_bytes());
        }
        let dn = self.descriptive_name.as_deref().filter(|s| !s.is_empty());
        hasher.update(format!("{}:{}", dn.unwrap_or(&self.name), names.join(",")).as_bytes());
        let mut hash = Sha256::new();
        hash.update(b"lastdb:record-schema:v1\0");
        hash.update(format!("{:x}", hasher.finalize()).as_bytes());
        hash.update(serde_json::to_vec(keys).expect("record key definitions serialize"));
        if self.file_required {
            hash.update(b"\0file-required");
        }
        self.identity_hash = Some(format!("{:x}", hash.finalize()));
        let ver = if self.file_required { BIN_VER } else { 3 };
        self.identity_hash_algo_version = Some(ver);
    }

    pub fn normalize_owned_declaration(
        &mut self,
        ns_label: &str,
        ns: &str,
    ) -> Result<OwnedDeclaration, String> {
        let namespace = ns.trim().to_owned();
        if namespace.is_empty() || namespace.contains('/') {
            let msg = format!("{ns_label} must be non-empty and must not contain '/'");
            return Err(msg);
        }
        let raw = self.name.trim();
        if raw.is_empty() {
            return Err("schema.name must be non-empty".into());
        }
        let local = match raw.split_once('/') {
            Some((owner, loc)) if owner == namespace && !loc.is_empty() => loc,
            Some(_) => {
                let msg = format!("schema.name namespace must match request.{ns_label}");
                return Err(msg);
            }
            None => raw,
        };
        let local_schema_id = format!("{namespace}/{local}");
        self.owner_app_id = Some(namespace.clone());
        let dn = self.descriptive_name.as_deref();
        if dn.is_none_or(|n| n.trim().is_empty()) {
            self.descriptive_name = Some(local.to_owned());
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

    pub(crate) fn require_typed_fields(&self) -> Result<(), String> {
        let fields = self.fields.as_deref().unwrap_or_default();
        if (fields.is_empty() && self.record_keys.is_none())
            || fields.iter().any(|f| f.trim().is_empty())
        {
            return Err("schema.fields must be non-empty without blank names".into());
        }
        let untyped: Vec<&str> = fields
            .iter()
            .map(String::as_str)
            .filter(|&f| !self.field_types.contains_key(f))
            .collect();
        if !untyped.is_empty() {
            return Err(format!(
                "every field needs a type in schema.field_types; these fields have none: {}",
                untyped.join(", ")
            ));
        }
        Ok(())
    }
}

impl PartialEq for DeclarativeSchemaDefinition {
    fn eq(&self, o: &Self) -> bool {
        self.name == o.name
            && self.descriptive_name == o.descriptive_name
            && self.purpose_statement == o.purpose_statement
            && self.schema_type == o.schema_type
            && self.record_keys == o.record_keys
            && self.file_required == o.file_required
            && self.fields == o.fields
            && self.field_classifications == o.field_classifications
            && self.field_descriptions == o.field_descriptions
            && self.field_interest_categories == o.field_interest_categories
            && self.field_types == o.field_types
            && self.field_hashes == o.field_hashes
            && self.field_declarations == o.field_declarations
            && self.field_versions == o.field_versions
            && self.identity_hash == o.identity_hash
            && self.owner_app_id == o.owner_app_id
    }
}

impl Serialize for DeclarativeSchemaDefinition {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, s)
    }
}

impl<'de> Deserialize<'de> for DeclarativeSchemaDefinition {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        decode_definition(Map::<String, Value>::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

fn decode_definition(mut map: Map<String, Value>) -> Result<DeclarativeSchemaDefinition, String> {
    if let Some(k) = ["field_key_id", "record_mapper"]
        .into_iter()
        .find(|k| map.contains_key(*k))
    {
        return Err(format!("schema-level '{k}' is not supported"));
    }
    if map.remove("key").is_some_and(|v| !v.is_null()) {
        return Err("schema-level 'key' is not supported; use record_keys".into());
    }
    let alt = map.remove("alternate_keys");
    if alt.is_some_and(|v| !v.is_null() && !v.as_object().is_some_and(Map::is_empty)) {
        return Err("schema-level 'alternate_keys' is not supported; use record_keys".into());
    }
    match map.remove("fields") {
        None | Some(Value::Null) => {}
        Some(Value::Array(arr)) => {
            let ns: Vec<String> = arr
                .into_iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or("Invalid fields array; expected strings")
                })
                .collect::<Result<_, _>>()?;
            map.insert("fields".into(), ns.into());
        }
        Some(Value::Object(obj)) => {
            let mut ns: Vec<String> = obj.into_iter().map(|(k, _)| k).collect();
            ns.sort();
            map.insert("fields".into(), ns.into());
        }
        Some(_) => return Err("Invalid fields; expected array or object map".into()),
    }
    let had_type = map.get("schema_type").is_some_and(|v| !v.is_null());
    if !had_type {
        map.insert("schema_type".into(), Value::String("Single".into()));
    }
    let raw = Value::Object(map);
    let mut s = DeclarativeSchemaDefinition::deserialize(raw).map_err(|e| e.to_string())?;
    if !had_type {
        let single = DeclarativeSchemaType::Single;
        s.schema_type = s
            .record_keys
            .as_ref()
            .map_or(single, |k| k.primary.kind.schema_type());
    }
    s.validate_definition()?;
    if s.identity_hash.is_none() && (s.owner_app_id.is_some() || s.file_required) {
        s.compute_identity_hash();
    }
    Ok(s)
}
