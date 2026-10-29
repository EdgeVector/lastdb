//! In-memory state, seed load, and schema identity rows.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use schema_service_core::snapshot::AppMetadata;
use schema_service_core::{
    AppIdentityConfig, AppRegisterOutcome, AppRegisterRequest, DisabledEmbeddingModel,
    SchemaServiceState,
};
use schema_types::{DeclarativeSchemaType, KeyConfig, Schema};
use serde_json::{json, Value};

use crate::release::Fixture;
use crate::{fail, normalize};

pub(crate) struct Prepared {
    pub(crate) state: SchemaServiceState,
    pub(crate) seed_hashes: HashSet<String>,
    pub(crate) operations: Vec<String>,
    pub(crate) schema_hashes: HashMap<String, String>,
}

pub(crate) async fn prepare(fixture: &Fixture) -> Prepared {
    let state = open_state(fixture);
    schema_service_core::builtin_canonical_fields::seed(&state)
        .await
        .unwrap_or_else(|error| fail("canonical field seed", error));
    schema_service_core::builtin_schemas::seed(&state)
        .await
        .unwrap_or_else(|error| fail("builtin schema seed", error));
    schema_service_core::schema_org_seeds::seed(&state)
        .unwrap_or_else(|error| fail("schema.org seed", error));

    let mut operations = Vec::new();
    register_app(&state, fixture, &mut operations).await;
    let seed_hashes = schema_hashes(&state);
    let schema_hashes = add_fixed_schemas(&state, &mut operations).await;
    Prepared {
        state,
        seed_hashes,
        operations,
        schema_hashes,
    }
}

pub(crate) fn canonical_field_rows(state: &SchemaServiceState) -> Vec<Value> {
    let fields = state
        .canonical_fields
        .read()
        .unwrap_or_else(|_| fail("canonical fields", "lock poisoned"));
    let mut rows: Vec<_> = fields
        .iter()
        .map(|(name, field)| {
            json!({
                "classification": field.classification,
                "description": field.description,
                "field_hash": field.field_hash(name),
                "field_type": field.field_type,
                "interest_category": field.interest_category,
                "name": name,
                "name_policy_version": field.name_policy_version,
                "version": field.version,
            })
        })
        .map(normalize)
        .collect();
    rows.sort_by_key(row_name);
    rows
}

pub(crate) fn identity_rows(prepared: &Prepared) -> Vec<Value> {
    let schemas = prepared
        .state
        .get_all_schemas_cached()
        .unwrap_or_else(|error| fail("list schemas", error));
    let mut rows: Vec<_> = schemas
        .iter()
        .map(|schema| identity_row(&prepared.state, schema, &prepared.seed_hashes))
        .collect();
    rows.sort_by_key(|left| row_str(left, "identity_hash"));
    rows
}

fn open_state(fixture: &Fixture) -> SchemaServiceState {
    let embedder = Arc::new(DisabledEmbeddingModel);
    #[cfg(schema_core_pre_config)]
    let state = SchemaServiceState::new_in_memory(embedder)
        .unwrap_or_else(|error| fail("in-memory state", error));
    #[cfg(not(schema_core_pre_config))]
    let state =
        SchemaServiceState::new_in_memory(embedder, schema_service_core::SchemaConfig::default())
            .unwrap_or_else(|error| fail("in-memory state", error));
    state.configure_app_identity(AppIdentityConfig {
        deployment_env: app_identity_crypto::Env::Dev,
        trusted_roots: fixture.trusted_roots(),
        revoked_dev_pubkeys: HashSet::new(),
        verifier_pubkey: None,
    });
    state
}

async fn register_app(state: &SchemaServiceState, fixture: &Fixture, operations: &mut Vec<String>) {
    let body = serde_json::to_value(AppRegisterRequest {
        app_id: crate::release::APP_ID.to_string(),
        metadata: AppMetadata {
            display_name: "Baseline".to_string(),
            description: "Fixed input for the schema core baseline snapshot.".to_string(),
            homepage_url: "https://baseline.invalid/app".to_string(),
            icon_url: None,
        },
        version: "1.0.0".to_string(),
        code_signature: None,
        source: None,
        artifact: None,
        uses: Vec::new(),
    })
    .unwrap_or_else(|error| fail("app body", error));
    let signature = fixture.sign(app_identity_crypto::Purpose::AppRegister, &body);
    match state
        .register_app(&fixture.cert_header, &signature, &body)
        .await
    {
        Ok(AppRegisterOutcome::Created(_)) => operations.push("register_app:created".to_string()),
        Ok(other) => fail("register_app", format!("unexpected {other:?}")),
        Err(error) => fail("register_app", format!("{error:?}")),
    }
}

async fn add_fixed_schemas(
    state: &SchemaServiceState,
    operations: &mut Vec<String>,
) -> HashMap<String, String> {
    let vocabulary = vocabulary(state);
    let mut hashes = HashMap::new();
    for spec in FIXED_SCHEMAS {
        let schema = proposal(*spec, &vocabulary);
        let name = spec.descriptive_name.to_string();
        match state.add_schema(schema, HashMap::new()).await {
            Ok(schema_service_core::types::SchemaAddOutcome::Added(added, _)) => {
                let hash = added
                    .get_identity_hash()
                    .unwrap_or_else(|| fail("add_schema", "missing identity hash"))
                    .clone();
                operations.push(format!("add_schema:{name}:added"));
                hashes.insert(name, hash);
            }
            Ok(other) => fail("add_schema", format!("{name} unexpected {other:?}")),
            Err(error) => fail("add_schema", format!("{name}: {error}")),
        }
    }
    hashes
}

fn proposal(
    spec: FixedSchema,
    vocabulary: &HashMap<String, schema_service_core::types::CanonicalField>,
) -> Schema {
    let fields: Vec<String> = spec.fields.iter().map(|name| (*name).to_string()).collect();
    let mut schema = Schema::new(
        spec.schema_name.to_string(),
        DeclarativeSchemaType::Hash,
        Some(KeyConfig::new(Some(spec.hash_field.to_string()), None)),
        Some(fields.clone()),
        None,
        None,
    );
    schema.descriptive_name = Some(spec.descriptive_name.to_string());
    schema.purpose_statement = Some(spec.purpose.to_string());
    schema.owner_app_id = Some(crate::release::APP_ID.to_string());
    for name in fields {
        let field = vocabulary
            .get(&name)
            .unwrap_or_else(|| fail("vocabulary", format!("missing {name}")));
        schema
            .field_descriptions
            .insert(name.clone(), field.description.clone());
        schema
            .field_types
            .insert(name.clone(), field.field_type.clone());
        schema.field_versions.insert(name, field.version);
    }
    schema
}

fn vocabulary(
    state: &SchemaServiceState,
) -> HashMap<String, schema_service_core::types::CanonicalField> {
    state
        .canonical_fields
        .read()
        .unwrap_or_else(|_| fail("canonical fields", "lock poisoned"))
        .clone()
}

fn schema_hashes(state: &SchemaServiceState) -> HashSet<String> {
    state
        .get_all_schemas_cached()
        .unwrap_or_else(|error| fail("list seed schemas", error))
        .into_iter()
        .filter_map(|schema| schema.get_identity_hash().map(ToString::to_string))
        .collect()
}

fn identity_row(
    state: &SchemaServiceState,
    schema: &Schema,
    seed_hashes: &HashSet<String>,
) -> Value {
    let identity_hash = schema
        .get_identity_hash()
        .unwrap_or_else(|| fail("identity", "schema has no identity hash"))
        .clone();
    let origin = if seed_hashes.contains(&identity_hash) {
        "seed"
    } else {
        "fixed"
    };
    let mut fields = schema.fields.clone().unwrap_or_default();
    fields.sort();
    let field_hashes = sorted_field_hashes(schema);
    normalize(json!({
        "descriptive_name": schema.descriptive_name,
        "field_hashes": field_hashes,
        "fields": fields,
        "identity_hash": identity_hash,
        "identity_hash_algo_version": schema.identity_hash_algo_version(),
        "key": schema.key,
        "origin": origin,
        "owner_app_id": schema.owner_app_id,
        "purpose_statement": schema.purpose_statement,
        "schema_type": schema.schema_type,
        "source": schema.source,
        "system": state.is_system_schema(&identity_hash),
    }))
}

fn sorted_field_hashes(schema: &Schema) -> serde_json::Map<String, Value> {
    let mut pairs: Vec<_> = schema
        .field_hashes
        .iter()
        .map(|(name, hash)| (name.clone(), Value::String(hash.clone())))
        .collect();
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    pairs.into_iter().collect()
}

fn row_name(value: &Value) -> String {
    row_str(value, "name")
}

fn row_str(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

#[derive(Clone, Copy)]
struct FixedSchema {
    schema_name: &'static str,
    descriptive_name: &'static str,
    purpose: &'static str,
    hash_field: &'static str,
    fields: &'static [&'static str],
}

const FIXED_SCHEMAS: &[FixedSchema] = &[
    FixedSchema {
        schema_name: "family_profiles",
        descriptive_name: "Family Profiles",
        purpose: "Profiles of family members",
        hash_field: "user_id",
        fields: &["user_id", "person_full_name", "user_email"],
    },
    FixedSchema {
        schema_name: "concert_photos",
        descriptive_name: "Concert Photos",
        purpose: "Photographs taken at concerts",
        hash_field: "photo_url",
        fields: &["photo_url", "photo_caption", "photo_taken_at"],
    },
];
