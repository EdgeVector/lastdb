use schema_service_core::{
    types::CanonicalField, Clock, DeclareFieldInput, DeclareFieldRequest, DisabledEmbeddingModel,
    ExternalSchemaPersistence, SchemaConfig, SchemaServiceState,
};
use schema_service_server_shared::SystemClock;
use schema_types::{DeclarativeSchemaType, FieldValueType, KeyConfig, Schema};
use std::{future::Future, sync::Arc};

pub fn run<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

pub async fn state(backend: Arc<dyn ExternalSchemaPersistence>) -> SchemaServiceState {
    SchemaServiceState::new_with_external(
        backend,
        Arc::new(DisabledEmbeddingModel),
        SchemaConfig::default(),
        process_clock(),
    )
    .await
    .expect("empty or valid backend")
}

/// The shell adapter. Canonical tests pass the same clock the process entry uses.
pub fn process_clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

pub fn field(description: &str, field_type: FieldValueType) -> CanonicalField {
    CanonicalField {
        description: description.into(),
        field_type,
        version: 1,
        name_policy_version: 1,
        classification: None,
        interest_category: None,
    }
}

pub fn declaration(owner: &str, handle: &str) -> DeclareFieldRequest {
    DeclareFieldRequest {
        owner_app_id: owner.into(),
        handle: handle.into(),
        fields: vec![DeclareFieldInput {
            name: "parcel_delivery_status".into(),
            field_type: FieldValueType::String,
            description: "Parcel delivery status code from the carrier".into(),
            version: 1,
        }],
        readers: Vec::new(),
    }
}

pub fn schema(owner: &str, reverse: bool) -> Schema {
    let mut fields = vec!["parcel_identifier".into(), "parcel_delivery_status".into()];
    if reverse {
        fields.reverse();
    }
    let mut schema = Schema::new(
        "Parcels".into(),
        DeclarativeSchemaType::Hash,
        Some(KeyConfig::new(Some("parcel_identifier".into()), None)),
        Some(fields),
        None,
        None,
    );
    schema.descriptive_name = Some("Parcels".into());
    schema.purpose_statement = Some("Parcels tracked by a delivery carrier".into());
    schema.owner_app_id = Some(owner.into());
    schema.field_descriptions.insert(
        "parcel_identifier".into(),
        "Parcel identifier assigned by the carrier".into(),
    );
    schema.field_descriptions.insert(
        "parcel_delivery_status".into(),
        "Parcel delivery status code from the carrier".into(),
    );
    schema
        .field_types
        .insert("parcel_identifier".into(), FieldValueType::String);
    schema
        .field_types
        .insert("parcel_delivery_status".into(), FieldValueType::String);
    schema
}
