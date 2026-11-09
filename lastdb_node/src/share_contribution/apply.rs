//! Apply verified contributor intent through the ordinary durable record writer.
use crate::host::Host;
use cloud_backup_protocol::{Digest, Id};
use fold_db::canonical::CanonicalWriter;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::cas::CasExpectation;
use fold_db::schema::types::mutation::ContributionAdmission;
use fold_db::schema::types::{KeyValue, Mutation, MutationType, SchemaError};
use hkdf::Hkdf;
use lastdb_host::{HostNode, Lane};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::{BTreeMap, HashMap};
use zeroize::Zeroizing;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRule {
    pub schema: String,
    pub definition_id: Digest,
    pub action: MutationType,
}

impl WriteRule {
    pub(super) fn valid(&self) -> bool {
        self.action == MutationType::Create
            && self.schema.len() <= 256
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
}

pub(super) async fn apply(
    host: &Host,
    writer: &Digest,
    rules: &[WriteRule],
    request: WriteRequest,
) -> Result<bool, String> {
    if request.action != MutationType::Create
        || !rules.iter().any(|rule| {
            rule.schema == request.schema
                && rule.definition_id == request.definition_id
                && rule.action == request.action
        })
    {
        return Ok(false);
    }
    let schema = host
        .db
        .schema_manager()
        .resolve_schema(&request.schema)
        .await
        .map_err(|_| "cannot resolve share contribution schema")?;
    let Some(schema) = schema else {
        return Ok(false);
    };
    let Ok(keys) = schema.require_record_keys() else {
        return Ok(false);
    };
    let mut alternates = request.alternates.clone();
    for (name, definition) in &keys.alternates {
        if definition.allow_absent {
            alternates.entry(name.clone()).or_insert(None);
        }
    }
    let target = RecordTarget {
        schema: request.schema.clone(),
        key: request.key.clone(),
        alternates,
    };
    let fields = schema.fields.as_deref().unwrap_or_default();
    if target.validate(keys).is_err()
        || request.record.iter().any(|(field, value)| {
            !fields.contains(field) || schema.get_field_type(field).validate(value).is_err()
        })
    {
        return Ok(false);
    }
    let encoded = serde_json::to_vec(&request).map_err(|_| "cannot encode contribution intent")?;
    let permit = host
        .acquire_op_permit(Lane::for_write_bytes(encoded.len()))
        .await
        .map_err(|_| "share mutation writer is busy")?;
    let admission = admission(host, writer, &request, &encoded)?;
    let id = uuid::Builder::from_random_bytes(
        admission.receipt_id[..16]
            .try_into()
            .expect("the receipt ID has 32 bytes"),
    )
    .into_uuid();
    let mut mutation = Mutation::with_uuid(
        id.to_string(),
        target,
        request.record.into_iter().collect(),
        host.public_key(),
        MutationType::Create,
        CasExpectation::Absent,
    );
    mutation.metadata = Some(HashMap::from([(
        "share_author".into(),
        writer.as_str().to_owned(),
    )]));
    mutation.contribution = Some(admission);
    mutation.synchronous = Some(true);
    let outcome = host
        .db
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await;
    drop(permit);
    match outcome {
        Ok(_) => Ok(true),
        Err(
            SchemaError::NotFound(_)
            | SchemaError::InvalidField(_)
            | SchemaError::CasConflict { .. }
            | SchemaError::RecordTooLarge { .. },
        ) => Ok(false),
        Err(_) => Err("native share mutation commit unavailable".into()),
    }
}

fn admission(
    host: &Host,
    writer: &Digest,
    request: &WriteRequest,
    encoded: &[u8],
) -> Result<ContributionAdmission, String> {
    let seed = Zeroizing::new(host.keypair.secret_key_bytes());
    let hkdf = Hkdf::<Sha256>::new(Some(b"lastdb:share-contribution-receipt:v1"), &*seed);
    let identity = CanonicalWriter::new()
        .field(b"identity")
        .field(writer.as_str().as_bytes())
        .field(request.mutation_id.as_str().as_bytes())
        .finish();
    let intent = CanonicalWriter::new()
        .field(b"intent")
        .field(&identity)
        .field(encoded)
        .finish();
    let mut receipt_id = [0; 32];
    let mut intent_digest = [0; 32];
    hkdf.expand(&identity, &mut receipt_id)
        .and_then(|()| hkdf.expand(&intent, &mut intent_digest))
        .map_err(|_| "cannot derive contribution receipt")?;
    Ok(ContributionAdmission {
        definition_id: request.definition_id.as_str().to_owned(),
        receipt_id,
        intent_digest,
    })
}
