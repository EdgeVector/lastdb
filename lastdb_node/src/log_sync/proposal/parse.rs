//! Parse, validate, and canonicalize member proposal documents.

use std::collections::{BTreeMap, HashMap};

use cloud_backup_protocol::Id;
use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::KeyValue;
use serde_json::Value;

use super::ContributionError;
use crate::host::Host;
use crate::registered_share::{self, RegisteredShare};

const BODY_MAX: usize = 64 * 1024;
const NAMESPACE_MAX: usize = 128;

pub(super) struct Proposed {
    pub(super) namespace: String,
    hash: String,
    range: Option<String>,
    pub(super) record_key: String,
    pub(super) mutation_type: String,
    pub(super) expected: Value,
    pub(super) record: Option<Value>,
    pub(super) alternates: signed_history_materialize::Alternates,
}

/// Map the owner submission document onto the history record convention.
pub(super) fn parse(body: &[u8]) -> Result<Proposed, ContributionError> {
    let Ok(Value::Object(doc)) = serde_json::from_slice::<Value>(body) else {
        return Err(ContributionError::Invalid);
    };
    let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_owned);
    if text(doc.get("type")).as_deref() != Some("mutation") {
        return Err(ContributionError::Invalid);
    }
    let namespace = text(doc.get("schema")).ok_or(ContributionError::Invalid)?;
    let key = doc.get("key_value").and_then(Value::as_object);
    let hash = text(key.and_then(|key| key.get("hash"))).ok_or(ContributionError::Invalid)?;
    let range = match key.and_then(|key| key.get("range")) {
        None | Some(Value::Null) => None,
        Some(range) => Some(text(Some(range)).ok_or(ContributionError::Invalid)?),
    };
    let record_key =
        mutation_log::join_record_key(&hash, range.as_deref()).ok_or(ContributionError::Invalid)?;
    let carries_file = doc.get("file").is_some_and(|file| !file.is_null());
    let mutation_type = text(doc.get("mutation_type")).ok_or(ContributionError::Invalid)?;
    if namespace.is_empty()
        || namespace.len() > NAMESPACE_MAX
        || record_key.is_empty()
        || record_key.len() > usize::from(u16::MAX)
        || carries_file
        || !matches!(mutation_type.as_str(), "create" | "replace" | "delete")
    {
        return Err(ContributionError::Invalid);
    }
    let expected = doc
        .get("expected")
        .ok_or(ContributionError::Invalid)?
        .clone();
    validate_expected(&mutation_type, &expected)?;
    let (record, alternates) = if mutation_type == "delete" {
        if doc
            .get("alternates")
            .is_some_and(|v| !v.is_null() && v.as_object().is_none_or(|m| !m.is_empty()))
        {
            return Err(ContributionError::Invalid);
        }
        (None, BTreeMap::new())
    } else {
        let Some(record @ Value::Object(_)) = doc.get("record") else {
            return Err(ContributionError::Invalid);
        };
        let alts = match doc.get("alternates") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(val) => {
                serde_json::from_value(val.clone()).map_err(|_| ContributionError::Invalid)?
            }
        };
        (Some(record.clone()), alts)
    };
    Ok(Proposed {
        namespace,
        hash,
        range,
        record_key,
        mutation_type,
        expected,
        record,
        alternates,
    })
}

fn validate_expected(mutation_type: &str, expected: &Value) -> Result<(), ContributionError> {
    match (mutation_type, expected.get("type").and_then(Value::as_str)) {
        ("create", Some("absent")) => Ok(()),
        ("replace" | "delete", Some("revision"))
            if expected.get("revision").and_then(Value::as_str).is_some() =>
        {
            Ok(())
        }
        _ => Err(ContributionError::Invalid),
    }
}

pub(super) async fn canonicalize_proposed(
    host: &Host,
    share: &RegisteredShare,
    proposed: &mut Proposed,
) -> Result<(), ContributionError> {
    let id =
        Id::try_from(share.id.as_str().to_owned()).map_err(|_| ContributionError::Unavailable)?;
    let store = registered_share::receiver_store(host, &id)
        .await
        .map_err(|_| ContributionError::Unavailable)?;
    let declared = store
        .schema_manager()
        .get_schema_metadata(&proposed.namespace)
        .map_err(|_| ContributionError::Unavailable)?
        .is_some();
    if !declared {
        let (has_history_schemas, in_history) = super::super::histories_for(host)
            .peek_with(share.id.as_str(), |history| {
                (
                    !history.schemas().is_empty(),
                    history.schemas().contains_key(&proposed.namespace),
                )
            })
            .unwrap_or((false, false));
        if in_history {
            return check_proposed_size(None, proposed);
        }
        let has_store_schemas = !store
            .schema_manager()
            .get_schemas()
            .map_err(|_| ContributionError::Unavailable)?
            .is_empty();
        if has_history_schemas || has_store_schemas {
            return Err(ContributionError::Invalid);
        }
        return check_proposed_size(None, proposed);
    }
    proposed.record_key = signed_history_materialize::canonical_record_key(
        &store,
        &proposed.namespace,
        &proposed.hash,
        proposed.range.as_deref(),
    )
    .map_err(|_| ContributionError::Invalid)?;
    if proposed.mutation_type != "delete" {
        proposed.alternates = signed_history_materialize::resolve_alternates(
            &store,
            &proposed.namespace,
            std::mem::take(&mut proposed.alternates),
        )
        .map_err(|_| ContributionError::Invalid)?;
    }
    check_proposed_size(Some(&store), proposed)
}

fn check_proposed_size(
    store: Option<&FoldDB>,
    proposed: &Proposed,
) -> Result<(), ContributionError> {
    if proposed.mutation_type == "delete" {
        return Ok(());
    }
    let Some(Value::Object(map)) = &proposed.record else {
        return Err(ContributionError::Invalid);
    };
    let key = if let Some(store) = store {
        signed_history_materialize::key_value(store, &proposed.namespace, &proposed.record_key)
            .map_err(|_| ContributionError::Invalid)?
    } else {
        KeyValue::new(Some(proposed.hash.clone()), proposed.range.clone())
    };
    let target = RecordTarget {
        schema: proposed.namespace.clone(),
        key,
        alternates: proposed.alternates.clone(),
    };
    let fields: HashMap<String, Value> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    signed_history_materialize::check_shared_record_size(&target, &fields)
        .map_err(|_| ContributionError::Invalid)
}

/// The publisher decision envelope that goes into the signed proposal.
pub(super) fn envelope(proposed: &Proposed, expected: Value) -> Result<Vec<u8>, ContributionError> {
    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "mutation_type".into(),
        Value::String(proposed.mutation_type.clone()),
    );
    envelope.insert("expected".into(), expected);
    if let Some(record) = &proposed.record {
        envelope.insert("record".into(), record.clone());
    }
    if !proposed.alternates.is_empty() {
        let alts =
            serde_json::to_value(&proposed.alternates).map_err(|_| ContributionError::Invalid)?;
        envelope.insert("alternates".into(), alts);
    }
    let value =
        serde_json::to_vec(&Value::Object(envelope)).map_err(|_| ContributionError::Invalid)?;
    if value.len() > BODY_MAX {
        return Err(ContributionError::Invalid);
    }
    Ok(value)
}
