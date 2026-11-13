//! Retain the current cloud winner only while an optimistic edit can obscure it.

use super::{MutationManager, ReplayStamp};
use crate::schema::types::{cas::CasExpectation, KeyValue, Mutation, MutationType};
use crate::schema::SchemaError;
use crate::storage::format::records::RecordAddress;
use lastdb_model::record_replacement::RecordTarget;
use lastdb_model::record_value::StoredRecord;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayBaseline<T> {
    stamp: ReplayStamp,
    record: Option<T>,
}

impl MutationManager {
    pub(super) async fn stage_replay_baselines(
        &self,
        addresses: &[RecordAddress],
        preimages: &[Option<Arc<StoredRecord>>],
    ) -> Result<(), SchemaError> {
        let addresses: Vec<_> = addresses.iter().map(RecordAddress::as_str).collect();
        let metadata = self.db_ops.metadata();
        let previous = metadata.replay_baseline_sources(&addresses).await?;
        let mut baselines = Vec::new();
        for ((address, preimage), stamp) in addresses.into_iter().zip(preimages).zip(previous) {
            if let Some(stamp) = stamp {
                let record = preimage.as_deref().filter(|value| value.is_live());
                let bytes = serde_json::to_vec(&ReplayBaseline { stamp, record })
                    .map_err(invalid_baseline)?;
                baselines.push((address, bytes));
            }
        }
        metadata.stage_replay_baselines(baselines).await
    }

    /// Restore the durable preimage when the cloud kept the winner this local edit replaced.
    pub async fn apply_replay_baseline(
        &self,
        schema: &str,
        key: &KeyValue,
        winner: &ReplayStamp,
    ) -> Result<bool, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        let metadata = self.db_ops.metadata();
        let Some(bytes) = metadata.replay_baseline(&address).await? else {
            return Ok(false);
        };
        let baseline: ReplayBaseline<StoredRecord> =
            serde_json::from_slice(&bytes).map_err(invalid_baseline)?;
        if baseline.stamp != *winner || metadata.pending_replay_attachment(&address).await? {
            return Ok(false);
        }
        let before = self.replay_preimage(address).await?;
        let before = before.as_deref().filter(|record| record.is_live());
        let saved = baseline.record.as_ref();
        if before.and_then(|record| record.file.as_ref())
            != saved.and_then(|record| record.file.as_ref())
            || before.and_then(|record| record.inline_file.as_ref())
                != saved.and_then(|record| record.inline_file.as_ref())
        {
            return Ok(false);
        }
        if let Some(record) = saved {
            if !self.replay_record_key_matches(schema, key, &record.key)? {
                return Err(invalid_baseline("stored record key differs"));
            }
        }
        self.apply_replay_record(as_mutation(schema, key.clone(), baseline.record))
            .await?;
        Ok(true)
    }
}

fn as_mutation(schema: &str, key: KeyValue, record: Option<StoredRecord>) -> Mutation {
    let mut target = RecordTarget {
        schema: schema.to_owned(),
        key,
        alternates: BTreeMap::new(),
    };
    let kind = if record.is_some() {
        MutationType::Create
    } else {
        MutationType::Delete
    };
    let mut fields = HashMap::new();
    let mut meta = None;
    let mut definition_id = None;
    if let Some(record) = record {
        target.alternates = record.alternates;
        fields = record.fields.into_iter().collect();
        meta = record.meta;
        definition_id = record.definition_id.map(|id| id.as_str().to_owned());
    }
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields,
        String::new(),
        kind,
        CasExpectation::Absent,
    );
    mutation.definition_id = definition_id;
    if let Some(meta) = meta {
        mutation.source_file_name = meta.source_file_name;
        mutation.metadata = Some(meta.metadata.into_iter().collect());
    }
    mutation
}

fn invalid_baseline(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("invalid replay baseline: {error}"))
}
