//! Field history wiring: the activation signal and wrappers.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use super::super::atom_history::{AtomHistory, AtomHistoryPage, FieldHistoryPolicy, HistoryMode};
use super::super::conflict_record::Recorded;
use super::super::counter::RecordRefs;
use super::super::ports::HistorySignals;
use super::super::time_queue::QueueReader;
use super::super::types::PerKeyRecord;
use super::super::AtomStore;
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};

/// The rows and the deletes of the history key counts.
type CountRows = (Vec<(String, Value)>, Vec<Vec<u8>>);

impl HistorySignals for AtomStore {
    fn history_activation(&self) -> &tokio::sync::Notify {
        &self.history_activation
    }
}

impl AtomStore {
    pub(crate) fn history_activation(&self) -> &tokio::sync::Notify {
        &self.history_activation
    }

    pub(crate) async fn prepare_atom_history(
        &self,
        mode: HistoryMode,
        scope: Option<&str>,
        tips: &BTreeMap<String, Option<PerKeyRecord>>,
        old: &HashMap<String, PerKeyRecord>,
    ) -> Result<Recorded, SchemaError> {
        AtomHistory::new(self)
            .prepare_atom_history(mode, scope, tips, old)
            .await
    }

    pub(crate) async fn history_key_rows(
        &self,
        records: &[RecordRefs],
        new_records: &[bool],
        released: &[String],
    ) -> Result<CountRows, SchemaError> {
        AtomHistory::new(self)
            .history_key_rows(records, new_records, released)
            .await
    }

    pub(crate) async fn atom_history_presence(
        &self,
        slots: &[String],
    ) -> Result<Vec<bool>, SchemaError> {
        AtomHistory::new(self).atom_history_presence(slots).await
    }

    pub(crate) async fn atom_history_enabled(&self) -> Result<bool, SchemaError> {
        AtomHistory::new(self).atom_history_enabled().await
    }

    pub(crate) async fn bind_history_schema(&self, schema: &Schema) -> Result<(), SchemaError> {
        AtomHistory::new(self).bind_history_schema(schema).await
    }

    pub(crate) async fn bind_history_schemas<'a>(
        &self,
        schemas: impl Iterator<Item = &'a Schema> + Send,
    ) -> Result<(), SchemaError> {
        AtomHistory::new(self).bind_history_schemas(schemas).await
    }

    pub async fn set_field_history_policy(
        &self,
        field: &str,
        policy: FieldHistoryPolicy,
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        AtomHistory::new(self)
            .set_field_history_policy(field, policy, scope)
            .await
    }

    pub(crate) async fn atom_history_expire_step(
        &self,
        reader: &mut QueueReader,
    ) -> Result<u64, SchemaError> {
        AtomHistory::new(self)
            .atom_history_expire_step(reader)
            .await
    }

    pub async fn field_history_policy(
        &self,
        field: &str,
        scope: Option<&str>,
    ) -> Result<FieldHistoryPolicy, SchemaError> {
        AtomHistory::new(self)
            .field_history_policy(field, scope)
            .await
    }

    pub async fn purge_atom_history(
        &self,
        slot: &str,
        cursor: Option<&str>,
    ) -> Result<Value, SchemaError> {
        AtomHistory::new(self)
            .purge_atom_history(slot, cursor)
            .await
    }

    pub async fn atom_history_slot(
        &self,
        molecule: &str,
        key: &KeyValue,
        scope: Option<&str>,
    ) -> Result<String, SchemaError> {
        AtomHistory::new(self)
            .atom_history_slot(molecule, key, scope)
            .await
    }

    pub async fn atom_history_page(
        &self,
        slot: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<AtomHistoryPage, SchemaError> {
        AtomHistory::new(self)
            .atom_history_page(slot, cursor, limit)
            .await
    }

    pub async fn atom_history_value(
        &self,
        slot: &str,
        id: &str,
    ) -> Result<Option<Value>, SchemaError> {
        AtomHistory::new(self).atom_history_value(slot, id).await
    }
}
