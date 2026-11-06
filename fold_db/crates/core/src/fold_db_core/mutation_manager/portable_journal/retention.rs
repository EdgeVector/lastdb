//! Bind a completed mutation's file marker to its exact local record address.

use super::{invalid, MutationManager};
use crate::schema::SchemaError;
use lastdb_model::portable_mutation::{MutationEntry, MutationOperation, SchemaIdentity};
use std::collections::{BTreeMap, BTreeSet};

impl MutationManager {
    pub(super) async fn release_uploaded_files(
        &self,
        retired: &[MutationEntry],
    ) -> Result<(), SchemaError> {
        let Some(last) = retired.last() else {
            return Ok(());
        };
        let files: Vec<_> = retired
            .iter()
            .filter_map(|entry| match &entry.operation {
                MutationOperation::ReplaceRecord { primary_key, data } => data
                    .file
                    .as_ref()
                    .map(|file| (&entry.schema, primary_key, &file.version)),
                MutationOperation::DeleteRecord { .. } => None,
            })
            .collect();
        if files.is_empty() {
            return Ok(());
        }
        let bindings = self.db_ops.metadata().portable_bound_schemas().await?;
        let ids = files
            .iter()
            .map(|(schema, _, _)| match schema {
                SchemaIdentity::Shared(id) => Ok(id),
                SchemaIdentity::Local(_) => Err(invalid("local mutation in the shared log")),
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        let bindings = &bindings;
        let schemas = futures::future::try_join_all(ids.into_iter().map(|id| async move {
            let name = bindings
                .get(id)
                .ok_or_else(|| invalid("completed mutation has no local schema"))?;
            Ok::<_, SchemaError>((id, self.load_schema_for_delta(name).await?))
        }))
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let records = self.db_ops.records();
        let addresses = files
            .into_iter()
            .map(|(schema, key, version)| {
                let SchemaIdentity::Shared(id) = schema else {
                    return Err(invalid("local file reference in the shared log"));
                };
                Ok((
                    records.record_address(&schemas[id], &key.clone().into())?,
                    version.clone(),
                ))
            })
            .collect::<Result<_, SchemaError>>()?;
        records
            .stage_completed_file_references(addresses, last.stamp.position.sequence)
            .await
    }
}
