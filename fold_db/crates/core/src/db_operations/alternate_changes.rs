//! The alternate change fence that schema removal and refresh check.

use super::DbOperations;
use crate::records::alternate_change::{job_key, Change, Phase, NAMESPACE};
use crate::schema::SchemaError;

pub(crate) async fn require_finished(db: &DbOperations, schema: &str) -> Result<(), SchemaError> {
    let store = db.open_namespace(NAMESPACE).await?;
    if let Some(bytes) = store.get(job_key(schema).as_bytes()).await? {
        let change: Change = serde_json::from_slice(&bytes)
            .map_err(|_| SchemaError::InvalidData("alternate change is unreadable".into()))?;
        if !matches!(change.phase, Phase::Complete | Phase::Aborted) {
            return Err(SchemaError::InvalidData(
                "finish or abort the alternate change before schema removal or refresh".into(),
            ));
        }
    }
    Ok(())
}
