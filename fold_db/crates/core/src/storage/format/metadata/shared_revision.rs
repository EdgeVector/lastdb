//! Accepted mutation identity of each live record in a whole-database share.

use super::{read_err, Metadata};
use crate::schema::SchemaError;
use lastdb_model::hex::hex_lower;
use lastdb_model::portable_mutation::Stamp;
use sha2::{Digest, Sha256};

type SharedRecordWrite<'a> = (&'a str, Option<(&'a Stamp, &'a str)>);

fn key(address: &str) -> String {
    format!(
        "shared-record-mutation:{}",
        hex_lower(Sha256::digest(address))
    )
}

impl Metadata {
    pub(crate) async fn shared_record_mutation(
        &self,
        address: &str,
        local_revision: &str,
    ) -> Result<Option<String>, SchemaError> {
        let Some(bytes) = self
            .store
            .get(key(address).as_bytes())
            .await
            .map_err(read_err)?
        else {
            return Ok(None);
        };
        let marker = String::from_utf8(bytes).map_err(read_err)?;
        let (saved_revision, id) = marker
            .split_once(':')
            .ok_or_else(|| read_err("invalid shared record mutation marker"))?;
        Ok((saved_revision == local_revision).then(|| id.to_owned()))
    }

    pub(crate) async fn stage_shared_record_mutations<'a>(
        &self,
        records: impl Iterator<Item = SharedRecordWrite<'a>>,
    ) -> Result<(), SchemaError> {
        self.stage(
            records
                .map(|(address, mutation)| {
                    (
                        key(address),
                        mutation
                            .map(|(stamp, id)| format!("{}:{id}", stamp.revision()).into_bytes()),
                    )
                })
                .collect(),
        )
        .await
        .map_err(read_err)
    }
}
