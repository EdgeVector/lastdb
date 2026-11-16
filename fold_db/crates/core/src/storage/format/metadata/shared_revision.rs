//! Accepted mutation identity of each live record in a whole-database share.

use super::{read_err, Metadata};
use crate::schema::SchemaError;
use lastdb_model::hex::sha256_hex;
use lastdb_model::portable_mutation::Stamp;

type SharedRecordWrite<'a> = (&'a str, Option<(&'a Stamp, &'a str)>);

pub(crate) struct SharedCommentWrite<'a> {
    pub address: &'a str,
    pub before: Option<&'a Stamp>,
    pub after: Option<&'a Stamp>,
    pub creator: Option<&'a str>,
}

fn key(prefix: &str, address: &str) -> String {
    format!("{prefix}:{}", sha256_hex(address))
}

fn at_revision(bytes: &[u8], revision: &str) -> Result<Option<String>, SchemaError> {
    let marker = std::str::from_utf8(bytes).map_err(read_err)?;
    let (saved_revision, value) = marker
        .split_once(':')
        .ok_or_else(|| read_err("invalid shared record marker"))?;
    Ok((saved_revision == revision).then(|| value.to_owned()))
}

impl Metadata {
    pub(crate) async fn shared_record_mutation(
        &self,
        address: &str,
        local_revision: &str,
    ) -> Result<Option<String>, SchemaError> {
        let Some(bytes) = self
            .store
            .get(key("shared-record-mutation", address).as_bytes())
            .await
            .map_err(read_err)?
        else {
            return Ok(None);
        };
        at_revision(&bytes, local_revision)
    }

    pub(crate) async fn stage_shared_record_mutations<'a>(
        &self,
        records: impl Iterator<Item = SharedRecordWrite<'a>>,
    ) -> Result<(), SchemaError> {
        self.stage(
            records
                .map(|(address, mutation)| {
                    (
                        key("shared-record-mutation", address),
                        mutation
                            .map(|(stamp, id)| format!("{}:{id}", stamp.revision()).into_bytes()),
                    )
                })
                .collect(),
        )
        .await
        .map_err(read_err)
    }

    pub(crate) async fn shared_comment_creator(
        &self,
        address: &str,
        revision: &str,
    ) -> Result<Option<String>, SchemaError> {
        let Some(bytes) = self
            .store
            .get(key("shared-comment-creator", address).as_bytes())
            .await
            .map_err(read_err)?
        else {
            return Ok(None);
        };
        at_revision(&bytes, revision)
    }

    /// Carry forward verified origin with the new revision in the record's transaction.
    /// A missing marker can never be supplied by ordinary record fields or metadata.
    pub(crate) async fn stage_shared_comment_creators<'a>(
        &self,
        writes: impl Iterator<Item = SharedCommentWrite<'a>>,
    ) -> Result<(), SchemaError> {
        let entries: Vec<_> = writes
            .map(|write| (key("shared-comment-creator", write.address), write))
            .collect();
        let previous = self
            .store
            .get_many(
                entries
                    .iter()
                    .map(|(key, _)| key.as_bytes().to_vec())
                    .collect(),
            )
            .await
            .map_err(read_err)?;
        if previous.len() != entries.len() {
            return Err(read_err("incomplete shared comment creator lookup"));
        }
        let mut changes = Vec::with_capacity(entries.len());
        for ((key, write), previous) in entries.into_iter().zip(previous) {
            let creator = match (write.before, previous.as_deref()) {
                (Some(before), Some(bytes)) => at_revision(bytes, &before.revision())?,
                _ => write.creator.map(str::to_owned),
            };
            let next = write
                .after
                .zip(creator)
                .map(|(stamp, creator)| format!("{}:{creator}", stamp.revision()).into_bytes());
            if next.is_some() || previous.is_some() {
                changes.push((key, next));
            }
        }
        self.stage(changes).await.map_err(read_err)
    }
}
