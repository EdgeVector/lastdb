use super::{Result, SharedObjectUploader};
use cloud_backup_client::shared_objects::SharedObjectClient;
use cloud_backup_protocol::shared::objects::{SharedObjectRequest, SharedObjectRole};
use cloud_backup_protocol::shared::{FileDependency, LogBatch, WriterStream};
use fold_db::security::Ed25519KeyPair;
use std::io::Cursor;

/// Exact provider upload and verification through the authenticated service.
pub struct ServiceObjects<'a> {
    pub client: &'a SharedObjectClient,
    pub device_signer: &'a Ed25519KeyPair,
    pub deadline_ms: u64,
}

#[async_trait::async_trait]
impl SharedObjectUploader for ServiceObjects<'_> {
    async fn put_file(
        &self,
        stream: &WriterStream,
        file: &FileDependency,
        sealed: Vec<u8>,
    ) -> Result<()> {
        self.upload(
            &SharedObjectRequest {
                database_id: stream.database_id.clone(),
                object: file.object.clone(),
                role: SharedObjectRole::File {
                    source_stream: file.source_stream.clone(),
                    sequence: file.sequence,
                },
            },
            sealed,
        )
        .await
    }

    async fn put_segment(&self, batch: &LogBatch, sealed: Vec<u8>) -> Result<()> {
        self.upload(
            &SharedObjectRequest {
                database_id: batch.stream.database_id.clone(),
                object: batch.segment.clone(),
                role: SharedObjectRole::Segment {
                    source_stream: batch.stream.source_stream.clone(),
                    after: batch.after.clone(),
                    through: batch.through,
                },
            },
            sealed,
        )
        .await
    }
}

impl ServiceObjects<'_> {
    async fn upload(&self, request: &SharedObjectRequest, sealed: Vec<u8>) -> Result<()> {
        self.client
            .upload_exact(
                request,
                Box::new(Cursor::new(sealed)),
                self.deadline_ms,
                &|message| Ok(self.device_signer.sign(message).to_bytes()),
            )
            .await?;
        Ok(())
    }
}
