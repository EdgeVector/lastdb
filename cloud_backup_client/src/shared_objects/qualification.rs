//! Bounded DEV operations that intentionally separate a B2 part and its settlement.

use super::*;

impl SharedObjectClient {
    /// Send one B2 snapshot part without settling its multipart upload.
    #[doc(hidden)]
    pub async fn stage_snapshot_part(
        &self,
        request: &SharedObjectRequest,
        source: Box<dyn Read + Send>,
        deadline_ms: u64,
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<()> {
        require_snapshot_part(request)?;
        let prepare = self
            .post_transfer(ObjectAuthorityRequest::Prepare(request), deadline_ms, sign)
            .await?;
        let SharedObjectPrepare::Upload { grant } = prepare else {
            return Err(BackupTransportError::Integrity("new DEV B2 part"));
        };
        self.put_grant(request, source, deadline_ms, grant).await
    }

    /// Settle the exact B2 part using the normal snapshot completion path.
    #[doc(hidden)]
    pub async fn complete_snapshot_part(
        &self,
        request: &SharedObjectRequest,
        deadline_ms: u64,
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<SealedObject> {
        require_snapshot_part(request)?;
        self.complete(request, deadline_ms, sign).await
    }
}

fn require_snapshot_part(request: &SharedObjectRequest) -> Result<()> {
    if destination(request) != Some(Destination::B2) {
        return Err(BackupTransportError::Configuration(
            "shared B2 snapshot part",
        ));
    }
    Ok(())
}
