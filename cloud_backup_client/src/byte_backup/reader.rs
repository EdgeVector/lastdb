use super::*;
use cloud_backup_protocol::v2::access::{ReadGrant, ReadGrantRequest, ReadGrantResponse};
use cloud_backup_protocol::v2::publication::{CommitResponse, CommitStatusRequest};
use cloud_backup_protocol::v2::{AcceptedBackup, Job};
use cloud_backup_protocol::{Digest, Id};
use reqwest::header::{HeaderMap, HeaderValue};

/// Read one accepted backup through owner or explicitly granted recipient access.
pub struct ByteBackupReader {
    transport: BackupTransport,
    recipient: Option<RecipientAccess>,
}

struct RecipientAccess {
    header: HeaderValue,
    job: Job,
    commit_sha256: Digest,
}

impl ByteBackupClient {
    pub fn reader(&self) -> ByteBackupReader {
        ByteBackupReader {
            transport: self.transport.clone(),
            recipient: None,
        }
    }
}

impl ByteBackupReader {
    /// Use the recipient's own credentials and a grant bound to this exact accepted point.
    pub fn recipient(
        auth: AuthClient,
        grant_id: &Id,
        job: Job,
        commit_sha256: Digest,
    ) -> Result<Self> {
        job.validate()?;
        let id = grant_id.as_str();
        // Id already enforces canonical UUID syntax. A grant requires UUID v4.
        let bytes = id.as_bytes();
        if bytes.get(14) != Some(&b'4') || !matches!(bytes.get(19), Some(b'8' | b'9' | b'a' | b'b'))
        {
            return Err(BackupTransportError::Configuration("recipient grant id"));
        }
        let mut header = HeaderValue::from_str(id)
            .map_err(|_| BackupTransportError::Configuration("recipient grant id"))?;
        header.set_sensitive(true);
        Ok(Self {
            transport: BackupTransport::from_auth(auth)?,
            recipient: Some(RecipientAccess {
                header,
                job,
                commit_sha256,
            }),
        })
    }

    pub fn recipient_job(&self) -> Option<&Job> {
        self.recipient.as_ref().map(|access| &access.job)
    }

    pub fn canonical_endpoint(&self) -> String {
        self.transport.endpoint("cloud/backups/v2/")
    }

    pub async fn commit_status(&self, request: &CommitStatusRequest) -> Result<CommitResponse> {
        request.validate()?;
        let operation = if let Some(access) = &self.recipient {
            access.validate_job(&request.job)?;
            "recipients/accepted/read"
        } else {
            "commits/status"
        };
        let response: CommitResponse = self.post(operation, request).await?;
        response.validate_for_status(request)?;
        if let Some(access) = &self.recipient {
            let CommitResponse::Accepted { backup } = &response else {
                return Err(BackupTransportError::InvalidResponse(
                    "recipient accepted point",
                ));
            };
            if backup.commit_sha256 != access.commit_sha256 {
                return Err(BackupTransportError::Integrity("recipient accepted point"));
            }
        }
        Ok(response)
    }

    pub async fn read_grants(
        &self,
        request: &ReadGrantRequest,
        accepted: &AcceptedBackup,
    ) -> Result<ReadGrantResponse> {
        request.validate()?;
        accepted.validate()?;
        if &request.job != accepted.job() {
            return Err(BackupTransportError::Configuration("accepted selection"));
        }
        if let Some(access) = &self.recipient {
            access.validate_job(&request.job)?;
            if accepted.commit_sha256 != access.commit_sha256 {
                return Err(BackupTransportError::Configuration(
                    "recipient accepted point",
                ));
            }
        }
        let response: ReadGrantResponse = self.post("objects/read", request).await?;
        response.validate_for(request, accepted)?;
        Ok(response)
    }

    /// Provider requests carry only the existing temporary URL.
    pub async fn open(&self, grant: &ReadGrant) -> Result<ObjectStream> {
        grant.validate()?;
        self.transport
            .open(
                StoredObject {
                    sha256: &grant.sha256,
                    bytes: grant.bytes.get(),
                },
                TemporaryGrant {
                    url: grant.url.as_str(),
                    expires_ms: grant.expires_ms.get(),
                },
            )
            .await
    }

    async fn post<Q: Serialize, R: DeserializeOwned>(
        &self,
        operation: &str,
        request: &Q,
    ) -> Result<R> {
        let path = format!("cloud/backups/v2/{operation}");
        if let Some(access) = &self.recipient {
            let mut headers = HeaderMap::new();
            headers.insert("x-lastdb-recipient-grant", access.header.clone());
            self.transport
                .post_bounded_with_headers(
                    self.transport.api_endpoint(&path),
                    request,
                    limits::CONTROL_JSON_MAX_BYTES,
                    headers,
                )
                .await
        } else {
            self.transport
                .post(&path, request, limits::CONTROL_JSON_MAX_BYTES)
                .await
        }
    }
}

impl RecipientAccess {
    fn validate_job(&self, job: &Job) -> Result<()> {
        if job != &self.job {
            return Err(BackupTransportError::Configuration(
                "recipient accepted job",
            ));
        }
        Ok(())
    }
}
