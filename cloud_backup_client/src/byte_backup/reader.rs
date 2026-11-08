use super::*;
use cloud_backup_protocol::v2::access::{ReadGrant, ReadGrantRequest, ReadGrantResponse};
use cloud_backup_protocol::v2::publication::{CommitResponse, CommitStatusRequest};
use cloud_backup_protocol::v2::{AcceptedBackup, Job};
use cloud_backup_protocol::{Digest, Id};
use reqwest::header::{HeaderMap, HeaderValue};

mod guest;

/// Read accepted backups with owner, named recipient, or guest authority.
pub struct ByteBackupReader {
    transport: BackupTransport,
    proof: ReadProof,
}

enum ReadProof {
    Owner,
    Recipient(RecipientAccess),
    Guest(guest::GuestAccess),
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
            proof: ReadProof::Owner,
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
            proof: ReadProof::Recipient(RecipientAccess {
                header,
                job,
                commit_sha256,
            }),
        })
    }

    pub fn recipient_job(&self) -> Option<&Job> {
        if let ReadProof::Recipient(access) = &self.proof {
            Some(&access.job)
        } else {
            None
        }
    }

    pub fn canonical_endpoint(&self) -> String {
        self.transport.endpoint("cloud/backups/v2/")
    }

    pub async fn commit_status(&self, request: &CommitStatusRequest) -> Result<CommitResponse> {
        request.validate()?;
        if let ReadProof::Guest(access) = &self.proof {
            return self.guest_commit_status(access, request).await;
        }
        let operation = if let ReadProof::Recipient(access) = &self.proof {
            access.validate_job(&request.job)?;
            "recipients/accepted/read"
        } else {
            "commits/status"
        };
        let response: CommitResponse = self.post(operation, request).await?;
        response.validate_for_status(request)?;
        if let ReadProof::Recipient(access) = &self.proof {
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
        let operation = match &self.proof {
            ReadProof::Owner => "objects/read",
            ReadProof::Recipient(access) => {
                access.validate_job(&request.job)?;
                if accepted.commit_sha256 != access.commit_sha256 {
                    return Err(BackupTransportError::Configuration(
                        "recipient accepted point",
                    ));
                }
                "objects/read"
            }
            ReadProof::Guest(access) => {
                access.validate_stream(&request.job.stream)?;
                guest::require_independent(accepted)?;
                "guest/objects/read"
            }
        };
        let response: ReadGrantResponse = self.post(operation, request).await?;
        response.validate_for(request, accepted)?;
        if let ReadProof::Guest(access) = &self.proof {
            access.validate_grant_expiry(&response)?;
        }
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
        let mut headers = HeaderMap::new();
        match &self.proof {
            ReadProof::Owner => {}
            ReadProof::Recipient(access) => {
                headers.insert("x-lastdb-recipient-grant", access.header.clone());
            }
            ReadProof::Guest(access) => {
                headers.insert(reqwest::header::AUTHORIZATION, access.header.clone());
            }
        }
        self.transport
            .post_bounded_with_headers(
                self.transport.api_endpoint(&path),
                request,
                limits::CONTROL_JSON_MAX_BYTES,
                headers,
            )
            .await
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
