use super::{
    BackupTransport, BackupTransportError, ByteBackupClient, GuestAppendPermission, Result,
};
use crate::profile::endpoints::exemem_api_url_for;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_protocol::v2::contributions::{
    ContributionEntry, ReadAudience, SetReadAudience, MAX_CONTRIBUTION_CIPHERTEXT_BYTES,
    MAX_CONTRIBUTION_SEQUENCE, MAX_PENDING_CONTRIBUTIONS,
};
use cloud_backup_protocol::v2::{limits, Stream};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct Append<'a> {
    stream: &'a Stream,
    sequence: u64,
    ciphertext: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Appended {
    sequence: u64,
}

#[derive(Serialize)]
struct Read<'a> {
    stream: &'a Stream,
    after: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entries {
    entries: Vec<ContributionEntry>,
}

#[derive(Serialize)]
struct Ack<'a> {
    stream: &'a Stream,
    through: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Acknowledged {
    through: u64,
}

impl ByteBackupClient {
    /// Confirm the owner-set audience for this exact stream.
    pub async fn set_read_audience(
        &self,
        stream: &Stream,
        read_audience: ReadAudience,
    ) -> Result<()> {
        stream.validate()?;
        let request = SetReadAudience {
            stream: stream.clone(),
            read_audience,
        };
        let response: SetReadAudience = self.post("streams/read-audience/set", &request).await?;
        if response != request {
            return Err(BackupTransportError::InvalidResponse(
                "stream read audience",
            ));
        }
        Ok(())
    }

    /// Read a bounded page of pending contributions as the scope owner.
    pub async fn read_contributions(
        &self,
        stream: &Stream,
        after: u64,
    ) -> Result<Vec<ContributionEntry>> {
        stream.validate()?;
        if after > MAX_CONTRIBUTION_SEQUENCE {
            return Err(BackupTransportError::Configuration("contribution cursor"));
        }
        let response: Entries = self
            .post("contributions/read", &Read { stream, after })
            .await?;
        if response.entries.len() > MAX_PENDING_CONTRIBUTIONS
            || response.entries.iter().enumerate().any(|(offset, entry)| {
                entry.sequence != after + offset as u64 + 1
                    || entry.sequence > MAX_CONTRIBUTION_SEQUENCE
                    || !valid_ciphertext(&entry.ciphertext)
            })
        {
            return Err(BackupTransportError::InvalidResponse(
                "contribution entries",
            ));
        }
        Ok(response.entries)
    }

    /// Acknowledge contributions only after durably recording their outcomes.
    pub async fn ack_contributions(&self, stream: &Stream, through: u64) -> Result<()> {
        stream.validate()?;
        if !(1..=MAX_CONTRIBUTION_SEQUENCE).contains(&through) {
            return Err(BackupTransportError::Configuration("contribution cursor"));
        }
        let response: Acknowledged = self
            .post("contributions/ack", &Ack { stream, through })
            .await?;
        if response.through != through {
            return Err(BackupTransportError::InvalidResponse(
                "contribution acknowledgement",
            ));
        }
        Ok(())
    }
}

/// Append encrypted contributions to the single stream named by a guest permission.
pub struct GuestContributionWriter {
    transport: BackupTransport,
    stream: Stream,
    header: HeaderValue,
}

impl GuestContributionWriter {
    /// Open the pinned environment with the guest credential and no account session.
    pub fn new(permission: &GuestAppendPermission) -> Result<Self> {
        let endpoint = exemem_api_url_for(permission.environment().registry());
        let mut header = HeaderValue::from_str(permission.authorization())
            .map_err(|_| BackupTransportError::Configuration("guest permission header"))?;
        header.set_sensitive(true);
        Ok(Self {
            transport: BackupTransport::for_guest(endpoint)?,
            stream: permission.stream().clone(),
            header,
        })
    }

    /// Upload one encrypted entry. The response confirms transport acceptance only.
    pub async fn append(&self, sequence: u64, ciphertext: &str) -> Result<()> {
        if !(1..=MAX_CONTRIBUTION_SEQUENCE).contains(&sequence) || !valid_ciphertext(ciphertext) {
            return Err(BackupTransportError::Configuration("contribution entry"));
        }
        let response: Appended = self
            .transport
            .post_bounded_with_headers(
                self.transport
                    .api_endpoint("cloud/backups/v2/contributions/append"),
                &Append {
                    stream: &self.stream,
                    sequence,
                    ciphertext,
                },
                limits::CONTROL_JSON_MAX_BYTES,
                HeaderMap::from_iter([(AUTHORIZATION, self.header.clone())]),
            )
            .await?;
        if response.sequence != sequence {
            return Err(BackupTransportError::InvalidResponse(
                "contribution sequence",
            ));
        }
        Ok(())
    }
}

fn valid_ciphertext(ciphertext: &str) -> bool {
    !ciphertext.is_empty()
        && ciphertext.len() <= MAX_CONTRIBUTION_CIPHERTEXT_BYTES
        && URL_SAFE_NO_PAD.decode(ciphertext).is_ok()
}
