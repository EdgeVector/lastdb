//! Publisher view of a shared database's members as one configuration.
//!
//! The service stores one signed entry per member. The owner API still reads
//! and writes the member set as a whole, so a read collects every entry and a
//! write signs only the entries that change.

use super::entries::{self, Grant};
use super::{exchange, lock, PublisherSession};
use cloud_backup_protocol::v2::share_access::{
    MemberEntry, ShareMemberRole, ShareMemberState, ShareWriteScope,
};
use cloud_backup_protocol::{DecimalU64, Digest, Id};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ConfigUpdate {
    pub expected_revision: DecimalU64,
    pub request_id: Id,
    members: Vec<ConfigMember>,
}

impl ConfigUpdate {
    pub(crate) fn scoped_members(&self) -> impl Iterator<Item = &ShareWriteScope> {
        self.members
            .iter()
            .filter_map(|member| member.write_schemas.as_ref())
    }

    pub(crate) fn has_legacy_writer(&self) -> bool {
        self.members.iter().any(|member| {
            member.role == ShareMemberRole::Submit
                && member.state == ShareMemberState::Active
                && member.write_schemas.is_none()
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigMember {
    pub node_key: Digest,
    pub role: ShareMemberRole,
    pub state: ShareMemberState,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub regrant: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_schemas: Option<ShareWriteScope>,
}

impl From<MemberEntry> for ConfigMember {
    fn from(entry: MemberEntry) -> Self {
        Self {
            node_key: entry.node_public_key,
            role: entry.role,
            state: entry.state,
            regrant: false,
            write_schemas: entry.write_schemas,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Configuration {
    database_id: String,
    revision: DecimalU64,
    members: Vec<ConfigMember>,
}

pub(crate) enum ConfigError {
    Invalid(String),
    Conflict(Option<DecimalU64>),
    PaymentRequired,
    Unconfirmed,
}

pub(super) fn unconfirmed_or_payment(error: &str) -> ConfigError {
    if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
        ConfigError::PaymentRequired
    } else {
        ConfigError::Unconfirmed
    }
}

pub(crate) async fn read_config(
    home: &Path,
    database_id: &str,
) -> Result<Configuration, ConfigError> {
    let _lock = lock::hold(home, database_id).map_err(|_| ConfigError::Unconfirmed)?;
    let session = open(home).await?;
    let (revision, stored) = exchange::list(&session, database_id)
        .await
        .map_err(|e| unconfirmed_or_payment(&e))?;
    Ok(Configuration {
        database_id: database_id.to_owned(),
        revision: DecimalU64::new(revision),
        members: stored.into_iter().map(Into::into).collect(),
    })
}

/// Make the stored members equal `request.members`.
///
/// Every stored member must be named: leaving one out is not a revoke. Only
/// the members whose grant changes get a new signed entry. When nothing
/// changes, the stored configuration is already the requested one and the
/// call succeeds at the current revision, which makes a repeat safe.
pub(crate) async fn set_config(
    home: &Path,
    database_id: &str,
    request: ConfigUpdate,
) -> Result<DecimalU64, ConfigError> {
    let mut members = request.members;
    members.sort_by(|left, right| left.node_key.cmp(&right.node_key));
    if members
        .windows(2)
        .any(|pair| pair[0].node_key == pair[1].node_key)
    {
        return Err(ConfigError::Invalid("share.members.order".into()));
    }
    for member in &mut members {
        match &mut member.write_schemas {
            Some(ShareWriteScope::Selected(ids)) => {
                ids.sort_unstable();
                ids.dedup();
            }
            Some(ShareWriteScope::Named { names }) => {
                names.sort_unstable();
                names.dedup();
            }
            _ => {}
        }
    }
    let _lock = lock::hold(home, database_id).map_err(|_| ConfigError::Unconfirmed)?;
    let session = open(home).await?;
    let (revision, stored) = exchange::list(&session, database_id)
        .await
        .map_err(|e| unconfirmed_or_payment(&e))?;
    if stored.iter().any(|entry| {
        members
            .binary_search_by(|member| member.node_key.cmp(&entry.node_public_key))
            .is_err()
    }) {
        return Err(ConfigError::Invalid("share.grant.omitted".into()));
    }
    let mut signed = Vec::new();
    for member in members {
        if member.node_key == session.publisher {
            return Err(ConfigError::Invalid("share.members.publisher".into()));
        }
        let current = stored
            .binary_search_by(|entry| entry.node_public_key.cmp(&member.node_key))
            .ok()
            .map(|index| &stored[index]);
        let desired = Grant {
            role: member.role,
            state: member.state,
            write_schemas: member.write_schemas,
        };
        let Some(grant) =
            entries::config_step(current, desired, member.regrant).map_err(ConfigError::Invalid)?
        else {
            continue;
        };
        signed.push(
            entries::sign(&session, database_id, &member.node_key, current, grant)
                .map_err(ConfigError::Invalid)?,
        );
    }
    if signed.is_empty() {
        return Ok(DecimalU64::new(revision));
    }
    if revision != request.expected_revision.get() {
        return Err(ConfigError::Conflict(Some(DecimalU64::new(revision))));
    }
    exchange::set(&session, database_id, signed)
        .await
        .map(DecimalU64::new)
        .map_err(update_error)
}

/// Hold the member lock and read one member's entry, verified as this
/// publisher's own. The lock keeps the entry current through a member commit.
pub(crate) async fn lock_member(
    home: &Path,
    database_id: &str,
    node: &Digest,
) -> Result<(std::fs::File, Option<MemberEntry>), ConfigError> {
    let lock = lock::hold(home, database_id).map_err(|_| ConfigError::Unconfirmed)?;
    let session = open(home).await?;
    let (_, entry) = exchange::get(&session, database_id, node)
        .await
        .map_err(|e| unconfirmed_or_payment(&e))?;
    Ok((lock, entry))
}

pub(super) async fn open(home: &Path) -> Result<PublisherSession, ConfigError> {
    PublisherSession::open(home)
        .await
        .map_err(|e| unconfirmed_or_payment(&e))
}

fn update_error(error: String) -> ConfigError {
    match error.as_str() {
        msg if msg == crate::cloud::PAYMENT_REQUIRED_MESSAGE => ConfigError::PaymentRequired,
        "share.grant.revision" => ConfigError::Conflict(None),
        "share.grant.identity" | "share.grant.personal" | "mutation_log.transaction" => {
            ConfigError::Invalid(error)
        }
        _ => ConfigError::Unconfirmed,
    }
}
