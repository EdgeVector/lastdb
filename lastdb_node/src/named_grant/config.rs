//! Publisher access to ExeMem's complete signed member configuration.

use super::{exchange, pending, session, successor};
use cloud_backup_protocol::v2::share_access::{
    MemberListReadResponse, ShareMember, ShareMemberRole, ShareMemberState,
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigMember {
    pub node_key: Digest,
    pub role: ShareMemberRole,
    pub state: ShareMemberState,
    #[serde(default, skip_serializing_if = "is_false")]
    pub regrant: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl From<ConfigMember> for ShareMember {
    fn from(member: ConfigMember) -> Self {
        Self {
            node_public_key: member.node_key,
            role: member.role,
            state: member.state,
            regrant: member.regrant,
        }
    }
}

impl From<ShareMember> for ConfigMember {
    fn from(member: ShareMember) -> Self {
        Self {
            node_key: member.node_public_key,
            role: member.role,
            state: member.state,
            regrant: member.regrant,
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
    Pending,
    Unconfirmed,
}

pub(crate) async fn read_config(
    home: &Path,
    database_id: &str,
) -> Result<Configuration, ConfigError> {
    let _lock = pending::lock(home, database_id).map_err(|_| ConfigError::Unconfirmed)?;
    if pending::load(home, database_id)
        .map_err(|_| ConfigError::Unconfirmed)?
        .is_some()
    {
        return Err(ConfigError::Pending);
    }
    let session = session::open(home)
        .await
        .map_err(|_| ConfigError::Unconfirmed)?;
    let document = read(&session, database_id).await?;
    Ok(Configuration {
        database_id: database_id.to_owned(),
        revision: document.revision,
        members: successor::current_members(&document)
            .into_iter()
            .map(Into::into)
            .collect(),
    })
}

pub(crate) async fn set_config(
    home: &Path,
    database_id: &str,
    request: ConfigUpdate,
) -> Result<DecimalU64, ConfigError> {
    let mut members: Vec<ShareMember> = request.members.into_iter().map(Into::into).collect();
    members.sort_by(|left, right| left.node_public_key.cmp(&right.node_public_key));
    let _lock = pending::lock(home, database_id).map_err(|_| ConfigError::Unconfirmed)?;
    let session = session::open(home)
        .await
        .map_err(|_| ConfigError::Unconfirmed)?;
    let expected = request.expected_revision.get();
    if let Some(saved) = pending::load(home, database_id).map_err(|_| ConfigError::Unconfirmed)? {
        if !saved.matches_config(&request.request_id, expected, &members) {
            return Err(ConfigError::Pending);
        }
        return exchange::post_saved(&session, home, database_id, &saved.body)
            .await
            .map(|ack| DecimalU64::new(ack.revision))
            .map_err(update_error);
    }
    let document = read(&session, database_id).await?;
    let body = if document.revision.get() == expected {
        if members == successor::current_members(&document) {
            return Ok(document.revision);
        }
        successor::sign_members(&session, &document, members)
            .map_err(ConfigError::Invalid)?
            .body
    } else {
        let existing = document.update.as_ref().filter(|update| {
            expected.checked_add(1) == Some(update.revision.get()) && update.members == members
        });
        let existing = existing.ok_or(ConfigError::Conflict(Some(document.revision)))?;
        serde_json::to_vec(existing).map_err(|_| ConfigError::Unconfirmed)?
    };
    pending::store_config(home, database_id, request.request_id, &body)
        .map_err(|_| ConfigError::Unconfirmed)?;
    exchange::post_saved(&session, home, database_id, &body)
        .await
        .map(|ack| DecimalU64::new(ack.revision))
        .map_err(update_error)
}

pub(super) async fn read(
    session: &session::PublisherSession,
    database_id: &str,
) -> Result<MemberListReadResponse, ConfigError> {
    let document = exchange::read_document(session, database_id)
        .await
        .map_err(|_| ConfigError::Unconfirmed)?;
    successor::require_stream(&document, session, database_id)
        .map_err(|_| ConfigError::Unconfirmed)?;
    Ok(document)
}

fn update_error(error: String) -> ConfigError {
    match error.as_str() {
        "share.grant.revision" => ConfigError::Conflict(None),
        "share.grant.identity" | "share.grant.personal" | "mutation_log.transaction" => {
            ConfigError::Invalid(error)
        }
        _ => ConfigError::Unconfirmed,
    }
}
