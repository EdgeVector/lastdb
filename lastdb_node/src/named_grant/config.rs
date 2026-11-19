//! Publisher view of a shared database's members and settings as one configuration.
//!
//! The service stores one signed entry per member and one settings statement.
//! The owner API still reads and writes the member set as a whole, so a read
//! collects every entry and a write signs only the entries that change. A
//! settings change is a separate signed statement. The service decides which
//! pairs it accepts and when a change is allowed.

use super::entries::{self, Grant};
use super::settings;
use super::{exchange, lock, PublisherSession};
use cloud_backup_protocol::v2::share_access::{
    MemberEntry, ShareMemberRole, ShareMemberState, ShareRead, ShareWrite, ShareWriteScope,
};
use cloud_backup_protocol::{DecimalU64, Digest, ErrorCode, Id};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ConfigUpdate {
    pub expected_revision: DecimalU64,
    pub request_id: Id,
    members: Vec<ConfigMember>,
    /// Omitted keeps the stored value. No stored statement is members.
    #[serde(default)]
    read: Option<ShareRead>,
    /// Omitted keeps the stored value. No stored statement is proposals.
    #[serde(default)]
    write: Option<ShareWrite>,
}

impl ConfigUpdate {
    pub(crate) fn scoped_members(&self) -> impl Iterator<Item = &ShareWriteScope> {
        self.members
            .iter()
            .filter_map(|member| member.write_schemas.as_ref())
    }

    /// An active submitter must name a schema scope. A reader omits it.
    pub(crate) fn writer_missing_scope(&self) -> bool {
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
    read: ShareRead,
    write: ShareWrite,
    members: Vec<ConfigMember>,
}

pub(crate) enum ConfigError {
    Invalid(String),
    Conflict(Option<DecimalU64>),
    PaymentRequired,
    Unconfirmed,
    /// The service refused the settings statement. `code` is its error code
    /// and `message` is its field. The node does not replace either one.
    Refused {
        status: u16,
        code: String,
        message: String,
    },
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
        .map_err(|error| unconfirmed_or_payment(&error))?;
    let current = exchange::get_settings(&session, database_id)
        .await
        .map_err(settings_failure)?;
    let (read, write) = settings::mode(current.as_ref());
    Ok(Configuration {
        database_id: database_id.to_owned(),
        revision: DecimalU64::new(revision),
        read,
        write,
        members: stored.into_iter().map(Into::into).collect(),
    })
}

/// Make the stored members and settings equal the request.
///
/// Settings go out first, then member entries. When the member list would
/// change and `expectedRevision` is not the stored member revision, the call
/// returns that conflict before either write. A settings-only change does not
/// move the member revision and does not require it to match.
///
/// If the settings call fails, no member entry is sent and the caller sees
/// the service refusal. If settings succeed and the member call fails, the
/// caller sees the member error and the new pair is already stored. A repeat
/// of the same body reads that pair, sends no statement, and retries the
/// members while `expectedRevision` still matches. `requestId` is echoed and
/// is not sent to the service.
///
/// An omitted `read` or `write` keeps the stored value. No stored statement
/// is members and proposals. The same pair sends no statement: the service
/// refuses it, and a repeat whose answer was lost must not send it again.
/// Every stored member must be named: leaving one out is not a revoke.
pub(crate) async fn set_config(
    home: &Path,
    database_id: &str,
    request: ConfigUpdate,
) -> Result<DecimalU64, ConfigError> {
    let read = request.read;
    let write = request.write;
    let expected = request.expected_revision.get();
    let members = normalize(request.members)?;
    let _lock = lock::hold(home, database_id).map_err(|_| ConfigError::Unconfirmed)?;
    let session = open(home).await?;
    let (revision, stored) = exchange::list(&session, database_id)
        .await
        .map_err(|error| unconfirmed_or_payment(&error))?;
    let signed = member_updates(&session, database_id, &stored, members)?;
    if !signed.is_empty() && revision != expected {
        return Err(ConfigError::Conflict(Some(DecimalU64::new(revision))));
    }
    if read.is_some() || write.is_some() {
        store_settings(&session, database_id, read, write).await?;
    }
    if signed.is_empty() {
        return Ok(DecimalU64::new(revision));
    }
    exchange::set(&session, database_id, signed)
        .await
        .map(DecimalU64::new)
        .map_err(update_error)
}

fn normalize(mut members: Vec<ConfigMember>) -> Result<Vec<ConfigMember>, ConfigError> {
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
    Ok(members)
}

fn member_updates(
    session: &PublisherSession,
    database_id: &str,
    stored: &[MemberEntry],
    members: Vec<ConfigMember>,
) -> Result<Vec<MemberEntry>, ConfigError> {
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
            entries::sign(session, database_id, &member.node_key, current, grant)
                .map_err(ConfigError::Invalid)?,
        );
    }
    Ok(signed)
}

async fn store_settings(
    session: &PublisherSession,
    database_id: &str,
    read: Option<ShareRead>,
    write: Option<ShareWrite>,
) -> Result<(), ConfigError> {
    let current = exchange::get_settings(session, database_id)
        .await
        .map_err(settings_failure)?;
    let Some(statement) = settings::prepare(session, database_id, current.as_ref(), read, write)
        .map_err(ConfigError::Invalid)?
    else {
        return Ok(());
    };
    exchange::set_settings(session, database_id, statement)
        .await
        .map_err(settings_failure)
}

fn settings_failure(error: exchange::CloudRefusal) -> ConfigError {
    match error {
        exchange::CloudRefusal::Unconfirmed => ConfigError::Unconfirmed,
        exchange::CloudRefusal::Payment => ConfigError::PaymentRequired,
        exchange::CloudRefusal::Service { status, error } => {
            let code = protocol_code(&error.code);
            let message = if error.field.is_empty() {
                code.clone()
            } else {
                error.field
            };
            ConfigError::Refused {
                status,
                code,
                message,
            }
        }
    }
}

fn protocol_code(code: &ErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_input".into())
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
