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
    MemberEntry, MemberWriteBehaviour, ShareMemberRole, ShareMemberState, ShareRead, ShareWrite,
};
use cloud_backup_protocol::{DecimalU64, Digest, ErrorCode, Id};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
    /// Omitted keeps the stored map. An empty object clears it. No stored
    /// statement is an empty map, and a schema that is absent is owner_only.
    #[serde(default)]
    member_writes: Option<BTreeMap<String, MemberWriteBehaviour>>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigMember {
    pub node_key: Digest,
    pub role: ShareMemberRole,
    pub state: ShareMemberState,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub regrant: bool,
}

impl From<MemberEntry> for ConfigMember {
    fn from(entry: MemberEntry) -> Self {
        Self {
            node_key: entry.node_public_key,
            role: entry.role,
            state: entry.state,
            regrant: false,
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
    /// Empty when the database has no member-write map.
    member_writes: BTreeMap<String, MemberWriteBehaviour>,
    members: Vec<ConfigMember>,
}

pub(crate) enum ConfigError {
    Invalid(String),
    Conflict(Option<DecimalU64>),
    PaymentRequired,
    Unconfirmed,
    /// The node has no usable cloud account. The caller does not wait.
    NoAccount,
    /// The node has an account and this database's cloud log is not confirmed.
    Enrolling,
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

/// Classify a cloud failure by the database's current enrollment.
/// This does not wait. `Unconfirmed` remains when the database is enrolled or
/// the saved credentials could not be verified.
pub(crate) fn cloud_failure(home: &Path, database_id: &str, error: &str) -> ConfigError {
    if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
        return ConfigError::PaymentRequired;
    }
    match super::cloud_state::database_cloud_state(home, database_id) {
        super::cloud_state::CloudState::NoAccount => ConfigError::NoAccount,
        super::cloud_state::CloudState::Enrolling => ConfigError::Enrolling,
        super::cloud_state::CloudState::Unverified | super::cloud_state::CloudState::Enrolled => {
            ConfigError::Unconfirmed
        }
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
        .map_err(|error| cloud_failure(home, database_id, &error))?;
    let current = exchange::get_settings(&session, database_id)
        .await
        .map_err(|error| settings_failure(home, database_id, error))?;
    let (read, write) = settings::mode(current.as_ref());
    Ok(Configuration {
        database_id: database_id.to_owned(),
        revision: DecimalU64::new(revision),
        read,
        write,
        member_writes: settings::writes(current.as_ref()),
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
/// An omitted `read`, `write`, or `memberWrites` keeps the stored value. No
/// stored statement is members, proposals, and an empty map. The same three
/// send no statement: the service refuses an unchanged statement, and a
/// repeat whose answer was lost must not send it again. Every stored member
/// must be named: leaving one out is not a revoke.
pub(crate) async fn set_config(
    home: &Path,
    database_id: &str,
    request: ConfigUpdate,
) -> Result<DecimalU64, ConfigError> {
    let read = request.read;
    let write = request.write;
    let member_writes = request.member_writes;
    let expected = request.expected_revision.get();
    let members = normalize(request.members)?;
    let _lock = lock::hold(home, database_id).map_err(|_| ConfigError::Unconfirmed)?;
    let session = open(home).await?;
    let (revision, stored) = exchange::list(&session, database_id)
        .await
        .map_err(|error| cloud_failure(home, database_id, &error))?;
    let signed = member_updates(&session, database_id, &stored, members)?;
    if !signed.is_empty() && revision != expected {
        return Err(ConfigError::Conflict(Some(DecimalU64::new(revision))));
    }
    if read.is_some() || write.is_some() || member_writes.is_some() {
        store_settings(
            home,
            &session,
            database_id,
            read,
            write,
            member_writes.as_ref(),
        )
        .await?;
    }
    if signed.is_empty() {
        return Ok(DecimalU64::new(revision));
    }
    exchange::set(&session, database_id, signed)
        .await
        .map(DecimalU64::new)
        .map_err(|error| update_error(home, database_id, error))
}

fn normalize(mut members: Vec<ConfigMember>) -> Result<Vec<ConfigMember>, ConfigError> {
    members.sort_by(|left, right| left.node_key.cmp(&right.node_key));
    if members
        .windows(2)
        .any(|pair| pair[0].node_key == pair[1].node_key)
    {
        return Err(ConfigError::Invalid("share.members.order".into()));
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
    home: &Path,
    session: &PublisherSession,
    database_id: &str,
    read: Option<ShareRead>,
    write: Option<ShareWrite>,
    member_writes: Option<&BTreeMap<String, MemberWriteBehaviour>>,
) -> Result<(), ConfigError> {
    let current = exchange::get_settings(session, database_id)
        .await
        .map_err(|error| settings_failure(home, database_id, error))?;
    let Some(statement) = settings::prepare(
        session,
        database_id,
        current.as_ref(),
        read,
        write,
        member_writes,
    )
    .map_err(ConfigError::Invalid)?
    else {
        return Ok(());
    };
    exchange::set_settings(session, database_id, statement)
        .await
        .map_err(|error| settings_failure(home, database_id, error))
}

fn settings_failure(home: &Path, database_id: &str, error: exchange::CloudRefusal) -> ConfigError {
    let payment = matches!(error, exchange::CloudRefusal::Payment);
    match super::cloud_state::database_cloud_state(home, database_id) {
        super::cloud_state::CloudState::NoAccount if !payment => ConfigError::NoAccount,
        super::cloud_state::CloudState::Enrolling if !payment => ConfigError::Enrolling,
        _ => service_refusal(error),
    }
}

fn service_refusal(error: exchange::CloudRefusal) -> ConfigError {
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

/// The member-write map on the statement this publisher just confirmed.
///
/// The caller holds the member lock. This does not take that lock: the lock
/// is not re-entrant. The read is `POST /api/cloud/backups/v2/share/members/settings/get`.
/// `exchange::get_settings` accepts the body only when this publisher's
/// signature verifies. No stored statement is a confirmed empty map. A failed
/// read is unconfirmed. The caller must not treat that failure as `owner_only`
/// and must not discard a proposal because of it.
pub(crate) async fn confirmed_member_writes(
    home: &Path,
    database_id: &str,
) -> Result<BTreeMap<String, MemberWriteBehaviour>, String> {
    let session = open(home)
        .await
        .map_err(|_| "share settings are unconfirmed".to_owned())?;
    let current = exchange::get_settings(&session, database_id)
        .await
        .map_err(|_| "share settings are unconfirmed".to_owned())?;
    Ok(settings::writes(current.as_ref()))
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
        .map_err(|error| cloud_failure(home, database_id, &error))?;
    Ok((lock, entry))
}

pub(super) async fn open(home: &Path) -> Result<PublisherSession, ConfigError> {
    PublisherSession::open(home).await.map_err(|error| {
        if super::cloud_state::no_cloud_account(home) {
            ConfigError::NoAccount
        } else {
            unconfirmed_or_payment(&error)
        }
    })
}

fn update_error(home: &Path, database_id: &str, error: String) -> ConfigError {
    match error.as_str() {
        "share.grant.revision" => ConfigError::Conflict(None),
        "share.grant.identity" | "share.grant.personal" | "mutation_log.transaction" => {
            ConfigError::Invalid(error)
        }
        _ => cloud_failure(home, database_id, &error),
    }
}
