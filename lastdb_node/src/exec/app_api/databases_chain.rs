//! Signed-history grant actions of the `databases` extension.
//!
//! These answer from the folded signed chain on this node. They do not call
//! an owner route and they do not reach any member service: grants exist only
//! as publisher-signed `grant` entries.

use fold_db::access::AccessContext;
use mutation_log::{Grantee, Hex32, Role};
use serde_json::{Map, Value};

use super::databases::{allow, database_id, required_string};
use super::{bad, ok};
use crate::exec::reply::{envelope, HostError};
use crate::host::Host;
use crate::lastdb_uds::uds_http::UdsResponse;
use crate::log_sync::chain_error::ChainError;
use crate::log_sync::{chain_grant, chain_read, lease_status};

const ACTIONS: [&str; 8] = [
    "grant",
    "revoke",
    "grants",
    "heads",
    "relays",
    "bootstrap_relay",
    "lease",
    "lease_length",
];

pub(super) fn handles(action: &str) -> bool {
    ACTIONS.contains(&action)
}

pub(super) fn run(
    action: &str,
    body: &Map<String, Value>,
    ctx: &AccessContext,
    host: &Host,
) -> Result<UdsResponse, HostError> {
    if !ctx.is_owner {
        return Err(HostError::new(404, "Not Found"));
    }
    let reply = match action {
        "grant" => grant(body, host),
        "revoke" => revoke(body, host),
        "grants" => read(body, host, chain_read::grants),
        "heads" => read(body, host, chain_read::heads),
        "relays" => read(body, host, chain_read::relays),
        "lease" => read(body, host, lease_status::status),
        "lease_length" => lease_length(body, host),
        _ => read(body, host, chain_read::bootstrap_relay),
    }?;
    Ok(ok(&envelope(&reply, &ctx.user_id)))
}

fn grant(body: &Map<String, Value>, host: &Host) -> Result<Value, HostError> {
    allow(body, &["database", "grantee", "role"])?;
    let selector = database_id(body, host)?;
    let grantee = grantee(body)?;
    let role = match required_string(body, "role")? {
        "read" => Role::Read,
        "submit" => Role::Submit,
        "write" => Role::Write,
        _ => return Err(bad("role must be read, submit, or write")),
    };
    chain_grant::grant(host, &selector, grantee, role).map_err(|error| refuse(&error))
}

fn revoke(body: &Map<String, Value>, host: &Host) -> Result<Value, HostError> {
    allow(body, &["database", "grantee"])?;
    let selector = database_id(body, host)?;
    chain_grant::revoke(host, &selector, grantee(body)?).map_err(|error| refuse(&error))
}

/// Set the lease length of an owned database (`length_ms` a number), or clear it
/// (`length_ms` null).
fn lease_length(body: &Map<String, Value>, host: &Host) -> Result<Value, HostError> {
    allow(body, &["database", "length_ms"])?;
    let selector = database_id(body, host)?;
    let length = match body.get("length_ms") {
        Some(Value::Null) => None,
        Some(Value::Number(number)) => Some(
            number
                .as_u64()
                .ok_or_else(|| bad("length_ms must be a whole number"))?,
        ),
        _ => return Err(bad("length_ms must be a whole number or null")),
    };
    lease_status::set_length(host, &selector, length).map_err(|error| refuse(&error))
}

fn read(
    body: &Map<String, Value>,
    host: &Host,
    view: fn(&Host, &str) -> Result<Value, ChainError>,
) -> Result<Value, HostError> {
    allow(body, &["database"])?;
    let selector = database_id(body, host)?;
    view(host, &selector).map_err(|error| refuse(&error))
}

fn grantee(body: &Map<String, Value>) -> Result<Grantee, HostError> {
    match required_string(body, "grantee")? {
        "any" => Ok(Grantee::Any),
        key => Hex32::from_hex(key)
            .map(Grantee::Member)
            .map_err(|_| bad("grantee must be 64 lowercase hex characters or any")),
    }
}

fn refuse(error: &ChainError) -> HostError {
    let reply = error.reply();
    let refusal = HostError::new(reply.status, reply.message);
    let refusal = match reply.code {
        Some(code) => refusal.with_code(code),
        None => refusal,
    };
    if reply.retry {
        refusal.with_retry_after(crate::exec::database_share::RETRY_AFTER_SECS)
    } else {
        refusal
    }
}
