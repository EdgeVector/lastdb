//! LastDBNode token and the exact digest input for one mutation-log request.
//!
//! The signature covers the lowercase hex of this digest, not these bytes.
//! The tuple order is the one exemem-infra #769 uses at a73cf5a8:
//! method, path, stream id, issued time, raw-body hash, version, stream,
//! nonce, member-list revision, node public key.
//! This binary does not sign and does not store the nonce.

use std::io::{self, Read, Write};
use std::process::ExitCode;

use cloud_backup_protocol::v2::share_access::{ShareHttpMethod, SHARE_NODE_READ_AUTHORITY_DOMAIN};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{
    bytes_sha256, canonical_bytes, input_digest, AccountId, CloudScope, DecimalU64, Digest, Id,
    ProtocolError, Text,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Token body. Unknown fields are rejected. `method` is always POST.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeReadToken {
    version: Version,
    environment: Text<16>,
    stream: Stream,
    method: ShareHttpMethod,
    path: Text<512>,
    issued_at_ms: DecimalU64,
    nonce: Text<64>,
    member_list_revision: DecimalU64,
    node_public_key: Digest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    environment: String,
    account_id: String,
    account_generation: String,
    stream_id: String,
    path: String,
    issued_at_ms: String,
    nonce: String,
    member_list_revision: String,
    node_public_key: String,
    body: String,
}

struct Built {
    digest: String,
    token_json: String,
    canonical: String,
}

fn main() -> ExitCode {
    match read_input().and_then(build) {
        Ok(built) => write_output(&built),
        Err(error) => {
            let _ = writeln!(io::stderr(), "node_read_token: {error}");
            ExitCode::from(1)
        }
    }
}

fn read_input() -> Result<Input, ProtocolError> {
    let mut buffer = Vec::new();
    io::stdin()
        .take(65_537)
        .read_to_end(&mut buffer)
        .map_err(|_| ProtocolError::invalid("node_read.input"))?;
    if buffer.len() > 65_536 {
        return Err(ProtocolError::invalid("node_read.input"));
    }
    serde_json::from_slice(&buffer).map_err(|_| ProtocolError::invalid("node_read.input"))
}

fn build(input: Input) -> Result<Built, ProtocolError> {
    let public_key = Digest::try_from(input.node_public_key)?;
    let account = AccountId::try_from(input.account_id)?;
    account_matches(&public_key, &account)?;
    let stream = Stream {
        scope: CloudScope {
            account_id: account,
            account_generation: DecimalU64::try_from(input.account_generation)?,
        },
        stream_id: Id::try_from(input.stream_id)?,
    };
    stream.validate()?;
    let token = NodeReadToken {
        version: Version::V2,
        environment: environment(&input.environment)?,
        stream,
        method: ShareHttpMethod::Post,
        path: mutation_path(&input.path)?,
        issued_at_ms: issued(&input.issued_at_ms)?,
        nonce: nonce(&input.nonce)?,
        member_list_revision: DecimalU64::try_from(input.member_list_revision)?,
        node_public_key: public_key,
    };
    authority(&token, input.body.as_bytes())
}

fn authority(token: &NodeReadToken, body: &[u8]) -> Result<Built, ProtocolError> {
    let covered = (
        token.method,
        token.path.as_str(),
        token.stream.stream_id.clone(),
        token.issued_at_ms,
        bytes_sha256(body),
        &token.version,
        &token.stream,
        token.nonce.as_str(),
        token.member_list_revision,
        &token.node_public_key,
    );
    let digest = input_digest(SHARE_NODE_READ_AUTHORITY_DOMAIN, &covered)?;
    let canonical = String::from_utf8(canonical_bytes(&covered)?)
        .map_err(|_| ProtocolError::invalid("node_read.canonical"))?;
    let token_json =
        serde_json::to_string(token).map_err(|_| ProtocolError::invalid("node_read.token"))?;
    let parsed: NodeReadToken =
        serde_json::from_str(&token_json).map_err(|_| ProtocolError::invalid("node_read.token"))?;
    if parsed != *token {
        return Err(ProtocolError::invalid("node_read.token"));
    }
    Ok(Built {
        digest: digest.as_str().to_string(),
        token_json,
        canonical,
    })
}

fn write_output(built: &Built) -> ExitCode {
    let output = serde_json::json!({
        "digest": built.digest,
        "token_json": built.token_json,
        "canonical": built.canonical,
    });
    if writeln!(io::stdout(), "{output}").is_err() {
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

/// `dev` and `prod` are `Environment::as_str` and the `environments.json` keys.
fn environment(value: &str) -> Result<Text<16>, ProtocolError> {
    if value != "dev" && value != "prod" {
        return Err(ProtocolError::invalid("node_read.environment"));
    }
    Text::<16>::try_from(value.to_string())
}

fn mutation_path(path: &str) -> Result<Text<512>, ProtocolError> {
    let text = Text::<512>::try_from(path.to_string())?;
    let Some(rest) = path.strip_prefix("/api/cloud/backups/v2/mutation-log/") else {
        return Err(ProtocolError::invalid("node_read.path"));
    };
    let bad = !path.is_ascii()
        || path.contains(['?', '#', '\\'])
        || rest
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..");
    if bad {
        return Err(ProtocolError::invalid("node_read.path"));
    }
    Ok(text)
}

fn issued(value: &str) -> Result<DecimalU64, ProtocolError> {
    let parsed = DecimalU64::try_from(value.to_string())?;
    if parsed.get() == 0 {
        return Err(ProtocolError::invalid("node_read.issued_at_ms"));
    }
    Ok(parsed)
}

fn nonce(value: &str) -> Result<Text<64>, ProtocolError> {
    let text = Text::<64>::try_from(value.to_string())?;
    let hex = value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !hex {
        return Err(ProtocolError::invalid("node_read.nonce"));
    }
    Ok(text)
}

fn account_matches(public_key: &Digest, account: &AccountId) -> Result<(), ProtocolError> {
    let raw = decode_key(public_key)?;
    let hash = Sha256::digest(raw);
    let derived: String = hash[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if derived != account.as_str() {
        return Err(ProtocolError::invalid("node_read.account_id"));
    }
    Ok(())
}

fn decode_key(encoded: &Digest) -> Result<[u8; 32], ProtocolError> {
    let bytes = encoded.as_str().as_bytes();
    if bytes.len() != 64 {
        return Err(ProtocolError::invalid("node_read.public_key"));
    }
    let mut raw = [0u8; 32];
    for (index, slot) in raw.iter_mut().enumerate() {
        let high = hex_value(bytes[index * 2])?;
        let low = hex_value(bytes[index * 2 + 1])?;
        *slot = (high << 4) | low;
    }
    Ok(raw)
}

fn hex_value(byte: u8) -> Result<u8, ProtocolError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(ProtocolError::invalid("node_read.public_key")),
    }
}
