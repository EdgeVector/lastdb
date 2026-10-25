//! Public-key and exact-payload checks. Cloud membership remains authoritative.

use super::*;
use base64::Engine as _;
use ed25519_dalek::{Signature, VerifyingKey};
use serde_json::Value;

pub fn verify(request: &OrgAuthorityPublishRequest) -> SyncResult<(CloudAccount, Vec<String>)> {
    let envelope = &request.org_authority;
    if serde_json::to_vec(envelope)?.len() > ORG_AUTHORITY_BODY_LIMIT
        || envelope.epochs.is_empty()
        || envelope.epochs.len() > ORG_AUTHORITY_EPOCH_LIMIT
    {
        return Err(invalid(
            "the signed authority exceeds its byte or epoch limit",
        ));
    }
    let spki = base64::engine::general_purpose::STANDARD
        .decode(&envelope.org_root_spki_base64)
        .map_err(|_| invalid("the Org root is not base64"))?;
    let prefix = [
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    ];
    if spki.len() != 44 || spki[..12] != prefix {
        return Err(invalid("the Org root must be an Ed25519 SPKI"));
    }
    let bytes: [u8; 32] = spki[12..]
        .try_into()
        .map_err(|_| invalid("invalid Org root length"))?;
    let key = VerifyingKey::from_bytes(&bytes).map_err(|_| invalid("invalid Org root key"))?;
    let account = CloudAccount {
        kind: CloudAccountKind::Org,
        id: crate::hex::sha256_hex(&spki),
    };
    if request
        .account
        .as_ref()
        .is_some_and(|selected| selected != &account)
    {
        return Err(invalid("the account differs from the signed Org root"));
    }
    let mut hashes = Vec::with_capacity(envelope.epochs.len());
    for (number, epoch) in envelope.epochs.iter().enumerate() {
        let payload = verify_epoch(epoch, &key)?;
        let previous = hashes.last().map_or("", String::as_str);
        if payload["v"] != 1
            || payload["org_hash"] != account.id
            || payload["epoch_no"].as_u64() != u64::try_from(number).ok()
            || payload["prev_epoch"].as_str() != Some(previous)
        {
            return Err(invalid(
                "the authority must be an exact genesis-to-tip chain",
            ));
        }
        let hash = crate::hex::sha256_hex(epoch.payload_jcs.as_bytes());
        if number + 1 == envelope.epochs.len() {
            let authority = json!({"account":&account,"epoch_no":number,"epoch_hash":&hash,
                "org_root_spki_base64":&envelope.org_root_spki_base64,"epoch":&payload});
            if serde_json::to_vec(&authority)?.len() > ORG_AUTHORITY_TERMINAL_JSON_LIMIT {
                return Err(SyncError::Storage(
                    "cloud_org_authority_item_too_large".into(),
                ));
            }
        }
        hashes.push(hash);
    }
    Ok((account, hashes))
}

fn verify_epoch(epoch: &OrgAuthoritySignedPayload, key: &VerifyingKey) -> SyncResult<Value> {
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&epoch.signature_base64url)
        .map_err(|_| invalid("the epoch signature is not base64url"))?;
    let signature =
        Signature::from_slice(&signature).map_err(|_| invalid("invalid epoch signature length"))?;
    key.verify_strict(epoch.payload_jcs.as_bytes(), &signature)
        .map_err(|_| invalid("the epoch signature does not match the Org root"))?;
    let payload: Value =
        serde_json::from_str(&epoch.payload_jcs).map_err(|_| invalid("invalid epoch JSON"))?;
    if serde_json::to_string(&payload)? != epoch.payload_jcs {
        return Err(invalid(
            "the epoch payload must retain canonical JSON bytes",
        ));
    }
    Ok(payload)
}

pub fn bind_tip(
    request: &OrgAuthorityPublishRequest,
    tip: &OrgAuthorityTip,
    hashes: &[String],
) -> SyncResult<()> {
    if !tip.absent {
        let remote = tip.epoch_no.and_then(|value| usize::try_from(value).ok());
        if remote.and_then(|number| hashes.get(number)) != tip.epoch_hash.as_ref() {
            return Err(conflict());
        }
    }
    let exact_replay = !tip.absent && tip.epoch_hash.as_ref() == hashes.last();
    match &request.expected_epoch_hash {
        OrgAuthorityExpectedEpoch::Unspecified => {}
        OrgAuthorityExpectedEpoch::Absent if tip.absent || exact_replay => {}
        OrgAuthorityExpectedEpoch::Exact(expected)
            if valid_hash(expected)
                && (tip.epoch_hash.as_ref() == Some(expected)
                    || exact_replay && hashes.contains(expected)) => {}
        _ => return Err(conflict()),
    }
    Ok(())
}
