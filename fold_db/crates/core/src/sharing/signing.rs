//! Ed25519 signing helpers for [`ShareRule`].
//!
//! A share rule authorizes the writer to publish encrypted writes under
//! `share_prefix` that any holder of `share_e2e_secret` can decrypt. The
//! signature binds `rule_id`, `recipient_pubkey`, `share_prefix`,
//! `share_e2e_secret`, and `created_at` to `writer_pubkey` so an observer
//! (the recipient, or anyone verifying the rule later) can confirm the rule
//! was issued by the claimed writer.
//!
//! Verification is currently non-enforcing: the receiver-side of cross-user
//! sharing does NOT reject unsigned or invalid rules yet. Signing is added
//! first so the wire format is future-proof; enforcement lands in a follow-up.

use super::types::{ShareAccessGrant, ShareInvite, ShareRule};
use crate::security::{Ed25519KeyPair, Ed25519PublicKey, KeyUtils, SecurityError, SecurityResult};

/// Domain-separation tag for [`ShareAccessGrant`] signatures. Prevents a
/// signature minted for another message type (or another version) from ever
/// verifying as an access grant.
pub const SHARE_ACCESS_GRANT_DOMAIN: &[u8] = b"folddb:share_access_grant:v1";

/// Canonical byte serialization used for both signing and verification.
///
/// Format: each variable-length field is preceded by a 4-byte big-endian
/// length prefix and followed by its raw bytes, in this fixed order:
/// `len(rule_id) || rule_id || len(recipient_pubkey) || recipient_pubkey ||
///  len(share_prefix) || share_prefix || len(share_e2e_secret) ||
///  share_e2e_secret || created_at.to_be_bytes()`
///
/// Length-prefixing (rather than a `0x00` separator) is required because
/// `share_e2e_secret` is random bytes that may legitimately contain `0x00`.
/// With a separator, an attacker could shift bytes across the
/// share_prefix/share_e2e_secret boundary so two semantically-different
/// rules produce identical canonical bytes — letting one signature verify
/// both. See `nul_in_secret_does_not_collide_with_shifted_prefix`.
///
/// The `signature`, `writer_pubkey`, `recipient_display_name`, `active`, and
/// `scope` fields are NOT included — display name / scope are mutable policy,
/// `active` toggles on deactivate, and signature/writer_pubkey are the
/// signature bindings themselves.
pub fn canonical_bytes(rule: &ShareRule) -> Vec<u8> {
    crate::canonical::CanonicalWriter::with_capacity(
        rule.rule_id.len()
            + rule.recipient_pubkey.len()
            + rule.share_prefix.len()
            + rule.share_e2e_secret.len()
            + 8
            + 4 * 4,
    )
    .field(rule.rule_id.as_bytes())
    .field(rule.recipient_pubkey.as_bytes())
    .field(rule.share_prefix.as_bytes())
    .field(&rule.share_e2e_secret)
    .u64(rule.created_at)
    .finish()
}

/// Sign `rule` with `keypair`. Returns the base64-encoded signature.
/// The caller is responsible for placing the signature on `rule.signature` and
/// ensuring `rule.writer_pubkey` matches `keypair`'s public key.
pub fn sign_share_rule(rule: &ShareRule, keypair: &Ed25519KeyPair) -> String {
    let bytes = canonical_bytes(rule);
    let sig = keypair.sign(&bytes);
    KeyUtils::signature_to_base64(&sig)
}

/// Verify the signature on `rule` against `rule.writer_pubkey`.
/// Returns `Ok(true)` on valid signature, `Ok(false)` on mismatch,
/// `Err` only when the public-key or signature encoding itself is malformed.
pub fn verify_share_rule(rule: &ShareRule) -> SecurityResult<bool> {
    if rule.signature.is_empty() {
        return Ok(false);
    }
    let pubkey = Ed25519PublicKey::from_base64(&rule.writer_pubkey)
        .map_err(|e| SecurityError::InvalidPublicKey(e.to_string()))?;
    let signature = KeyUtils::signature_from_base64(&rule.signature)
        .map_err(|e| SecurityError::InvalidSignature(e.to_string()))?;
    let bytes = canonical_bytes(rule);
    Ok(pubkey.verify(&bytes, &signature))
}

/// Canonical byte serialization for a share invite.
///
/// The signature binds the exact share prefix and plaintext E2E secret that
/// `POST /api/sharing/accept` will persist into a subscription. The signature
/// itself is excluded.
pub fn share_invite_canonical_bytes(invite: &ShareInvite) -> Vec<u8> {
    crate::canonical::CanonicalWriter::with_capacity(
        invite.sender_pubkey.len()
            + invite.sender_display_name.len()
            + invite.share_prefix.len()
            + invite.share_e2e_secret.len()
            + invite.scope_description.len()
            + 6 * 4,
    )
    .field(b"folddb:share_invite:v1")
    .field(invite.sender_pubkey.as_bytes())
    .field(invite.sender_display_name.as_bytes())
    .field(invite.share_prefix.as_bytes())
    .field(&invite.share_e2e_secret)
    .field(invite.scope_description.as_bytes())
    .finish()
}

/// Sign `invite` with the sender's node keypair.
pub fn sign_share_invite(invite: &ShareInvite, keypair: &Ed25519KeyPair) -> String {
    let bytes = share_invite_canonical_bytes(invite);
    let sig = keypair.sign(&bytes);
    KeyUtils::signature_to_base64(&sig)
}

/// Verify the signature on `invite` against `invite.sender_pubkey`.
pub fn verify_share_invite(invite: &ShareInvite) -> SecurityResult<bool> {
    if invite.signature.is_empty() {
        return Ok(false);
    }
    let pubkey = Ed25519PublicKey::from_base64(&invite.sender_pubkey)
        .map_err(|e| SecurityError::InvalidPublicKey(e.to_string()))?;
    let signature = KeyUtils::signature_from_base64(&invite.signature)
        .map_err(|e| SecurityError::InvalidSignature(e.to_string()))?;
    let bytes = share_invite_canonical_bytes(invite);
    Ok(pubkey.verify(&bytes, &signature))
}

/// Canonical byte serialization for a [`ShareAccessGrant`].
///
/// Fixed field order, each variable field length-prefixed (see
/// [`crate::canonical`]):
/// `SHARE_ACCESS_GRANT_DOMAIN || share_prefix || sender_pubkey ||
///  expires_at.to_be_bytes()`.
///
/// The `share_e2e_secret` is intentionally **absent** — a grant is an
/// authorization to read the (already-encrypted) log, not a key. The
/// `signature` field is excluded (it is the binding itself).
///
/// The storage service re-implements this exact framing to verify the grant
/// without linking `fold_db`; any change here MUST be mirrored in
/// `exemem_service/lambdas/storage_service` (see its `share_access` module) or
/// recipient reads will start failing signature verification. See
/// `SHARE_ACCESS_CONTRACT.md`.
pub fn share_access_grant_canonical_bytes(grant: &ShareAccessGrant) -> Vec<u8> {
    crate::canonical::CanonicalWriter::with_capacity(
        SHARE_ACCESS_GRANT_DOMAIN.len()
            + grant.share_prefix.len()
            + grant.sender_pubkey.len()
            + 8
            + 3 * 4,
    )
    .field(SHARE_ACCESS_GRANT_DOMAIN)
    .field(grant.share_prefix.as_bytes())
    .field(grant.sender_pubkey.as_bytes())
    .u64(grant.expires_at)
    .finish()
}

/// Sign a [`ShareAccessGrant`] with the sender's node keypair. Returns the
/// base64-encoded signature; the caller places it on `grant.signature` and is
/// responsible for ensuring `grant.sender_pubkey` matches `keypair`.
pub fn sign_share_access_grant(grant: &ShareAccessGrant, keypair: &Ed25519KeyPair) -> String {
    let bytes = share_access_grant_canonical_bytes(grant);
    let sig = keypair.sign(&bytes);
    KeyUtils::signature_to_base64(&sig)
}

/// Verify the signature on a [`ShareAccessGrant`] against `grant.sender_pubkey`.
///
/// Returns `Ok(true)` on a valid signature, `Ok(false)` on mismatch or an empty
/// signature, and `Err` only when the public-key or signature encoding is
/// malformed. Callers that authorize a recipient MUST additionally check that
/// `grant.share_prefix` is the prefix being requested, that
/// `hex(SHA256(sender_pubkey)[..16])` equals the prefix's `sender_hash`, and
/// that `grant.expires_at` is in the future — the signature alone binds those
/// fields but does not enforce the policy.
pub fn verify_share_access_grant(grant: &ShareAccessGrant) -> SecurityResult<bool> {
    if grant.signature.is_empty() {
        return Ok(false);
    }
    let pubkey = Ed25519PublicKey::from_base64(&grant.sender_pubkey)
        .map_err(|e| SecurityError::InvalidPublicKey(e.to_string()))?;
    let signature = KeyUtils::signature_from_base64(&grant.signature)
        .map_err(|e| SecurityError::InvalidSignature(e.to_string()))?;
    let bytes = share_access_grant_canonical_bytes(grant);
    Ok(pubkey.verify(&bytes, &signature))
}
