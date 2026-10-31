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

use super::types::ShareRule;

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
/// both.
///
/// The `signature`, `writer_pubkey`, `recipient_display_name`, `active`, and
/// `scope` fields are NOT included — display name / scope are mutable policy,
/// `active` toggles on deactivate, and signature/writer_pubkey are the
/// signature bindings themselves.
pub fn canonical_bytes(rule: &ShareRule) -> Vec<u8> {
    lastdb_model::canonical::CanonicalWriter::with_capacity(
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
