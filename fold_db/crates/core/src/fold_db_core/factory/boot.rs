//! Native-home at-rest key proof and namespace checks.

use crate::error::{FoldDbError, FoldDbResult};

mod proof_scan;
use proof_scan::scan_proof_namespace;

pub(super) fn is_decrypt_failure_text(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("aead")
        || lower.contains("aes-gcm")
        || lower.contains("decrypt")
        || lower.contains("wrong encryption key")
        || lower.contains("wrong master key")
        || lower.contains("encryption error")
        || lower.contains("key mismatch")
        || lower.contains("undecryptable")
}

pub(super) fn map_boot_decrypt_error(msg: String) -> FoldDbError {
    if is_decrypt_failure_text(&msg) {
        FoldDbError::Database(format!(
            "cannot decrypt existing store — wrong master key or incompatible build; \
             refusing to start fresh: {msg}"
        ))
    } else {
        FoldDbError::Config(msg)
    }
}

/// Encrypted structural namespaces checked before the native home can serve.
/// These checks reject unreadable data; they never delete it. The sentinel
/// independently proves the encryption key even when these namespaces are empty.
const BOOT_DECRYPT_PROOF_STRUCTURAL: &[&str] = &["metadata"];

/// Namespace and key of the at-rest key proof — one small sealed row written on
/// first boot and verified on every subsequent one.
///
/// The scan-based proof above can only fail if some proof namespace happens to
/// hold rows, which on a young or lightly-used home is not guaranteed. This
/// sentinel makes the guarantee unconditional and O(1): after any successful
/// boot the home carries a row that only the correct at-rest key can open.
const AT_REST_KEY_PROOF_NAMESPACE: &str = "metadata";
const AT_REST_KEY_PROOF_KEY: &[u8] = b"__at_rest_key_proof";
const AT_REST_KEY_PROOF_VALUE: &[u8] = b"lastdb-at-rest-key-proof-v1";

/// Explicit diagnostics may also check the data namespace. Normal startup
/// does not read every data record before the socket can open.
const BOOT_DECRYPT_PROOF_HEAVY: &[&str] = &["main"];

/// Additional report-only namespace checks. Unreadable records are counted
/// but do not refuse startup. The parser cannot downgrade an enforcing check.
const BOOT_DECRYPT_PROOF_EXTRA_ENV: &str = "FOLD_BOOT_DECRYPT_PROOF_EXTRA";

/// A namespace to scan at boot, and whether an undecryptable row in it refuses
/// the boot or is merely reported.
struct ProofNamespace {
    name: String,
    enforcing: bool,
}

/// Parse [`BOOT_DECRYPT_PROOF_EXTRA_ENV`] into report-only namespace names,
/// dropping blanks, duplicates, and anything `existing` already covers.
///
/// Naming an already-enforced namespace is silently ignored rather than
/// honoured: honouring it would downgrade that namespace to report-only, which
/// would turn this diagnostic knob into a way to disarm the boot gate from the
/// environment.
fn parse_extra_proof_namespaces(raw: &str, existing: &[ProofNamespace]) -> Vec<String> {
    let mut seen: std::collections::HashSet<&str> =
        existing.iter().map(|p| p.name.as_str()).collect();
    let mut out = Vec::new();
    for candidate in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if seen.insert(candidate) {
            out.push(candidate.to_string());
        }
    }
    out
}

fn boot_decrypt_proof_namespaces() -> Vec<ProofNamespace> {
    let mut ns: Vec<ProofNamespace> = BOOT_DECRYPT_PROOF_STRUCTURAL
        .iter()
        .map(|n| ProofNamespace {
            name: (*n).to_string(),
            enforcing: true,
        })
        .collect();

    if env_flag::var_truthy("FOLD_BOOT_FULL_DECRYPT_PROOF") {
        ns.extend(BOOT_DECRYPT_PROOF_HEAVY.iter().map(|n| ProofNamespace {
            name: (*n).to_string(),
            enforcing: true,
        }));
        tracing::info!(
            target: "lastdbd::boot",
            "boot decrypt proof: full walk (FOLD_BOOT_FULL_DECRYPT_PROOF)"
        );
    } else {
        tracing::info!(
            target: "lastdbd::boot",
            "boot decrypt proof: structural namespaces only; set \
             FOLD_BOOT_FULL_DECRYPT_PROOF=1 for the full walk"
        );
    }

    let raw = std::env::var(BOOT_DECRYPT_PROOF_EXTRA_ENV).unwrap_or_default();
    for extra in parse_extra_proof_namespaces(&raw, &ns) {
        ns.push(ProofNamespace {
            name: extra,
            enforcing: false,
        });
    }

    let names: Vec<&str> = ns.iter().map(|p| p.name.as_str()).collect();
    tracing::info!(
        target: "lastdbd::boot",
        namespaces = ?names,
        "boot decrypt proof namespaces resolved"
    );
    ns
}

/// Refuse an unreadable enforcing namespace without changing any of its data.
///
/// Default scope is structural catalogs only — see
/// [`boot_decrypt_proof_namespaces`].
pub(super) async fn assert_critical_namespaces_decryptable(
    enc: &crate::storage::EncryptingNamespacedStore,
) -> FoldDbResult<()> {
    // Prove an existing sentinel before any namespace work. Establish a new
    // sentinel only after every enforcing namespace succeeds, so a wrong key
    // cannot create a replacement proof over unreadable data.
    let sentinel_proven = probe_at_rest_key_proof(enc).await?;

    for proof in boot_decrypt_proof_namespaces() {
        scan_proof_namespace(enc, &proof).await?;
    }

    if !sentinel_proven {
        assert_or_establish_at_rest_key_proof(enc).await?;
    }
    Ok(())
}

/// Read-only check of the at-rest key proof: `true` when the sentinel exists
/// and opens under this key, `false` when the home has no sentinel yet. A
/// sentinel that fails to decrypt or decrypts to the wrong bytes refuses the
/// boot here, the same way the establishing check does.
async fn probe_at_rest_key_proof(
    enc: &crate::storage::EncryptingNamespacedStore,
) -> FoldDbResult<bool> {
    use crate::storage::traits::NamespacedStore;

    if !enc.encrypts_namespace(AT_REST_KEY_PROOF_NAMESPACE) {
        return Err(FoldDbError::Config(format!(
            "at-rest key proof namespace '{AT_REST_KEY_PROOF_NAMESPACE}' is plaintext by \
             policy; the boot key proof would be meaningless there"
        )));
    }
    let kv = enc
        .open_namespace(AT_REST_KEY_PROOF_NAMESPACE)
        .await
        .map_err(|e| FoldDbError::Database(format!("at-rest key proof open: {e}")))?;
    match kv.get(AT_REST_KEY_PROOF_KEY).await {
        Ok(Some(value)) if value == AT_REST_KEY_PROOF_VALUE => Ok(true),
        Ok(Some(_)) => Err(FoldDbError::Database(
            "cannot decrypt existing store — at-rest key proof decrypted to unexpected \
             contents; refusing to start fresh (possible tampering or corruption)"
                .to_string(),
        )),
        Ok(None) => Ok(false),
        Err(e) => Err(FoldDbError::Database(format!(
            "cannot decrypt existing store — wrong master key or incompatible build; \
             refusing to start fresh (at-rest key proof unreadable: {e})"
        ))),
    }
}

/// Verify — or, on a home that has never carried one, establish — the at-rest
/// key proof. Returns `true` when an existing sentinel opened under this key,
/// `false` when this boot had to establish one (a young home: the key is not
/// proven by the sentinel, only by the scans).
///
/// The scan above can only catch a wrong key if some proof namespace happens to
/// hold rows. This makes it unconditional: one sealed sentinel row that the
/// correct key opens and a wrong key cannot.
///
/// Bootstrap honesty: a home written before this sentinel existed has no row to
/// check, so the first boot of such a home establishes one and is *not* itself
/// protected. That gap closes after one clean boot and is logged rather than
/// papered over.
async fn assert_or_establish_at_rest_key_proof(
    enc: &crate::storage::EncryptingNamespacedStore,
) -> FoldDbResult<bool> {
    use crate::storage::traits::NamespacedStore;

    // Guard against someone moving the sentinel into a namespace that policy
    // later makes plaintext — that would silently turn this proof off.
    if !enc.encrypts_namespace(AT_REST_KEY_PROOF_NAMESPACE) {
        return Err(FoldDbError::Config(format!(
            "at-rest key proof namespace '{AT_REST_KEY_PROOF_NAMESPACE}' is plaintext by \
             policy; the boot key proof would be meaningless there"
        )));
    }

    let kv = enc
        .open_namespace(AT_REST_KEY_PROOF_NAMESPACE)
        .await
        .map_err(|e| FoldDbError::Database(format!("at-rest key proof open: {e}")))?;

    match kv.get(AT_REST_KEY_PROOF_KEY).await {
        Ok(Some(value)) => {
            if value == AT_REST_KEY_PROOF_VALUE {
                return Ok(true);
            }
            // Decrypted to something else: not a key mismatch (AEAD would have
            // failed), so this is tamper or corruption. Refuse either way.
            Err(FoldDbError::Database(
                "cannot decrypt existing store — at-rest key proof decrypted to unexpected \
                 contents; refusing to start fresh (possible tampering or corruption)"
                    .to_string(),
            ))
        }
        Ok(None) => {
            kv.put(AT_REST_KEY_PROOF_KEY, AT_REST_KEY_PROOF_VALUE.to_vec())
                .await
                .map_err(|e| {
                    FoldDbError::Database(format!("failed to establish at-rest key proof: {e}"))
                })?;
            tracing::info!(
                target: "lastdbd::boot",
                "established at-rest key proof; subsequent boots verify the master key against it"
            );
            Ok(false)
        }
        // AEAD failure under a wrong key lands here. This is the case the whole
        // gate exists for (incident-lastdbd-0226-wrong-key-fresh-db).
        Err(e) => Err(FoldDbError::Database(format!(
            "cannot decrypt existing store — wrong master key or incompatible build; \
             refusing to start fresh (at-rest key proof unreadable: {e})"
        ))),
    }
}
