//! Check one namespace without changing its records.

use super::ProofNamespace;
use crate::error::{FoldDbError, FoldDbResult};

/// Enforcing namespaces refuse unreadable records; diagnostics only report them.
pub(super) async fn scan_proof_namespace(
    enc: &crate::storage::EncryptingNamespacedStore,
    proof: &ProofNamespace,
) -> FoldDbResult<()> {
    use crate::storage::traits::NamespacedStore;

    let ns = proof.name.as_str();
    let enforcing = proof.enforcing;
    // A plaintext-by-policy namespace holds no envelopes and so reports zero
    // undecryptable rows under every key. Scanning it is not a weak proof,
    // it is no proof — skip it loudly rather than let it pad the loop.
    if !enc.encrypts_namespace(ns) {
        tracing::warn!(
            target: "lastdbd::boot",
            namespace = ns,
            "boot decrypt proof namespace is plaintext by policy and proves \
             nothing about the at-rest key; skipping"
        );
        return Ok(());
    }
    // Namespace may not exist yet on a brand-new install — open creates it.
    let kv = enc
        .open_namespace(ns)
        .await
        .map_err(|e| FoldDbError::Database(format!("boot decrypt proof open '{ns}': {e}")))?;
    let started = std::time::Instant::now();
    let scan = kv
        .scan_prefix_partition_undecryptable(b"")
        .await
        .map_err(|e| FoldDbError::Database(format!("boot decrypt proof partition '{ns}': {e}")))?;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let decrypted = scan.rows.len();
    let undecryptable = scan.undecryptable.len();

    log_scan_outcome(ns, enforcing, decrypted, undecryptable, elapsed_ms);

    // Report-only candidate (FOLD_BOOT_DECRYPT_PROOF_EXTRA): the counts
    // above are the whole deliverable. Unreadable records do not authorize
    // deletion or refusal in a report-only namespace.
    if !enforcing {
        if undecryptable > 0 {
            tracing::warn!(
                target: "lastdbd::boot",
                namespace = ns,
                undecryptable,
                "report-only boot decrypt proof namespace holds undecryptable rows; \
                 it is NOT a candidate for the enforcing list"
            );
        }
        return Ok(());
    }

    if undecryptable > 0 {
        return Err(FoldDbError::Database(format!(
            "cannot decrypt existing store — wrong master key or incompatible build; \
             refusing to start fresh (namespace '{ns}', {undecryptable} undecryptable record(s))"
        )));
    }
    Ok(())
}

/// Log what one proof namespace scan found; an empty namespace is called out
/// because it proves nothing about the key.
fn log_scan_outcome(
    ns: &str,
    enforcing: bool,
    decrypted: usize,
    undecryptable: usize,
    elapsed_ms: u64,
) {
    // An empty namespace cannot prove its encryption key. The sentinel check
    // independently verifies an existing home or establishes a new proof.
    if decrypted == 0 && undecryptable == 0 {
        tracing::warn!(
            target: "lastdbd::boot",
            namespace = ns,
            enforcing,
            elapsed_ms,
            "boot decrypt proof namespace is empty; it proved nothing about the \
             at-rest key this boot (the sentinel is checked separately)"
        );
    } else {
        tracing::info!(
            target: "lastdbd::boot",
            namespace = ns,
            enforcing,
            rows = decrypted,
            undecryptable,
            elapsed_ms,
            "boot decrypt proof namespace scanned"
        );
    }
}
