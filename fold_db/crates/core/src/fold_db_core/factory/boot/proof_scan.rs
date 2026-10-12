//! Boot decrypt proof: scan one namespace, drop undecryptable org rows,
//! refuse on undecryptable personal rows.

use super::{is_cloud_storage_prefixed_key, is_cold_group_too_large_text, ProofNamespace};
use crate::error::{FoldDbError, FoldDbResult};

/// Scan one proof namespace. Returns the count of org-scoped rows dropped.
pub(super) async fn scan_proof_namespace(
    enc: &crate::storage::EncryptingNamespacedStore,
    proof: &ProofNamespace,
    sentinel_proven: bool,
) -> FoldDbResult<usize> {
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
        return Ok(0);
    }
    // Namespace may not exist yet on a brand-new install — open creates it.
    let kv = enc
        .open_namespace(ns)
        .await
        .map_err(|e| FoldDbError::Database(format!("boot decrypt proof open '{ns}': {e}")))?;
    let started = std::time::Instant::now();
    let scan = match kv.scan_prefix_partition_undecryptable(b"").await {
        Ok(scan) => scan,
        Err(e) if sentinel_proven && is_cold_group_too_large_text(&e.to_string()) => {
            tracing::warn!(
                target: "lastdbd::boot",
                namespace = ns,
                error = %e,
                "boot decrypt proof scan skipped: a hash group exceeds the cold-load \
                 cap; the at-rest key is proven by the sentinel row. Reclaim the group \
                 (`lastdb db reclaim-keep-small-legacy` for the keep-small legacy row, \
                 or `lastdb db compact`) so the org-row sweep can run again"
            );
            return Ok(0);
        }
        Err(e) => {
            return Err(FoldDbError::Database(format!(
                "boot decrypt proof partition '{ns}': {e}"
            )))
        }
    };
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let decrypted = scan.rows.len();
    let undecryptable = scan.undecryptable.len();

    log_scan_outcome(ns, enforcing, decrypted, undecryptable, elapsed_ms);

    // Report-only candidate (FOLD_BOOT_DECRYPT_PROOF_EXTRA): the counts
    // above are the whole deliverable. Do not drop org rows and do not
    // refuse — an evaluation knob must not mutate or halt the boot.
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
        return Ok(0);
    }

    let mut org_dropped = 0usize;
    let mut personal_poison = Vec::new();
    for key in scan.undecryptable {
        let is_org = is_cloud_storage_prefixed_key(&key);
        if is_org {
            if let Err(e) = kv.delete(&key).await {
                return Err(FoldDbError::Database(format!(
                    "boot org-row drop failed in '{ns}': {e}"
                )));
            }
            org_dropped += 1;
        } else {
            personal_poison.push(key);
        }
    }

    if !personal_poison.is_empty() {
        let sample = personal_poison
            .first()
            .and_then(|k| std::str::from_utf8(k).ok())
            .unwrap_or("<binary key>");
        return Err(FoldDbError::Database(format!(
            "cannot decrypt existing store — wrong master key or incompatible build; \
             refusing to start fresh (namespace '{ns}', {} undecryptable personal row(s), \
             sample key '{sample}')",
            personal_poison.len()
        )));
    }
    Ok(org_dropped)
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
    // A namespace with no rows at all is not a weak proof, it is no proof —
    // the same failure mode as scanning a plaintext-by-policy namespace,
    // and the reason `metadata` could not fail before the sentinel existed
    // (it is not created until something writes to it). Say so out loud;
    // silence here is what let the gate look armed for months.
    if decrypted == 0 && undecryptable == 0 {
        tracing::warn!(
            target: "lastdbd::boot",
            namespace = ns,
            enforcing,
            elapsed_ms,
            "boot decrypt proof namespace is empty; it proved nothing about the \
             at-rest key this boot (the sentinel below still did)"
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
