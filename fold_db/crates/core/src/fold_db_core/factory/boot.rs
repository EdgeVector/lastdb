//! Boot-time decrypt integrity and embedding-cache path helpers.

use crate::error::{FoldDbError, FoldDbResult};

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

/// Structural namespaces whose undecryptable rows at boot mean the daemon must
/// refuse to serve rather than present a hollow view of the store.
///
/// These catalogs are small and prove the at-rest key opens real Mini homes
/// without walking multi‑GB data namespaces.
///
/// **Every entry must be encrypted at rest.** Until 2026-07-28 this list was
/// `["schemas", "schema_states", "metadata"]`, and the first two are on
/// [`LASTSTORE_PLAINTEXT_NAMESPACES`](crate::storage::LASTSTORE_PLAINTEXT_NAMESPACES)
/// — plaintext by policy, therefore never undecryptable under any key. On a
/// freshly booted LastStore home those two are also the *only* namespaces that
/// exist (`metadata` is not created until something writes to it), so the gate
/// scanned two namespaces that cannot fail plus one that was absent, and a
/// wrong identity key booted clean. `boot_proof_namespaces_are_encrypted`
/// pins this invariant.
///
/// Card `lastdb-widen-boot-decrypt-proof-namespaces` proposed widening this to
/// `node_config` and `node_identity`, gated on a real-data scan. That scan ran
/// on 2026-07-28 against a copy-on-write copy of the real primary home and the
/// answer was **do not widen** — so the list stays at `metadata`.
///
/// MEASURED on the real home (report-only, via
/// [`BOOT_DECRYPT_PROOF_EXTRA_ENV`]): `metadata` 9 rows / 0 undecryptable / 2ms,
/// `node_config` 3 rows / 0 undecryptable / 1ms, `node_identity` 1 row / 0
/// undecryptable / 0ms. Both candidates are clean, and cost was never the
/// objection. They were rejected because they are **dead**:
///
/// - `node_identity` stopped being the identity's source of truth on 2026-07-10
///   (card `fold-unify-node-identity`) — the canonical artifact is the
///   `identity.key` keyfile beside the data dir. Nothing in this tree names that
///   namespace at all.
/// - `node_config` is the same story one layer up. [`NodeConfigStore`] is still
///   constructed at boot and handed to `FoldDB`, but `FoldDB::config_store()`
///   has no production caller and `set_identity`/`get_identity` have none
///   either. The 3 rows on the real home are pre-keyfile residue.
///
/// A namespace nothing reads is a namespace no future migration or re-key will
/// remember to carry — and putting one on the boot-refusal path means the
/// primary can start refusing to boot over data no user or code path depends
/// on. That trade buys nothing here, because the sealed sentinel below already
/// gives unconditional wrong-key detection in O(1); scanning stale identity
/// residue adds no detection the sentinel lacks.
///
/// The dead namespaces are their own finding, filed separately rather than
/// fixed here (card `lastdb-retire-dead-node-config-node-identity-namespaces`).
///
/// [`NodeConfigStore`]: crate::storage::NodeConfigStore
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

/// Large data namespaces that used to be full-scanned at every boot
/// (incident-lastdbd-0226). `scan_prefix_partition_undecryptable` decrypts
/// **every** row and retains plaintext in RAM — on Tom's real home that is
/// tens of thousands of atoms/embeddings and multi‑GiB RSS before the socket
/// opens. Default boot therefore only proofs structural namespaces; set
/// `FOLD_BOOT_FULL_DECRYPT_PROOF=1` to restore the full walk (maintenance /
/// suspected wrong-key forensics).
///
/// `native_index` was here until 2026-07-28 and was dropped for the same reason
/// as the structural entries: it is plaintext by policy, so walking every
/// embedding row cost real RSS and could not report a single undecryptable row
/// under any key.
const BOOT_DECRYPT_PROOF_HEAVY: &[&str] = &["main"];

/// Comma-separated extra namespaces to scan at boot, **report-only**.
///
/// This is the tool that makes "no namespace joins
/// [`BOOT_DECRYPT_PROOF_STRUCTURAL`] without real-data evidence" an actual
/// procedure rather than an intention: point a node at a copy-on-write copy of
/// a real home, name the candidate here, and read its row and undecryptable
/// counts out of the boot log — without shipping the boot-refusal risk first
/// and discovering the answer on someone's primary.
///
/// Report-only is the whole point. An extra namespace that turns out to hold
/// undecryptable rows is logged loudly and boot continues; only the compiled-in
/// structural list can refuse. An env var that could brick a boot would be a
/// worse foot-gun than the widening it exists to de-risk.
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

/// Boot-time decrypt integrity gate with consented org-row drop.
///
/// - Org-scoped keys that fail AES-GCM under the personal provider are
///   **deleted** with a loud count (the 2026-07-13 consented drop of
///   org-shared test data after the org-crypto map strip).
/// - Any other undecryptable row still **refuses boot** (no phantom empty
///   serve over a broken personal store).
///
/// Default scope is structural catalogs only — see
/// [`boot_decrypt_proof_namespaces`].
pub(super) async fn assert_critical_namespaces_decryptable(
    enc: &crate::storage::EncryptingNamespacedStore,
) -> FoldDbResult<()> {
    use crate::storage::traits::NamespacedStore;

    // The sentinel goes first. It is the O(1) key proof; the scans below are
    // the org-row sweep plus a belt-and-braces walk. Ordering matters for one
    // failure only: a scan that meets a hash group over the cold-load cap
    // (`ColdGroupTooLarge`). With the sentinel already proven, that scan is
    // skipped with a warning; without it, the refusal stands, because a
    // young home has nothing else to prove the key with. On 2026-09-22 the
    // candidate for the flush-storm fix could not boot the primary's copy
    // because this walk loaded `metadata/0/g/025` (1.3 GB by then) and the
    // cap said no — the cap was right, the walk was in the wrong place.
    // Read-only here: establishing a sentinel before the scans would, on a
    // pre-sentinel home opened with a wrong key, seal a wrong-key sentinel
    // that the next correct-key boot cannot open. Establish only after the
    // scans, exactly as before.
    let sentinel_proven = probe_at_rest_key_proof(enc).await?;

    let mut org_dropped = 0usize;
    for proof in boot_decrypt_proof_namespaces() {
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
            continue;
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
                continue;
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
            continue;
        }

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
    }

    if org_dropped > 0 {
        tracing::warn!(
            target: "lastdbd::boot",
            dropped = org_dropped,
            "boot dropped {org_dropped} undecryptable org-scoped at-rest row(s) \
             (consented drop after org-crypto strip); personal store otherwise clean"
        );
    }

    if !sentinel_proven {
        assert_or_establish_at_rest_key_proof(enc).await?;
    }
    Ok(())
}

/// The store's refusal to load one oversized cold hash group. Matched on text
/// because the storage error crosses the `NamespacedStore` trait as a string.
fn is_cold_group_too_large_text(msg: &str) -> bool {
    msg.contains("cold group too large")
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

#[cfg(feature = "cloud-sync")]
fn is_cloud_storage_prefixed_key(key: &[u8]) -> bool {
    std::str::from_utf8(key)
        .ok()
        .and_then(crate::sync::storage_prefix_for_key)
        .is_some()
}

#[cfg(not(feature = "cloud-sync"))]
fn is_cloud_storage_prefixed_key(_key: &[u8]) -> bool {
    false
}
