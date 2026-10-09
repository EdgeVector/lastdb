use super::error::{SyncError, SyncResult};
use crate::crypto::CryptoProvider;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;

/// A single KvStore operation recorded for sync.
///
/// Each entry captures one write operation (put, delete, batch_put, batch_delete)
/// along with its sequence number for ordered replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Timestamp-based ID (nanos since epoch). Used as the R2 object key.
    /// Not sequential — just unique enough to avoid collisions.
    pub seq: u64,
    /// Client timestamp (millis since epoch). Used for LWW conflict resolution.
    pub timestamp_ms: u64,
    /// Device ID that produced this entry.
    pub device_id: String,
    /// The operation.
    pub op: LogOp,
}

#[derive(Debug, Clone, Serialize)]
pub enum LogOp {
    Put {
        namespace: String,
        /// Base64-encoded key (keys are arbitrary bytes).
        key: String,
        /// Base64-encoded value.
        value: String,
    },
    Delete {
        namespace: String,
        key: String,
    },
    BatchPut {
        namespace: String,
        /// Vec of (base64 key, base64 value).
        items: Vec<(String, String)>,
    },
    BatchDelete {
        namespace: String,
        keys: Vec<String>,
    },
    /// One top-level logical mutation commit, represented by the ordered,
    /// replayable store changes it produced. This keeps replay byte-compatible
    /// with the proven KV apply path while making capture cardinality one log
    /// record per commit instead of one record per physical store call.
    ///
    /// **Replay-only** for bags already written to disk. Live capture emits
    /// [`Self::MutationIntent`] instead of a physical-row bag.
    LogicalCommit {
        changes: Vec<LogicalChange>,
    },
    /// Live capture format: the LastDB mutation intent (schema + key +
    /// fields), not the physical KV fanout that persist produced.
    MutationIntent {
        mutations: Vec<MutationEnvelope>,
    },
    /// Leftover physical write (catalog / drain) outside MutationManager.
    /// Items are `(base64 key, base64 sha256 of the body)` — not the body.
    /// Replay skips apply; the local write already committed and SOT is
    /// snapshot-backed. Old binaries map this tag to [`Self::Unknown`].
    PhysicalDigest {
        namespace: String,
        items: Vec<(String, String)>,
    },
    /// Forward-compat: an externally tagged name this binary does not know.
    /// Replay maps this to [`SyncError::PoisonEntry`] (skip + advance).
    /// Must not be `#[serde(other)]` — that cannot consume a struct payload.
    Unknown {
        tag: String,
    },
}

#[derive(Deserialize)]
struct LogOpPutBody {
    namespace: String,
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct LogOpDeleteBody {
    namespace: String,
    key: String,
}

#[derive(Deserialize)]
struct LogOpBatchPutBody {
    items: Vec<(String, String)>,
    namespace: String,
}

#[derive(Deserialize)]
struct LogOpBatchDeleteBody {
    keys: Vec<String>,
    namespace: String,
}

#[derive(Deserialize)]
struct LogOpLogicalCommitBody {
    changes: Vec<LogicalChange>,
}

#[derive(Deserialize)]
struct LogOpMutationIntentBody {
    mutations: Vec<MutationEnvelope>,
}

#[derive(Deserialize)]
struct LogOpPhysicalDigestBody {
    namespace: String,
    items: Vec<(String, String)>,
}

impl<'de> Deserialize<'de> for LogOp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(LogOpVisitor {
            accept_mutation_intent: true,
        })
    }
}

struct LogOpVisitor {
    accept_mutation_intent: bool,
}

impl<'de> Visitor<'de> for LogOpVisitor {
    type Value = LogOp;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an externally tagged LogOp")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<LogOp, A::Error> {
        let tag: String = map
            .next_key()?
            .ok_or_else(|| de::Error::custom("empty LogOp object"))?;
        let op = match tag.as_str() {
            "Put" => {
                let body: LogOpPutBody = map.next_value()?;
                LogOp::Put {
                    namespace: body.namespace,
                    key: body.key,
                    value: body.value,
                }
            }
            "Delete" => {
                let body: LogOpDeleteBody = map.next_value()?;
                LogOp::Delete {
                    namespace: body.namespace,
                    key: body.key,
                }
            }
            "BatchPut" => {
                let body: LogOpBatchPutBody = map.next_value()?;
                LogOp::BatchPut {
                    namespace: body.namespace,
                    items: body.items,
                }
            }
            "BatchDelete" => {
                let body: LogOpBatchDeleteBody = map.next_value()?;
                LogOp::BatchDelete {
                    namespace: body.namespace,
                    keys: body.keys,
                }
            }
            "LogicalCommit" => {
                let body: LogOpLogicalCommitBody = map.next_value()?;
                LogOp::LogicalCommit {
                    changes: body.changes,
                }
            }
            "MutationIntent" if self.accept_mutation_intent => {
                let body: LogOpMutationIntentBody = map.next_value()?;
                LogOp::MutationIntent {
                    mutations: body.mutations,
                }
            }
            "PhysicalDigest" => {
                let body: LogOpPhysicalDigestBody = map.next_value()?;
                LogOp::PhysicalDigest {
                    namespace: body.namespace,
                    items: body.items,
                }
            }
            _ => {
                let _: IgnoredAny = map.next_value()?;
                LogOp::Unknown { tag }
            }
        };
        if map.next_key::<IgnoredAny>()?.is_some() {
            return Err(de::Error::custom(
                "LogOp must be a single externally tagged variant",
            ));
        }
        Ok(op)
    }
}

/// One ordered store change inside a [`LogOp::LogicalCommit`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalChange {
    pub namespace: String,
    /// Base64-encoded key (keys are arbitrary bytes).
    pub key: String,
    /// Base64-encoded value for a put; `None` is a delete.
    pub value: Option<String>,
}

/// Logical LastDB mutation intent stored in the mutation log.
///
/// Field values are the caller's JSON, not serialized molecule/atom/tip
/// rows. Replay re-runs this intent with capture suppressed so derived
/// planes rebuild locally.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MutationEnvelope {
    pub schema_name: String,
    /// Lowercase `create` / `update` / `delete` / `purge` — matches
    /// [`crate::schema::types::operations::MutationType`]'s deserializer.
    pub mutation_type: String,
    pub key_value: crate::schema::types::key_value::KeyValue,
    /// Inline field bodies that make the logical mutation self-contained.
    ///
    /// Reference-only rows from builds before 0.23.4 remain readable through
    /// `field_atom_uuids`, but new durable rows keep these bodies. A queue
    /// record must not depend on a later read from a reclaimable storage plane.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub fields_and_values: std::collections::HashMap<String, serde_json::Value>,
    pub pub_key: String,
    /// Origin last-writer-wins clock (nanos). Replay must stamp this, not now.
    #[serde(default)]
    pub written_at: u64,
    /// Origin writer/device identity for LWW tie-break.
    #[serde(default)]
    pub writer_id: String,
    /// Durable per-device logical author counter. Legacy envelopes use zero.
    #[serde(
        default,
        skip_serializing_if = "crate::schema::types::mutation::is_zero_u64"
    )]
    pub logical_counter: u64,
    /// Mutation-level signature over content, id, and the complete author clock.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub author_clock_signature: String,
    /// Zero for legacy envelopes; two for the signed author-clock scheme.
    #[serde(
        default,
        skip_serializing_if = "crate::schema::types::mutation::is_zero_u8"
    )]
    pub author_clock_signature_version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<crate::atom::provenance::Provenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_version: Option<u64>,
    /// Writer's mutation id. Replay must reuse it, not mint a new UUID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mutation_uuid: String,
    /// Per-key association metadata (`PerKeyRecord.meta`). Not part of the
    /// content-addressed atom UUID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
    /// Content-addressed atom UUID the writer will persist for each field.
    /// Replay persist must produce the same ids (same schema+content hash).
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub field_atom_uuids: std::collections::HashMap<String, String>,
    /// A concrete aggregate-member replacement attached to this source
    /// mutation. The derived total is deliberately absent from the log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_set: Option<crate::schema::types::AggregateSet>,
}

impl MutationEnvelope {
    /// Keep the logical mutation self-contained for durable queues.
    ///
    /// This compatibility method used to remove every inline body when atom
    /// identities were present. The removal made an acknowledged write depend
    /// on later atom-store reads, so GC or a persistence hole could make the
    /// record impossible to seal. Call sites may keep invoking the method
    /// while old reference-only rows drain; new rows retain their bodies.
    pub fn strip_sot_field_values(&mut self) {
        // Intentionally empty. See the method contract above.
    }
}

impl LogOp {
    /// Preserve self-contained mutation-intent payloads for durable queues.
    pub fn strip_sot_field_values(&mut self) {
        if let Self::MutationIntent { mutations } = self {
            for mutation in mutations {
                mutation.strip_sot_field_values();
            }
        }
    }
}

/// Serialized + encrypted log entry with integrity hash.
///
/// Wire format:
/// ```text
/// [sha256: 32 bytes] [encrypted_payload: variable]
/// ```
///
/// The SHA-256 is computed over the plaintext JSON before encryption,
/// allowing the reader to verify integrity after decryption.
pub struct SealedLogEntry {
    pub bytes: Vec<u8>,
}

const HASH_SIZE: usize = 32;

impl LogEntry {
    /// Serialized JSON byte size of this entry — the dominant component of the
    /// bytes it costs in the cloud log (sealing adds only the 32-byte hash plus
    /// a small constant AEAD overhead). Used by the sync engine's size-based
    /// compaction trigger to estimate accumulated log growth without re-sealing.
    /// Returns 0 if serialization fails (it won't for a well-formed entry).
    pub fn serialized_len(&self) -> usize {
        serde_json::to_vec(self).map_or(0, |v| v.len())
    }

    /// Serialize, hash, encrypt.
    pub async fn seal(&self, crypto: &Arc<dyn CryptoProvider>) -> SyncResult<SealedLogEntry> {
        let json = serde_json::to_vec(self)?;

        let mut hasher = Sha256::new();
        hasher.update(&json);
        let hash: [u8; 32] = hasher.finalize().into();

        let mut plaintext = Vec::with_capacity(HASH_SIZE + json.len());
        plaintext.extend_from_slice(&hash);
        plaintext.extend_from_slice(&json);

        let ciphertext = crypto.encrypt(&plaintext).await?;

        Ok(SealedLogEntry { bytes: ciphertext })
    }

    /// Decrypt, verify hash, deserialize.
    pub async fn unseal(
        sealed: &[u8],
        crypto: &Arc<dyn CryptoProvider>,
        target: &str,
        seq: u64,
    ) -> SyncResult<Self> {
        let plaintext = match crypto.decrypt(sealed).await {
            Ok(plaintext) => plaintext,
            // Forward-compat: a version byte this build doesn't understand is a
            // format skip, not a key failure. Surface it as a TYPED variant so
            // replay can distinguish "skip-and-advance" from "wrong key / abort"
            // without matching on decrypt error text.
            Err(crate::crypto::CryptoError::UnsupportedVersion(version)) => {
                return Err(SyncError::UnsupportedEnvelope { version });
            }
            Err(e) => {
                return Err(SyncError::Crypto(format!(
                    "failed to decrypt log entry: {e}"
                )));
            }
        };

        if plaintext.len() < HASH_SIZE {
            return Err(SyncError::CorruptEntry {
                target: target.to_string(),
                seq,
                reason: "plaintext too short for hash".to_string(),
            });
        }

        let (stored_hash, json_bytes) = plaintext.split_at(HASH_SIZE);

        let mut hasher = Sha256::new();
        hasher.update(json_bytes);
        let computed_hash: [u8; 32] = hasher.finalize().into();

        if stored_hash != computed_hash.as_slice() {
            return Err(SyncError::CorruptEntry {
                target: target.to_string(),
                seq,
                reason: "hash mismatch — data corrupted".to_string(),
            });
        }

        let entry: Self = serde_json::from_slice(json_bytes)?;
        Ok(entry)
    }
}

impl LogOp {
    /// Returns the namespace this operation targets.
    pub fn namespace(&self) -> &str {
        match self {
            Self::Put { namespace, .. }
            | Self::Delete { namespace, .. }
            | Self::BatchPut { namespace, .. }
            | Self::BatchDelete { namespace, .. }
            | Self::PhysicalDigest { namespace, .. } => namespace,
            Self::LogicalCommit { .. } => "logical_commit",
            Self::MutationIntent { .. } => "mutation_intent",
            Self::Unknown { .. } => "unknown",
        }
    }

    /// Short human-readable description: op kind, namespace, item count.
    /// Used by sync replay instrumentation so every replayed entry shows up
    /// in the log with enough detail to diagnose drops (alpha BLOCKER 4439b).
    pub fn describe(&self) -> String {
        match self {
            Self::Put { namespace, .. } => format!("Put ns={namespace}"),
            Self::Delete { namespace, .. } => format!("Delete ns={namespace}"),
            Self::BatchPut { namespace, items } => {
                format!("BatchPut ns={} items={}", namespace, items.len())
            }
            Self::BatchDelete { namespace, keys } => {
                format!("BatchDelete ns={} keys={}", namespace, keys.len())
            }
            Self::LogicalCommit { changes } => {
                format!("LogicalCommit changes={}", changes.len())
            }
            Self::MutationIntent { mutations } => {
                format!("MutationIntent mutations={}", mutations.len())
            }
            Self::PhysicalDigest { namespace, items } => {
                format!("PhysicalDigest ns={} items={}", namespace, items.len())
            }
            Self::Unknown { tag } => format!("Unknown tag={tag}"),
        }
    }

    /// Encode key bytes to base64 for storage in the log entry.
    pub fn encode_bytes(bytes: &[u8]) -> String {
        BASE64.encode(bytes)
    }

    /// Decode base64 key/value framing. Failures are [`SyncError::Serialization`].
    ///
    /// Prefer [`Self::decode_bytes_for_replay`] on the post-decrypt apply path:
    /// invalid base64 inside an already-unsealed `LogEntry` is deterministic
    /// poison and must not wedge the download cursor forever.
    pub fn decode_bytes(encoded: &str) -> SyncResult<Vec<u8>> {
        BASE64
            .decode(encoded)
            .map_err(|e| SyncError::Serialization(format!("invalid base64: {e}")))
    }

    /// Decode base64 key/value framing during **replay** after a successful
    /// envelope unseal. Invalid base64 cannot become valid by retrying, so map
    /// it to [`SyncError::PoisonEntry`] (skip + advance) rather than
    /// [`SyncError::Serialization`] (abort + pin cursor).
    pub fn decode_bytes_for_replay(
        namespace: &str,
        field: &str,
        encoded: &str,
    ) -> SyncResult<Vec<u8>> {
        match Self::decode_bytes(encoded) {
            Ok(bytes) => Ok(bytes),
            Err(SyncError::Serialization(msg)) => Err(SyncError::PoisonEntry {
                namespace: namespace.to_string(),
                reason: format!("invalid base64 in log entry {field}: {msg}"),
            }),
            Err(other) => Err(other),
        }
    }
}
