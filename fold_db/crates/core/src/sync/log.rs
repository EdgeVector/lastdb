use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

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
    /// Product mutations emit [`Self::MutationIntent`]. Ordered schema-authority
    /// mixed KV writes use this existing replay format.
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
    /// Zero for unsigned input; see `MUTATION_AUTHOR_SIGNATURE_VERSION` for signed input.
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
    /// Signed TTL erasure boundary. Absence preserves legacy unconditional erase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub erase_tips_before_nanos: Option<u64>,
}
