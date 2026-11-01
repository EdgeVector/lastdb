use super::identity::Identity;
use super::model::Head;
use super::*;

pub(in crate::storage::disk_pack) struct Capture {
    pub(super) identity: Identity,
}

/// The physical home locks exclude another creator. Bootstrap precedes root selection.
pub(in crate::storage::disk_pack) fn open(
    v2: &physical::V2Home,
    policy: &laststore::space::SpacePolicy,
    allow_create: bool,
) -> StorageResult<Option<Capture>> {
    let head = v2
        .home
        .read(super::super::budget())
        .get(COLLECTION, &head_key())
        .map_err(map)?;
    let identity = Identity::load(v2)?;
    match (identity, head) {
        (Some(mut identity), Some(bytes)) => {
            let head: Head = decode(&bytes)?;
            head.validate(&identity)?;
            if !identity.ready {
                if !allow_create || head.position.sequence != 0 {
                    return Err(invalid(
                        "logical capture pending enrollment needs a writable head-zero open",
                    ));
                }
                let reservation =
                    laststore::space::reserve_space(v2.home.path(), policy, 4096).map_err(map)?;
                identity.mark_ready(v2)?;
                drop(reservation);
            } else if allow_create {
                // Complete an uncertain ready-byte barrier before this runtime can ack.
                identity.sync_control(v2)?;
            }
            Ok(Some(Capture { identity }))
        }
        (None, Some(_)) => Err(invalid(
            "logical capture head has no device control; no signer is recreated",
        )),
        (Some(identity), None) if !identity.ready && allow_create => {
            enroll(v2, policy, identity, false)
        }
        (Some(_), None) => Err(invalid(
            "logical capture control has no durable head; no sequence is reset",
        )),
        (None, None) if allow_create && enabled_from_env()? => {
            enroll(v2, policy, Identity::fresh(v2), true)
        }
        (None, None) => Ok(None),
    }
}

fn enroll(
    v2: &physical::V2Home,
    policy: &laststore::space::SpacePolicy,
    mut identity: Identity,
    fresh: bool,
) -> StorageResult<Option<Capture>> {
    let operations = vec![
        HomeMutation {
            collection: COLLECTION.into(),
            key: head_key(),
            body: Some(encode(&Head::initial(&identity)?)?),
        },
        HomeMutation {
            collection: REGISTRY.into(),
            key: format!("{NAMESPACES}{COLLECTION}"),
            body: Some(vec![1]),
        },
    ];
    let reservation =
        laststore::space::reserve_space(v2.home.path(), policy, bootstrap_space(v2, &operations)?)
            .map_err(map)?;
    if fresh {
        identity.save(v2)?;
    } else {
        identity.sync_control(v2)?;
    }
    v2.home.commit(operations).map_err(map)?;
    identity.mark_ready(v2)?;
    drop(reservation);
    Ok(Some(Capture { identity }))
}

fn bootstrap_space(v2: &physical::V2Home, operations: &[HomeMutation]) -> StorageResult<u64> {
    let logical = transaction::batch_bytes(operations, physical::V2_BATCH_OPERATIONS)?;
    if operations.len() != 2 || logical > 4096 {
        return Err(invalid(
            "logical capture bootstrap exceeds its physical space bound",
        ));
    }
    // CURRENT's uncompressed representation fits inside the held manifest's
    // RunFile objects, boundary-key capacities, lists and paths. Four KiB adds
    // the new RunMeta, frame header/tag and every possible varint-width growth.
    // Two bootstrap records under 4 KiB add at most two data/index blocks, one
    // top block, their frame headers/tags and a footer. Add a separate 8 KiB
    // allowance for the fixed control stage and final file. Frames never expand
    // their plaintext payloads. The ready-byte update stays within those files.
    let current = v2.home.read(super::super::budget()).retained_bytes();
    current
        .checked_add(4096 + 64 * 1024 + 8192)
        .ok_or_else(|| invalid("logical capture bootstrap space bound overflow"))
}

fn enabled_from_env() -> StorageResult<bool> {
    match std::env::var("LASTDB_LOGICAL_CAPTURE") {
        Ok(value) if value == "1" => Ok(true),
        Ok(value) if value == "0" || value.is_empty() => Ok(false),
        Err(std::env::VarError::NotPresent) => Ok(false),
        _ => Err(StorageError::ConfigurationError(
            "LASTDB_LOGICAL_CAPTURE must be 0 or 1".into(),
        )),
    }
}

impl PackRuntime {
    pub fn logical_capture_enabled(&self) -> bool {
        self.logical_capture.is_some()
    }

    /// Allocate no sequence outside the caller's complete private native transaction.
    pub(crate) async fn stage_logical_capture(
        self: &Arc<Self>,
        mutation_ids: &[String],
        outcomes: Outcomes,
    ) -> StorageResult<Position> {
        let capture = self
            .logical_capture
            .as_ref()
            .ok_or_else(|| invalid("logical capture is disabled"))?;
        if self.record_home() != matches!(outcomes, Outcomes::Records(_)) {
            return Err(invalid(
                "a format 7 home captures records; another home captures field tips",
            ));
        }
        let pending = transaction::PendingBatch::current(self).ok_or_else(|| {
            invalid("logical capture requires the data's active native transaction")
        })?;
        let context = read::ReadContext::current(self).await?;
        let runtime = Arc::clone(self);
        let source = Arc::clone(&pending);
        let bytes =
            run_blocking(move || runtime.raw_get(&context, COLLECTION, &head_key(), Some(&source)))
                .await?
                .ok_or_else(|| invalid("logical capture head disappeared"))?;
        let mut head: Head = decode(&bytes)?;
        head.validate(&capture.identity)?;
        let entry = CaptureEvent::authored(&capture.identity, &head, mutation_ids, outcomes)?;
        let body = encode(&entry)?;
        let event_key = key("e", entry.sequence);
        let metadata_key = key("m", entry.sequence);
        let metadata = encode(&entry.metadata(body.len()))?;
        let charge = body
            .len()
            .saturating_add(metadata.len())
            .saturating_add(event_key.len())
            .saturating_add(metadata_key.len()) as u64;
        head.encoded_backlog_bytes = head
            .encoded_backlog_bytes
            .checked_add(charge)
            .filter(|bytes| *bytes <= BACKLOG_CAP_BYTES)
            .ok_or_else(|| StorageError::WriteResourceLimit {
                detail:
                    "logical capture reached its 64 MiB encoded backlog cap; complete write refused"
                        .into(),
            })?;
        head.position = entry.position();
        pending.stage(vec![
            HomeMutation {
                collection: COLLECTION.into(),
                key: event_key,
                body: Some(body),
            },
            HomeMutation {
                collection: COLLECTION.into(),
                key: metadata_key,
                body: Some(metadata),
            },
            HomeMutation {
                collection: COLLECTION.into(),
                key: head_key(),
                body: Some(encode(&head)?),
            },
        ])?;
        transaction::mark_logical_capture(self)?;
        Ok(head.position)
    }
}
