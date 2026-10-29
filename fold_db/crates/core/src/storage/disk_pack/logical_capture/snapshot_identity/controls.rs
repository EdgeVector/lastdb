//! Only a bounded native control batch changes; donor data and old logs remain.

use super::model::{Preparation, PreparedHead, PreparedPeer, PreparedRoster};
use super::*;
use ed25519_dalek::Signature;

pub(super) fn mutations(
    source: &SourceIdentity,
    candidate: &SnapshotInclusionMap,
) -> StorageResult<Vec<HomeMutation>> {
    let own = source
        .map
        .positions
        .iter()
        .find(|entry| entry.origin == source.map.producer)
        .ok_or_else(|| invalid("snapshot identity local head is absent"))?;
    let (birth_main, birth_cas) = source.identity.birth_homes();
    let head = PreparedHead {
        version: 2,
        origin: source.map.producer.clone(),
        birth_main,
        birth_cas,
        anchor_sha256: own.anchor_sha256.clone(),
        base: SnapshotIdentityPosition::of(own),
        position: SnapshotIdentityPosition::of(own),
        encoded_backlog_bytes: DecimalU64::new(0),
        authored_key_version: 2,
    };
    let mut roster = PreparedRoster {
        version: 2,
        origins: Vec::new(),
        receipt_count: DecimalU64::new(0),
    };
    let mut operations = vec![operation(head_key(), &head)?];
    for entry in candidate
        .positions
        .iter()
        .filter(|entry| entry.origin != source.map.producer)
    {
        roster.origins.push(entry.origin.clone());
        operations.push(operation(
            peer_key(&entry.origin),
            &PreparedPeer {
                version: 2,
                origin: entry.origin.clone(),
                anchor_sha256: entry.anchor_sha256.clone(),
                base: SnapshotIdentityPosition::of(entry),
                position: SnapshotIdentityPosition::of(entry),
            },
        )?);
    }
    let mut preparation = Preparation {
        version: 1,
        source_root: source.map.snapshot.clone(),
        candidate_root: candidate.snapshot.clone(),
        source_map_sha256: source.map.sha256.clone(),
        candidate_map_sha256: candidate.sha256.clone(),
        local_origin: source.map.producer.clone(),
        signature: String::new(),
    };
    preparation.signature = source.identity.sign(&preparation.signing_bytes()?);
    operations.push(operation(native_routing::encode(b"peers"), &roster)?);
    operations.push(operation(preparation_key(), &preparation)?);
    Ok(operations)
}

pub(super) fn verify_preparation(
    source: &SourceIdentity,
    candidate: &SnapshotInclusionMap,
    main: &mut RunRead,
) -> StorageResult<()> {
    let bytes = main
        .get_bounded(COLLECTION, &preparation_key(), peer::CAPTURE_RECORD_BYTES)
        .map_err(map)?
        .ok_or_else(|| invalid("snapshot identity preparation record is absent"))?;
    let preparation: Preparation = decode(&bytes)?;
    if preparation.version != 1
        || preparation.source_root != source.map.snapshot
        || preparation.candidate_root != candidate.snapshot
        || preparation.source_map_sha256 != source.map.sha256
        || preparation.candidate_map_sha256 != candidate.sha256
        || preparation.local_origin != source.map.producer
    {
        return Err(invalid("snapshot identity preparation provenance differs"));
    }
    verify::origin_key(&preparation.local_origin)?
        .verify_strict(
            &preparation.signing_bytes()?,
            &Signature::from_bytes(&unhex::<64>(&preparation.signature)?),
        )
        .map_err(|_| invalid("snapshot identity preparation signature differs"))
}

pub(super) fn verify_bases(
    source: &SourceIdentity,
    candidate: &SnapshotInclusionMap,
    prepared: &SnapshotInclusionMap,
) -> StorageResult<()> {
    if prepared.version != 2
        || prepared.producer != source.map.producer
        || prepared.positions.len() != candidate.positions.len()
    {
        return Err(invalid("prepared snapshot device set differs"));
    }
    for (actual, original) in prepared.positions.iter().zip(&candidate.positions) {
        if actual.origin != original.origin
            || actual.anchor_sha256 != original.anchor_sha256
            || actual.sequence != original.sequence
            || actual.sha256 != original.sha256
            || actual.base.as_ref() != Some(&SnapshotIdentityPosition::of(original))
        {
            return Err(invalid(
                "prepared snapshot cursor base differs from candidate coverage",
            ));
        }
    }
    Ok(())
}

pub(super) fn captured_positions(
    main: &mut RunRead,
    bytes: &[u8],
    roster: Option<&[u8]>,
) -> StorageResult<(String, Vec<SnapshotDevicePosition>)> {
    let head: PreparedHead = decode(bytes)?;
    verify::origin_key(&head.origin)?;
    let anchor = identity::anchor_for(&head.origin, head.birth_main, head.birth_cas)?;
    if head.version != 2
        || head.birth_main.is_nil()
        || head.birth_cas.is_nil()
        || head.anchor_sha256 != anchor
        || head.authored_key_version != 2
        || head.encoded_backlog_bytes.get() > BACKLOG_CAP_BYTES
    {
        return Err(invalid("prepared snapshot original local identity differs"));
    }
    let own = device_position(
        head.origin.clone(),
        head.anchor_sha256,
        head.base,
        head.position,
    )?;
    let roster: PreparedRoster =
        decode(roster.ok_or_else(|| invalid("prepared peer roster is absent"))?)?;
    if roster.version != 2
        || roster.origins.len() > 32
        || roster.origins.windows(2).any(|pair| pair[0] >= pair[1])
        || roster.origins.iter().any(|origin| origin == &head.origin)
        || roster.receipt_count.get() > 65_536
    {
        return Err(invalid("prepared snapshot peer roster differs"));
    }
    let coordinates = roster
        .origins
        .iter()
        .map(|origin| (COLLECTION.to_owned(), peer_key(origin)))
        .collect::<Vec<_>>();
    let values = main
        .get_many_bounded(&coordinates, peer::CAPTURE_RECORD_BYTES)
        .map_err(map)?;
    if values.len() != roster.origins.len() {
        return Err(invalid("prepared peer cursor result count differs"));
    }
    let mut positions = Vec::with_capacity(roster.origins.len() + 1);
    let mut receipts = 0u64;
    for (origin, bytes) in roster.origins.iter().zip(values) {
        let bytes = bytes
            .map_err(map)?
            .ok_or_else(|| invalid("prepared peer cursor is absent"))?;
        let peer: PreparedPeer = decode(&bytes)?;
        if peer.version != 2 || peer.origin != *origin {
            return Err(invalid("prepared peer original identity differs"));
        }
        receipts = receipts
            .checked_add(
                peer.position
                    .sequence
                    .get()
                    .saturating_sub(peer.base.sequence.get()),
            )
            .ok_or_else(|| invalid("prepared peer receipt count overflows"))?;
        positions.push(device_position(
            peer.origin,
            peer.anchor_sha256,
            peer.base,
            peer.position,
        )?);
    }
    if receipts != roster.receipt_count.get() {
        return Err(invalid("prepared peer retained receipt count differs"));
    }
    positions.push(own);
    positions.sort_by(|a, b| a.origin.cmp(&b.origin));
    Ok((head.origin, positions))
}

fn device_position(
    origin: String,
    anchor_sha256: String,
    base: SnapshotIdentityPosition,
    position: SnapshotIdentityPosition,
) -> StorageResult<SnapshotDevicePosition> {
    verify::origin_key(&origin)?;
    unhex::<32>(&anchor_sha256)?;
    unhex::<32>(&position.sha256)?;
    let entry = SnapshotDevicePosition {
        origin,
        anchor_sha256,
        sequence: position.sequence,
        sha256: position.sha256,
        base: Some(base),
    };
    entry
        .base
        .as_ref()
        .expect("base supplied")
        .validate_for(&entry)?;
    Ok(entry)
}

impl Preparation {
    fn signing_bytes(&self) -> StorageResult<Vec<u8>> {
        encode(&(
            "lastdb-snapshot-identity-preparation-v1",
            self.version,
            &self.source_root,
            &self.candidate_root,
            &self.source_map_sha256,
            &self.candidate_map_sha256,
            &self.local_origin,
        ))
    }
}

fn preparation_key() -> String {
    native_routing::encode(b"snapshot-adoption-prepared-v1")
}
fn peer_key(origin: &str) -> String {
    native_routing::encode(format!("p\0{origin}").as_bytes())
}

fn operation<T: serde::Serialize>(key: String, value: &T) -> StorageResult<HomeMutation> {
    bounded::serialized_size_with_limit(value, peer::CAPTURE_RECORD_BYTES as usize)?;
    Ok(HomeMutation {
        collection: COLLECTION.into(),
        key,
        body: Some(encode(value)?),
    })
}
