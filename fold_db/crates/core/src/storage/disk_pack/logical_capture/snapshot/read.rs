//! Bounded exact-key reads of one immutable captured native snapshot.

use super::super::model::Head;
use super::*;

pub(super) struct CapturedPositions {
    pub producer: String,
    pub positions: Vec<SnapshotDevicePosition>,
}

pub(super) fn positions(
    main: &mut RunRead,
    profile: &RunBackupProfile,
) -> StorageResult<Option<CapturedPositions>> {
    profile.validate()?;
    if main.home_id().to_string() != profile.main.home_id.as_str()
        || main.generation() != profile.main.generation.get()
        || main.frontier() != profile.main.frontier.get()
    {
        return Err(invalid(
            "snapshot inclusion reader differs from its captured profile",
        ));
    }
    let keys = vec![
        (COLLECTION.to_string(), head_key()),
        (COLLECTION.to_string(), native_routing::encode(b"peers")),
        (REGISTRY.to_string(), format!("{NAMESPACES}{COLLECTION}")),
    ];
    let mut values = main
        .get_many_bounded(&keys, peer::CAPTURE_RECORD_BYTES)
        .map_err(map)?
        .into_iter();
    let head = values.next().expect("head slot").map_err(map)?;
    let roster = values.next().expect("roster slot").map_err(map)?;
    let registry = values.next().expect("registry slot").map_err(map)?;
    let Some(head) = head else {
        if roster.is_some() || registry.is_some() {
            return Err(invalid(
                "snapshot inclusion has device metadata without a head",
            ));
        }
        return Ok(None);
    };
    if registry.as_deref() != Some(&[1u8][..]) {
        return Err(invalid("snapshot inclusion namespace registration differs"));
    }
    let head: Head = decode(&head)?;
    let own_anchor = validate_head(&head, profile)?;
    let mut positions = peer::captured_positions(main, roster.as_deref(), &head.position.origin)?
        .into_iter()
        .map(|peer| SnapshotDevicePosition {
            origin: peer.position.origin,
            anchor_sha256: peer.anchor.sha256,
            sequence: DecimalU64::new(peer.position.sequence),
            sha256: peer.position.sha256,
        })
        .collect::<Vec<_>>();
    let producer = head.position.origin.clone();
    positions.push(SnapshotDevicePosition {
        origin: head.position.origin,
        anchor_sha256: own_anchor,
        sequence: DecimalU64::new(head.position.sequence),
        sha256: head.position.sha256,
    });
    positions.sort_by(|a, b| a.origin.cmp(&b.origin));
    Ok(Some(CapturedPositions {
        producer,
        positions,
    }))
}

fn validate_head(head: &Head, profile: &RunBackupProfile) -> StorageResult<String> {
    verify::origin_key(&head.position.origin)?;
    unhex::<32>(&head.position.sha256)?;
    let main = uuid::Uuid::parse_str(profile.main.home_id.as_str())
        .map_err(|_| invalid("snapshot inclusion main home differs"))?;
    let cas = uuid::Uuid::parse_str(profile.cas.home_id.as_str())
        .map_err(|_| invalid("snapshot inclusion CAS home differs"))?;
    let anchor = identity::anchor_for(&head.position.origin, main, cas)?;
    if head.version != 1
        || head.encoded_backlog_bytes > BACKLOG_CAP_BYTES
        || (head.position.sequence == 0
            && (head.position.sha256 != anchor || head.encoded_backlog_bytes != 0))
    {
        return Err(invalid("snapshot inclusion local device head differs"));
    }
    Ok(anchor)
}
