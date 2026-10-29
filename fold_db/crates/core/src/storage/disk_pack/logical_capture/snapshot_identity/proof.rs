//! Compare original chains at exact common positions without event-body loads.

use super::*;

enum CommonKind {
    Authored,
    Received,
}
struct Common<'a> {
    entry: &'a SnapshotDevicePosition,
    kind: CommonKind,
}

pub(super) fn verify_coverage(
    source: &SourceIdentity,
    candidate: &SnapshotInclusionMap,
    main: &mut RunRead,
) -> StorageResult<()> {
    candidate.validate_for(&candidate.snapshot)?;
    if candidate.producer == source.map.producer
        || candidate.positions.len() != source.map.positions.len()
    {
        return Err(invalid(
            "snapshot identity needs the complete trusted foreign device set",
        ));
    }
    let mut common = Vec::with_capacity(32);
    let mut keys = Vec::with_capacity(32);
    for entry in &candidate.positions {
        let index = source
            .map
            .positions
            .binary_search_by(|known| known.origin.cmp(&entry.origin))
            .map_err(|_| invalid("snapshot identity candidate contains an unknown device"))?;
        let known = &source.map.positions[index];
        if entry.anchor_sha256 != known.anchor_sha256 || entry.sequence.get() < known.sequence.get()
        {
            return Err(invalid(
                "snapshot identity candidate loses a known original device position",
            ));
        }
        if entry.origin == source.map.producer && entry.sequence != known.sequence {
            return Err(invalid(
                "snapshot identity would require local authored-tail replay",
            ));
        }
        if entry.sequence == known.sequence {
            if entry.sha256 != known.sha256 {
                return Err(invalid("snapshot identity equal device positions fork"));
            }
            continue;
        }
        if known.sequence.get() == 0 {
            continue;
        }
        if let Some(base) = &entry.base {
            if known.sequence.get() < base.sequence.get() {
                return Err(invalid(
                    "snapshot identity common prefix predates retained receipts",
                ));
            }
            if known.sequence == base.sequence {
                if known.sha256 != base.sha256 {
                    return Err(invalid("snapshot identity retained prefix base forks"));
                }
                continue;
            }
        }
        let (key, kind) = if entry.origin == candidate.producer {
            let key = if candidate.version == 1 {
                key("m", known.sequence.get())
            } else {
                native_routing::encode(
                    format!("m2\0{}\0{:020}", entry.origin, known.sequence.get()).as_bytes(),
                )
            };
            (key, CommonKind::Authored)
        } else {
            (
                peer::receipt_key(&entry.origin, known.sequence.get()),
                CommonKind::Received,
            )
        };
        keys.push((COLLECTION.to_string(), key));
        common.push(Common { entry: known, kind });
    }
    let values = main
        .get_many_bounded(&keys, peer::CAPTURE_RECORD_BYTES)
        .map_err(map)?;
    if values.len() != common.len() {
        return Err(invalid(
            "snapshot identity common prefix result count differs",
        ));
    }
    for (proof, bytes) in common.into_iter().zip(values) {
        let bytes = bytes
            .map_err(map)?
            .ok_or_else(|| invalid("snapshot identity common prefix proof is absent"))?;
        verify_common(proof, &bytes)?;
    }
    Ok(())
}

fn verify_common(proof: Common<'_>, bytes: &[u8]) -> StorageResult<()> {
    match proof.kind {
        CommonKind::Authored => {
            let metadata: EventMetadata = decode(bytes)?;
            metadata.verify_prefix(
                &proof.entry.origin,
                proof.entry.sequence.get(),
                &proof.entry.sha256,
            )?;
        }
        CommonKind::Received => {
            if bytes != unhex::<32>(&proof.entry.sha256)?.as_slice() {
                return Err(invalid(
                    "snapshot identity original received prefix differs",
                ));
            }
        }
    }
    Ok(())
}
