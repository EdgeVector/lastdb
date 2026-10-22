use super::*;

/// The generations that must stay name too many parts for one reclaim to plan.
/// The error says what keeps them. Nothing was deleted, so the home is safe and
/// only grows until the references end.
fn retained_parts_limit(
    retained: &BTreeSet<u64>,
    references: usize,
    local_reads: usize,
) -> crate::Error {
    crate::Error::Config(format!(
        "disk-pack reclaim: the {} retained generations ({references} durable references, \
         {local_reads} held by active reads) name more than {MAX_RETAINED_PARTS} parts; \
         release stale backup or recovery references to resume reclaim; nothing was deleted",
        retained.len()
    ))
}

/// The parts that the retained generations name.
///
/// Every part of a retained generation is checked once per lease. A later call
/// reads its receipt only, so a drain of many calls does not open every part of
/// the current generation again for each call.
pub(super) fn live_parts(
    lock: &WriteLock,
    retained: &BTreeSet<u64>,
    references: usize,
    local_reads: usize,
) -> Result<BTreeSet<Coordinate>> {
    let root = lock.root();
    let mut live = BTreeSet::new();
    for number in retained {
        let held = at_generation(generation::load_current(root, *number), root, *number)?;
        let generation = if lock.verified(&held)? {
            at_generation(generation::read_receipt(root, &held), root, *number)?
        } else {
            let verified = at_generation(generation::verify(root, &held), root, *number)?;
            lock.remember_verified(&held)?;
            verified
        };
        for coordinate in generation.coordinates {
            live.insert(coordinate);
            if live.len() > MAX_RETAINED_PARTS {
                return Err(retained_parts_limit(retained, references, local_reads));
            }
        }
    }
    lock.keep_verified(retained)?;
    Ok(live)
}
