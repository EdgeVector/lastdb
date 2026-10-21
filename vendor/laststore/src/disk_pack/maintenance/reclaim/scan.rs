//! Planning for reclaim: which old generations retire and which parts only they name.

use super::*;

/// Old generations and the parts only they name.
pub(super) struct OldScan {
    pub(super) old: Vec<Current>,
    pub(super) candidates: BTreeMap<Coordinate, PathBuf>,
    pub(super) remaining: bool,
}

/// Plan the oldest old generations, at most `policy.reclaim_generations` of them.
/// A receipt that cannot be read is an error that names its directory; the listing
/// already removed the receipts that are provably unfinished.
pub(super) fn scan_old(
    root: &Path,
    generations: Vec<u64>,
    retained: &BTreeSet<u64>,
    live: &BTreeSet<Coordinate>,
    policy: &SpacePolicy,
    cutoff: u64,
) -> Result<OldScan> {
    let mut scan = OldScan {
        old: Vec::new(),
        candidates: BTreeMap::new(),
        remaining: false,
    };
    for number in generations {
        if retained.contains(&number) {
            continue;
        }
        if scan.old.len() >= policy.reclaim_generations {
            scan.remaining = true;
            break;
        }
        let held = at_generation(generation::load_current(root, number), root, number)?;
        if held.published_ms > cutoff {
            continue;
        }
        let receipt = at_generation(generation::read_receipt(root, &held), root, number)?;
        let fresh: Vec<Coordinate> = receipt
            .coordinates
            .iter()
            .filter(|coordinate| {
                !live.contains(*coordinate) && !scan.candidates.contains_key(*coordinate)
            })
            .copied()
            .collect();
        // One call plans a bounded set of parts. The rest wait for the next call.
        if !scan.old.is_empty() && scan.candidates.len() + fresh.len() > MAX_RETAINED_PARTS {
            scan.remaining = true;
            break;
        }
        for coordinate in fresh {
            scan.candidates.insert(coordinate, coordinate.path(root)?);
        }
        // Retain root identities only; old conservative receipts can overlap heavily.
        scan.old.push(held);
    }
    Ok(scan)
}
