//! Row-by-row walk state for the order-log audit.

use super::*;

/// Cursor state of one order-log audit pass over the `mk:` keyspace.
///
/// The molecule currently being walked and the last key seen for it. Nothing
/// is decided until the walk leaves the molecule, so a pass that is cut short
/// cannot report a half-counted molecule as healthy. `unique` in the open
/// walk tracks decoded (hash, range) identities so H1 (missing append, where
/// unique exceeds moc) can be told from H2 (dual encoding, where raw exceeds
/// unique and unique is about moc).
/// Undecodable suffixes fall back to the full storage key so they never
/// collapse into each other.
#[derive(Default)]
pub(super) struct OrderAuditWalk {
    pub(super) open: Option<OpenMoleculeWalk>,
    pub(super) last_key_of_open: Option<String>,
    pub(super) decided_molecules: HashSet<String>,
    pub(super) stop_at_boundary: bool,
}

/// What one `mk:` row did to the walk.
#[derive(PartialEq, Eq)]
pub(super) enum WalkStep {
    /// Not an `mk:{molecule}:` row.
    Skipped,
    /// Counted toward the open molecule.
    Counted,
    /// The pass is braked and this row starts the next molecule.
    StopPass,
}

impl AtomStore {
    /// Fold one `mk:` row into the walk. A row that leaves the open molecule
    /// decides it first; if the pass is braked, that row is not counted.
    pub(super) async fn walk_mk_row(
        &self,
        report: &mut OrderLogAudit,
        order_counts: &HashMap<String, u64>,
        walk: &mut OrderAuditWalk,
        key: String,
        mk_prefix: &str,
        storage_prefix: Option<&str>,
    ) -> WalkStep {
        let Some(molecule) = key
            .strip_prefix(mk_prefix)
            .and_then(|rest| rest.split_once(':'))
            .map(|(molecule, _)| molecule.to_string())
        else {
            return WalkStep::Skipped;
        };
        let logical = Self::order_audit_logical_id(&molecule, &key, mk_prefix);

        match &mut walk.open {
            Some(cur) if cur.molecule == molecule => {
                cur.raw_keys += 1;
                cur.unique.insert(logical);
                walk.last_key_of_open = Some(key);
            }
            _ => {
                // Left the previous molecule: it is now fully walked.
                if let Some(done) = walk.open.take() {
                    let molecule = self
                        .decide_open_molecule(report, order_counts, done, storage_prefix)
                        .await;
                    walk.decided_molecules.insert(molecule);
                    report.next_after_key = walk.last_key_of_open.take();
                }
                if walk.stop_at_boundary {
                    return WalkStep::StopPass;
                }
                let mut unique = HashSet::new();
                unique.insert(logical);
                walk.open = Some(OpenMoleculeWalk {
                    molecule,
                    raw_keys: 1,
                    unique,
                });
                walk.last_key_of_open = Some(key);
            }
        }
        WalkStep::Counted
    }
}
