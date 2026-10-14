//! Compact reverse-edge completion markers for a brand-new home.
//!
//! Automatic and manual `gc-atoms` refuse to delete a body until the compact
//! reverse-edge plane is proven complete
//! ([`AtomStore::atom_ref_v2_reads_ready`]). The proof is two markers. The
//! backfill that wrote them is gone, so a home created by this build had no
//! way to ever hold them, and every garbage-collection pass on it deleted
//! nothing and reported nothing.
//!
//! A home that holds no atom data is complete by construction when every row
//! that will ever enter it comes from a writer that maintains the edges. The
//! daemon stamps such a home once, on the one path that creates it: the first
//! boot that mints the identity over an empty store. No other open stamps.
//!
//! The stamp is a distinct value, `fresh-home`. Anything that installs rows
//! without edges (a photograph restore) must drop it first;
//! [`AtomStore::revoke_fresh_home_atom_ref_markers`] does, and the restore runs
//! it before the install as well as after. A home that holds data and lacks
//! the markers keeps the fail-closed behavior.
//!
//! Every error here means "not fresh". A failed stamp never fails boot.

use super::AtomStore;
use crate::atom::molecule_key_codec::{ATOM_REF_V2_COMPLETE_KEY, ATOM_REF_V2_HISTORY_COMPLETE_KEY};
use crate::schema::types::field::FilterUtils;
use crate::storage::{KvMutation, StorageError};

/// Marker value written by the fresh-home stamp. The real backfill wrote
/// `true`; a restore that carries the real marker overwrites this value.
pub(super) const ATOM_REF_V2_FRESH_HOME_MARKER: &[u8] = b"fresh-home";

/// Colon planes that must all be empty before the stamp is allowed.
const FRESH_HOME_PROBE_PLANES: [&str; 6] = ["mk:", "atom:", "tv:", "history:", "conflict:", "ref:"];

/// The reverse-edge plane: edges, live counts, GC candidates and the markers.
const FRESH_HOME_EDGE_PLANE: &str = "aref:";

/// Most physical handles one emptiness probe may visit per plane.
const FRESH_HOME_PROBE_HANDLES: usize = 16;

impl AtomStore {
    /// Stamp both completion markers when this home holds no atom data.
    ///
    /// Returns `true` when the stamp was written. A marker already present, a
    /// row in any probed plane, a plane the bounded walk cannot finish, or any
    /// storage error leaves the home unstamped. Never an `Err`, so a caller in
    /// boot cannot fail on it.
    pub(crate) async fn stamp_fresh_home_atom_ref_markers(&self) -> bool {
        match self.try_stamp_fresh_home_atom_ref_markers().await {
            Ok(stamped) => stamped,
            Err(error) => {
                tracing::warn!(
                    target: "fold_db::gc_atoms",
                    %error,
                    "could not prove the home empty; atom reverse-edge completion markers not stamped"
                );
                false
            }
        }
    }

    async fn try_stamp_fresh_home_atom_ref_markers(&self) -> Result<bool, StorageError> {
        let keys = [ATOM_REF_V2_COMPLETE_KEY, ATOM_REF_V2_HISTORY_COMPLETE_KEY];
        for key in keys {
            if self.main_store.inner().get(key.as_bytes()).await?.is_some() {
                return Ok(false);
            }
        }
        if !self.home_holds_no_atom_data().await? {
            return Ok(false);
        }
        let mutations = keys
            .iter()
            .map(|key| KvMutation::put(key.as_bytes().to_vec(), ATOM_REF_V2_FRESH_HOME_MARKER))
            .collect();
        self.main_store.inner().batch_mutate(mutations).await?;
        self.main_store.inner().flush().await?;
        tracing::info!(
            target: "fold_db::gc_atoms",
            "new home stamped atom reverse-edge completion markers (every writer maintains edges from the first write)"
        );
        Ok(true)
    }

    /// Remove fresh-home markers that a restore did not replace with the real
    /// ones, and forget the cached ready state. A marker the source carried
    /// holds the real value and stays. The removal is flushed: a revoke that a
    /// crash can lose would leave a stamp over rows with no edges.
    pub(crate) async fn revoke_fresh_home_atom_ref_markers(&self) -> Result<(), StorageError> {
        let mut removals = Vec::new();
        for key in [ATOM_REF_V2_COMPLETE_KEY, ATOM_REF_V2_HISTORY_COMPLETE_KEY] {
            let current = self.main_store.inner().get(key.as_bytes()).await?;
            if current.as_deref() == Some(ATOM_REF_V2_FRESH_HOME_MARKER) {
                removals.push(KvMutation::delete(key.as_bytes().to_vec()));
            }
        }
        if !removals.is_empty() {
            self.main_store.inner().batch_mutate(removals).await?;
            self.main_store.inner().flush().await?;
        }
        self.atom_ref_v2_read_ready
            .lock()
            .map_err(|error| StorageError::BackendError(format!("lock atom read gate: {error}")))?
            .clear();
        Ok(())
    }

    /// True only when every probed plane was walked to its end and held no row.
    ///
    /// Physical scans only, bounded to a few handles per plane. A logical
    /// range read would resolve every group of a large plane before it returned
    /// its first row (measured at 281-350 seconds on the primary). A plane the
    /// bound cannot finish, or a walk that errors, is "not proven empty".
    pub(super) async fn home_holds_no_atom_data(&self) -> Result<bool, StorageError> {
        for plane in FRESH_HOME_PROBE_PLANES {
            let (start, end) = Self::kind_plane_scan_bounds(None, plane);
            if !self.plane_proven_empty(&start, &end).await? {
                return Ok(false);
            }
        }
        let edge_end = FilterUtils::create_prefix_end(FRESH_HOME_EDGE_PLANE);
        self.plane_proven_empty(FRESH_HOME_EDGE_PLANE, &edge_end)
            .await
    }

    /// One physical walk of `start..end`, bounded to `FRESH_HOME_PROBE_HANDLES`.
    async fn plane_proven_empty(&self, start: &str, end: &str) -> Result<bool, StorageError> {
        let page = self
            .main_store
            .inner()
            .scan_range_physical_paged(
                start.as_bytes(),
                end.as_bytes(),
                None,
                1,
                FRESH_HOME_PROBE_HANDLES,
            )
            .await?;
        Ok(page.rows.is_empty() && page.next_cursor.is_none())
    }
}
