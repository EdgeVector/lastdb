//! Molecule history and atom content handlers.

use super::*;

// ---------------------------------------------------------------------------
// Molecule history (GET /api/history/{molecule_uuid})
// ---------------------------------------------------------------------------

/// Optional `?hash=`/`?range=` record-scope for the molecule-history route.
#[derive(Default)]
pub struct HistoryScope {
    pub hash: Option<String>,
    pub range: Option<String>,
}

impl HistoryScope {
    pub(super) fn is_unscoped(&self) -> bool {
        self.hash.is_none() && self.range.is_none()
    }

    /// True when `field_key` belongs to the record this scope names. A `Single`
    /// field has no key to scope by, so any provided scope excludes it; a
    /// hash-only scope matches any range and vice-versa; both must match when
    /// both are given.
    pub(super) fn matches(&self, field_key: &fold_db::atom::FieldKey) -> bool {
        // Single (no key dimensions) is never in a keyed scope.
        if field_key.is_single() {
            return false;
        }
        let hash_ok = |h: &str| self.hash.as_deref().is_none_or(|want| want == h);
        let range_ok = |r: &str| self.range.as_deref().is_none_or(|want| want == r);
        match (&field_key.hash, &field_key.range) {
            (Some(hash), None) => self.range.is_none() && hash_ok(hash),
            (None, Some(range)) => self.hash.is_none() && range_ok(range),
            (Some(hash), Some(range)) => hash_ok(hash) && range_ok(range),
            (None, None) => false,
        }
    }
}

/// Execute `GET /api/history/{molecule_uuid}`: list the molecule's mutation
/// events (every branch of a concurrent write, conflict winners AND losers),
/// optionally scoped to one record's slot. Returns the `{ molecule_uuid, events }`
/// payload. Single copy of both hosts' history route.
///
/// # Errors
/// [`HostError`] `500` on an event-log read failure.
pub async fn molecule_history<H: HostNode>(
    host: &H,
    molecule_uuid: &str,
    scope: &HistoryScope,
) -> Result<Value, HostError> {
    let events = host
        .fold_db()
        .db_ops()
        .atoms()
        .get_mutation_events(molecule_uuid, None)
        .await
        .map_err(|e| HostError::internal(e.to_string()))?;

    let summaries: Vec<Value> = events
        .into_iter()
        .filter(|e| scope.is_unscoped() || scope.matches(&e.field_key))
        .map(|e| {
            let mut ev = serde_json::json!({
                "timestamp": e.timestamp.to_rfc3339(),
                "version": e.version,
                "field_key": serde_json::to_value(&e.field_key).unwrap_or_default(),
                "old_atom_uuid": e.old_atom_uuid,
                "new_atom_uuid": e.new_atom_uuid,
                "is_conflict": e.is_conflict,
                "writer_pubkey": e.writer_pubkey,
            });
            if let Some(loser) = e.conflict_loser_atom {
                ev["conflict_loser_atom"] = Value::String(loser);
            }
            ev
        })
        .collect();

    Ok(serde_json::json!({
        "molecule_uuid": molecule_uuid,
        "events": summaries,
    }))
}

// ---------------------------------------------------------------------------
// Atom content (GET /api/atom/{atom_uuid})
// ---------------------------------------------------------------------------

/// Execute `GET /api/atom/{atom_uuid}`: hydrate any atom (including a conflict
/// loser surfaced by the history route) by UUID. Returns the
/// `{ atom_uuid, content, source_file_name, created_at }` payload.
///
/// # Errors
/// [`HostError`] `404` when the atom is unknown, `500` on a read failure.
pub async fn atom_content<H: HostNode>(host: &H, atom_uuid: &str) -> Result<Value, HostError> {
    // A raw atom fetch is the bulk-object read surface (how a large stored blob —
    // e.g. a lastgit pack object — is read back): admit it on the bulk lane so a
    // clone loop cannot crowd out interactive point reads.
    let _permit = host.acquire_op_permit(Lane::Bulk).await?;
    let atom = host
        .fold_db()
        .db_ops()
        .atoms()
        .get_atom_by_uuid(atom_uuid, None)
        .await
        .map_err(|e| HostError::internal(e.to_string()))?
        .ok_or_else(|| HostError::new(404, format!("Atom '{atom_uuid}' not found")))?;

    Ok(serde_json::json!({
        "atom_uuid": atom_uuid,
        "content": atom.content().clone(),
        "source_file_name": atom.source_file_name().cloned(),
        "created_at": atom.created_at().to_rfc3339(),
    }))
}
