//! Protein operations on [`AtomStore`]: create, bind, write, fold, backfill.

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use crate::atom::{AtomEntry, MoleculeHashRange};
use crate::db_operations::atom_store::{AtomStore, ChangedKey, MoleculeData, MoleculeRefEdge};
use crate::schema::SchemaError;
use crate::security::Ed25519KeyPair;

use super::keys::{
    member_backref_key, protein_fold_job_key, protein_record_key, PROTEIN_FOLD_JOB_PREFIX,
};
use super::types::{
    Protein, ProteinFoldJob, ProteinMember, ProteinWriteOutcome, PROTEIN_SCHEMA_MARKER,
};

mod batch;

impl AtomStore {
    /// Create a new empty protein (UUID identity). Membership is empty until
    /// [`Self::protein_add_member`].
    pub async fn protein_create(&self) -> Result<Protein, SchemaError> {
        let uuid = Uuid::new_v4().to_string();
        let protein = Protein::new(uuid);
        self.put_protein(&protein).await?;
        Ok(protein)
    }

    /// Load a protein by UUID.
    pub async fn protein_get(&self, protein_uuid: &str) -> Result<Option<Protein>, SchemaError> {
        self.raw()
            .get_item(&protein_record_key(protein_uuid))
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read protein {protein_uuid}: {e}")))
    }

    /// Protein UUID for a member molecule, if bound.
    pub async fn protein_of_molecule(
        &self,
        molecule_uuid: &str,
    ) -> Result<Option<String>, SchemaError> {
        let key = member_backref_key(molecule_uuid);
        let v: Option<String> =
            self.raw().get_item(&key).await.map_err(|e| {
                SchemaError::InvalidData(format!("read molprot {molecule_uuid}: {e}"))
            })?;
        Ok(v)
    }

    /// Bind a molecule as a protein member (bi-directional).
    ///
    /// Idempotent when the same molecule is already a member with the same
    /// key layout. Errors if the molecule is already bound to a *different*
    /// protein.
    pub async fn protein_add_member(
        &self,
        protein_uuid: &str,
        member: ProteinMember,
    ) -> Result<Protein, SchemaError> {
        if member.molecule_uuid.is_empty() {
            return Err(SchemaError::InvalidData(
                "protein member molecule_uuid must be non-empty".into(),
            ));
        }
        if member.hash_field.trim().is_empty() {
            return Err(SchemaError::InvalidData(
                "protein member hash_field must be non-empty".into(),
            ));
        }

        if let Some(existing) = self.protein_of_molecule(&member.molecule_uuid).await? {
            if existing != protein_uuid {
                return Err(SchemaError::InvalidData(format!(
                    "molecule {} already bound to protein {existing}, not {protein_uuid}",
                    member.molecule_uuid
                )));
            }
        }

        let mut protein = self
            .protein_get(protein_uuid)
            .await?
            .ok_or_else(|| SchemaError::InvalidData(format!("protein {protein_uuid} not found")))?;

        // Same molecule + same key layout → no-op. Same molecule + different
        // layout → additional conformation (shared atom, multi-key tips).
        if protein
            .member_for_layout(
                &member.molecule_uuid,
                &member.hash_field,
                member.range_field.as_deref(),
            )
            .is_some()
        {
            return Ok(protein);
        }

        let molecule_ref = MoleculeRefEdge::protein_member(
            protein_uuid,
            &member.molecule_uuid,
            &member.hash_field,
            member.range_field.as_deref(),
        );
        let _target_gate = self
            .lock_liveness_molecules(std::slice::from_ref(&member.molecule_uuid))
            .await;
        // Retain first. An interrupted bind can leave an extra edge, but it
        // cannot publish a protein member that the molecule reclaimer misses.
        self.put_molecule_ref_edge(&molecule_ref, None).await?;
        protein.members.push(member.clone());
        self.put_protein(&protein).await?;
        self.raw()
            .put_item(&member_backref_key(&member.molecule_uuid), &protein_uuid)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("write molprot {}: {e}", member.molecule_uuid))
            })?;
        Ok(protein)
    }

    /// Remove one molecule key layout from a protein.
    ///
    /// The protein source row changes before its `mref:v1` edge leaves the
    /// active set. An interrupted operation can retain an extra edge, but it
    /// cannot expose a live member without a retaining edge.
    pub async fn protein_remove_member(
        &self,
        protein_uuid: &str,
        molecule_uuid: &str,
        hash_field: &str,
        range_field: Option<&str>,
    ) -> Result<Protein, SchemaError> {
        let mut protein = self
            .protein_get(protein_uuid)
            .await?
            .ok_or_else(|| SchemaError::InvalidData(format!("protein {protein_uuid} not found")))?;
        let Some(member) = protein
            .member_for_layout(molecule_uuid, hash_field, range_field)
            .cloned()
        else {
            return Ok(protein);
        };
        let edge =
            MoleculeRefEdge::protein_member(protein_uuid, molecule_uuid, hash_field, range_field);
        let _target_gate = self
            .lock_liveness_molecules(&[molecule_uuid.to_string()])
            .await;
        protein.members.retain(|candidate| candidate != &member);
        self.put_protein(&protein).await?;
        if !protein.contains_molecule(molecule_uuid) {
            self.raw()
                .delete_item(&member_backref_key(molecule_uuid))
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "delete molprot {molecule_uuid} after protein member removal: {e}"
                    ))
                })?;
        }
        self.delete_molecule_ref_edge(&edge, None).await?;
        Ok(protein)
    }

    /// Write a shared atom tip on the **entry** member and enqueue fold jobs so
    /// sibling members converge on the same atom under their own key layouts.
    ///
    /// Returns immediately after the entry tip + atom are durable. Call
    /// [`Self::protein_process_folds`] (or let a background worker) to apply
    /// sibling tip repoints.
    pub async fn protein_write_via_member(
        &self,
        entry_molecule_uuid: &str,
        fields: &HashMap<String, String>,
        content: Value,
        keypair: &Ed25519KeyPair,
    ) -> Result<ProteinWriteOutcome, SchemaError> {
        let protein_uuid = self
            .protein_of_molecule(entry_molecule_uuid)
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "molecule {entry_molecule_uuid} is not a protein member"
                ))
            })?;
        let protein = self
            .protein_get(&protein_uuid)
            .await?
            .ok_or_else(|| SchemaError::InvalidData(format!("protein {protein_uuid} not found")))?;
        let entry_member = protein
            .member_for_molecule(entry_molecule_uuid)
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "protein {protein_uuid} missing member {entry_molecule_uuid}"
                ))
            })?
            .clone();

        let (hash, range) = entry_member.tip_coords_from_fields(fields).ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "entry member {} missing key fields in write map (need hash_field={})",
                entry_molecule_uuid, entry_member.hash_field
            ))
        })?;

        // Create + store shared atom (content-addressed).
        let atom = Self::create_atom(PROTEIN_SCHEMA_MARKER, content, None, None)
            .map_err(|e| SchemaError::InvalidData(format!("create protein atom: {e}")))?;
        let atom_uuid = atom.uuid().to_string();
        self.batch_store_atoms(vec![atom], None)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("store protein atom: {e}")))?;

        let written_at = crate::clock::unix_nanos();
        let device_id = keypair.public_key_base64();
        let tip = AtomEntry::thin(atom_uuid.clone(), written_at, device_id.clone());

        self.protein_set_member_tip(entry_molecule_uuid, &hash, &range, &tip, keypair)
            .await?;

        // Enqueue fold for every sibling member that can derive tip coords.
        let mut fold_jobs = 0usize;
        for sibling in &protein.members {
            if sibling.molecule_uuid == entry_molecule_uuid {
                continue;
            }
            if sibling.tip_coords_from_fields(fields).is_none() {
                continue;
            }
            let job = ProteinFoldJob {
                job_id: Uuid::new_v4().to_string(),
                protein_uuid: protein_uuid.clone(),
                entry_molecule_uuid: entry_molecule_uuid.to_string(),
                atom_uuid: atom_uuid.clone(),
                written_at,
                device_id: device_id.clone(),
                fields: fields.clone(),
                schema: PROTEIN_SCHEMA_MARKER.to_string(),
            };
            let pending_source = format!("protein-fold:{}", job.job_id);
            let pending_item = Self::pending_atom_ref_item(&job.atom_uuid, &pending_source, None)?;
            self.raw()
                .batch_put_items(vec![
                    (
                        protein_fold_job_key(&job.job_id),
                        serde_json::to_value(&job).map_err(|e| {
                            SchemaError::InvalidData(format!(
                                "serialize protein fold {}: {e}",
                                job.job_id
                            ))
                        })?,
                    ),
                    pending_item,
                ])
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("enqueue protein fold {}: {e}", job.job_id))
                })?;
            fold_jobs += 1;
        }
        self.keep_small()
            .add_pending_protein_folds(fold_jobs as u64);
        // Debounced persist only. The pending-fold gauge is not correctness
        // state; a per-write flush here was one of the 2026-09-21 flush-storm
        // sites (whole counter map per protein write into one `metadata` group).
        let _ = self.persist_keep_small().await;

        Ok(ProteinWriteOutcome {
            protein_uuid,
            atom_uuid,
            entry_molecule_uuid: entry_molecule_uuid.to_string(),
            fold_jobs_enqueued: fold_jobs,
            used_protein_path: true,
        })
    }

    /// Process up to `max_jobs` pending fold jobs. Returns how many were applied.
    pub async fn protein_process_folds(&self, max_jobs: usize) -> Result<usize, SchemaError> {
        if max_jobs == 0 {
            return Ok(0);
        }
        let mut jobs: Vec<(String, ProteinFoldJob)> = self
            .raw()
            .scan_items_with_prefix(PROTEIN_FOLD_JOB_PREFIX)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("scan protein fold jobs: {e}")))?;
        // Stable order by job key.
        jobs.sort_by(|(a, _), (b, _)| a.cmp(b));
        let mut applied = 0usize;
        let keypair = Ed25519KeyPair::generate()
            .map_err(|e| SchemaError::InvalidData(format!("fold keypair: {e}")))?;

        for (storage_key, job) in jobs.into_iter().take(max_jobs) {
            self.protein_apply_fold_job(&job, &keypair).await?;
            let pending_source = format!("protein-fold:{}", job.job_id);
            self.raw()
                .batch_delete_keys(vec![
                    storage_key.clone(),
                    Self::pending_atom_ref_key(&job.atom_uuid, &pending_source, None),
                ])
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("delete fold job {storage_key}: {e}"))
                })?;
            self.keep_small().complete_pending_protein_fold();
            applied += 1;
        }
        if applied > 0 {
            // Debounced persist only (2026-09-21 flush storm; see protein_write).
            let _ = self.persist_keep_small().await;
        }
        Ok(applied)
    }

    /// Apply one fold job: repoint every non-entry member tip to the shared atom.
    pub async fn protein_apply_fold_job(
        &self,
        job: &ProteinFoldJob,
        keypair: &Ed25519KeyPair,
    ) -> Result<(), SchemaError> {
        let protein = self.protein_get(&job.protein_uuid).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("protein {} not found for fold", job.protein_uuid))
        })?;

        let tip = AtomEntry::thin(
            job.atom_uuid.clone(),
            job.written_at,
            if job.device_id.is_empty() {
                keypair.public_key_base64()
            } else {
                job.device_id.clone()
            },
        );

        for member in &protein.members {
            if member.molecule_uuid == job.entry_molecule_uuid {
                continue;
            }
            let Some((hash, range)) = member.tip_coords_from_fields(&job.fields) else {
                continue;
            };
            self.protein_set_member_tip(&member.molecule_uuid, &hash, &range, &tip, keypair)
                .await?;
        }
        Ok(())
    }

    /// After a normal field write that already set the entry tip to `atom_uuid`,
    /// prepare sibling protein members to the same atom **in memory** (sync fold
    /// of tip state). Does **not** durable-store.
    ///
    /// Used by the mutation path when field_hash auto-protein is bound: entry
    /// write stays on the normal molecule path; siblings fold without creating
    /// a second atom. Under `LASTDB_RESIDENT_MODE=write` the caller publishes
    /// these tips dirty to resident and defers
    /// [`Self::store_molecules_changed_keys_batch`] with the entry durable put
    /// — sibling flush must not sit on the mutation hot path.
    ///
    /// `protein_uuid` is supplied by the caller because every caller has already
    /// resolved it to decide whether a fold is needed at all. Re-resolving it
    /// here cost one extra `molprot:` get **per mutated field** — 24 redundant
    /// storage reads on a single 24-field kanban card write — for an answer the
    /// caller was holding. See `fold_protein_siblings_after_write`.
    pub(crate) async fn protein_sibling_tip_updates(
        &self,
        protein_uuid: &str,
        entry_molecule_uuid: &str,
        fields: &HashMap<String, String>,
        atom_uuid: &str,
        keypair: &Ed25519KeyPair,
    ) -> Result<Vec<(String, MoleculeData, HashSet<ChangedKey>)>, SchemaError> {
        self.protein_sibling_tip_updates_for_layout(
            protein_uuid,
            entry_molecule_uuid,
            None,
            fields,
            atom_uuid,
            keypair,
            None,
        )
        .await
    }

    /// Prepare sibling tip updates and durable-store them immediately.
    ///
    /// Prefer the mutation path's deferred-persist wiring under write-mode;
    /// this helper remains for tests and callers that need store-before-return.
    pub async fn protein_fold_siblings_to_atom(
        &self,
        protein_uuid: &str,
        entry_molecule_uuid: &str,
        fields: &HashMap<String, String>,
        atom_uuid: &str,
        keypair: &Ed25519KeyPair,
    ) -> Result<usize, SchemaError> {
        let mut sibling_updates = self
            .protein_sibling_tip_updates(
                protein_uuid,
                entry_molecule_uuid,
                fields,
                atom_uuid,
                keypair,
            )
            .await?;
        let folded = sibling_updates.len();
        if !sibling_updates.is_empty() {
            self.store_molecules_changed_keys_batch(&sibling_updates, None)
                .await?;
            for (_, mol, _) in &mut sibling_updates {
                let _ = mol.take_pending_tip_versions();
            }
        }
        Ok(folded)
    }

    /// One-time tip backfill when adding a new key as a self-maintaining member.
    ///
    /// For each tip currently present on `source_molecule` under keys that can
    /// be re-derived via `source_member` / `new_member` field maps built from
    /// `records` (each record is a full field map), set the new member's tip to
    /// the same atom as the source tip.
    pub async fn protein_backfill_member_tips(
        &self,
        source_molecule_uuid: &str,
        new_member: &ProteinMember,
        records: &[HashMap<String, String>],
        keypair: &Ed25519KeyPair,
    ) -> Result<usize, SchemaError> {
        let source_protein = self
            .protein_of_molecule(source_molecule_uuid)
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "source molecule {source_molecule_uuid} is not a protein member"
                ))
            })?;
        let protein = self.protein_get(&source_protein).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("protein {source_protein} not found"))
        })?;
        let source_member = protein
            .member_for_molecule(source_molecule_uuid)
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "protein missing source member {source_molecule_uuid}"
                ))
            })?;

        // Ensure new member is bound.
        if !protein.contains_molecule(&new_member.molecule_uuid) {
            self.protein_add_member(&source_protein, new_member.clone())
                .await?;
        }

        let mut filled = 0usize;
        for fields in records {
            let Some((src_hash, src_range)) = source_member.tip_coords_from_fields(fields) else {
                continue;
            };
            let Some((dst_hash, dst_range)) = new_member.tip_coords_from_fields(fields) else {
                continue;
            };
            let Some(entry) = self
                .protein_get_member_tip(source_molecule_uuid, &src_hash, &src_range)
                .await?
            else {
                continue;
            };
            self.protein_set_member_tip(
                &new_member.molecule_uuid,
                &dst_hash,
                &dst_range,
                &entry,
                keypair,
            )
            .await?;
            filled += 1;
        }
        Ok(filled)
    }

    /// Read tip atom uuid at a member's key coordinates (None if absent).
    pub async fn protein_get_member_tip(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Result<Option<AtomEntry>, SchemaError> {
        let storage_hash = self.storage_hash(molecule_uuid, hash)?;
        let storage_range = self.storage_range(molecule_uuid, range)?;
        let base = crate::atom::molecule_key_codec::hash_range_record_key(
            molecule_uuid,
            &storage_hash,
            &storage_range,
        );
        match self.get_per_key(&base, None).await? {
            Some((_, rec)) => Ok(Some(rec.entry)),
            None => Ok(None),
        }
    }

    // ── internals ──────────────────────────────────────────────────────────

    async fn put_protein(&self, protein: &Protein) -> Result<(), SchemaError> {
        self.raw()
            .put_item(&protein_record_key(&protein.uuid), protein)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("write protein {}: {e}", protein.uuid)))
    }

    /// Set (or create) a tip on a molecule at (hash, range).
    async fn protein_set_member_tip(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        tip: &AtomEntry,
        keypair: &Ed25519KeyPair,
    ) -> Result<(), SchemaError> {
        let (molecule_uuid, mut mol, changed) = self
            .protein_member_tip_update(molecule_uuid, hash, range, tip, keypair, false)
            .await?;
        self.store_molecule_changed_keys(&molecule_uuid, &mol, &changed, None)
            .await?;
        let _ = mol.take_pending_tip_versions();
        Ok(())
    }

    async fn protein_member_tip_update(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        tip: &AtomEntry,
        keypair: &Ed25519KeyPair,
        honor_tip_lww: bool,
    ) -> Result<(String, MoleculeData, HashSet<ChangedKey>), SchemaError> {
        let changed: HashSet<ChangedKey> =
            HashSet::from([ChangedKey::hash_range(hash.to_string(), range.to_string())]);

        let mut mol: MoleculeData = match self
            .load_molecule_for_write(molecule_uuid, None, &changed)
            .await?
        {
            Some(m) => m,
            None => {
                // Brand-new molecule identity for this member.
                MoleculeHashRange::from_write_records(
                    molecule_uuid.to_string(),
                    0,
                    Utc::now(),
                    Vec::new(),
                )
            }
        };

        // Local folds mint a fresh clock via set_atom_uuid_from_values.
        // Imported/replay folds must keep the envelope written_at/writer and
        // LWW-skip when a newer sibling tip already won.
        if honor_tip_lww {
            let writer = if tip.device_id.is_empty() {
                tip.writer_pubkey.clone()
            } else {
                tip.device_id.clone()
            };
            mol.set_atom_uuid_from_values_imported_with_author(
                hash.to_string(),
                range.to_string(),
                tip.atom_uuid.clone(),
                writer,
                tip.writer_pubkey.clone(),
                String::new(),
                0,
                Some(tip.written_at),
                tip.logical_counter,
                tip.mutation_uuid.clone(),
            );
        } else {
            mol.set_atom_uuid_from_values(
                hash.to_string(),
                range.to_string(),
                tip.atom_uuid.clone(),
                keypair,
            );
        }

        Ok((molecule_uuid.to_string(), mol, changed))
    }
}
