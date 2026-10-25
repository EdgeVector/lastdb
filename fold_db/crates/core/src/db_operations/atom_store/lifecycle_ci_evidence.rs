//! Approved one-time generation/cleanup contention, on generated native homes.

use super::{
    counter::TipChangeSet, AtomStore, MoleculeData, MoleculeGenerationDelete, MoleculeKeyDomain,
};
use crate::atom::{molecule_key_codec, AtomEntry};
use crate::schema::{types::field::build_storage_key, SchemaError};
use crate::storage::disk_pack::{lifecycle_ci_pause, PackRuntime};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

type Result<T> = std::result::Result<T, SchemaError>;

fn require(ok: bool, detail: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(SchemaError::InvalidData(detail.into()))
    }
}

impl AtomStore {
    /// Prove both contention directions at the accepted-to-publication window.
    /// No sleep selects the window: native publication and cleanup signal it.
    pub async fn ci_generation_cleanup_evidence(&self, scope: Option<&str>) -> Result<Value> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|s| s.disk_pack_runtime())
            .ok_or_else(|| SchemaError::InvalidData("CI requires native home".into()))?;
        let molecule = crate::atom::deterministic_molecule_uuid(
            "ci-generated-lifecycle",
            &uuid::Uuid::new_v4().to_string(),
        );
        self.ci_seed_markers(&molecule, scope).await?;
        self.ci_prepared_contention(&runtime, &molecule, scope)
            .await?;
        self.ci_cleanup_contention(&runtime, &molecule, scope)
            .await?;
        self.ci_seed_markers(&molecule, scope).await?;
        self.in_background_scope(async {
            let cut = self.prepare_molecule_generation(&molecule, scope).await?;
            self.store_molecule_generations_batch(
                vec![(
                    molecule.clone(),
                    MoleculeData::with_uuid(molecule.clone()),
                    cut,
                )],
                &[],
                scope,
                MoleculeKeyDomain::Storage,
            )
            .await
        })
        .await?;
        let active = self
            .cleanup_delete_markers_page(&molecule, scope, None, 16, true)
            .await?;
        require(
            active["blocked"] == "active_generation" && active["deleted"] == 0,
            "active generation lost markers",
        )?;
        let pointer = build_storage_key(
            scope,
            &molecule_key_codec::molecule_generation_pointer_key(&molecule),
        );
        self.in_background_scope(async {
            self.commit_tip_changes(TipChangeSet {
                scope,
                deletes: vec![pointer.into_bytes()],
                ..TipChangeSet::default()
            })
            .await?;
            Ok(())
        })
        .await?;
        let mut cursor = None;
        let mut deleted = 0;
        for _ in 0..5 {
            let preview = self
                .cleanup_delete_markers_page(&molecule, scope, cursor.as_deref(), 16, false)
                .await?;
            let page = self
                .cleanup_delete_markers_page(&molecule, scope, cursor.as_deref(), 16, true)
                .await?;
            require(
                page["blocked"].is_null()
                    && preview["deleted"] == 0
                    && page["deleted"] == preview["eligible"],
                "marker page differs from preview",
            )?;
            deleted += page["deleted"].as_u64().unwrap_or(0);
            if page["complete"] == true {
                break;
            }
            cursor = page["next_cursor"].as_str().map(str::to_string);
        }
        require(deleted == 40, "marker continuation lost generated keys")?;
        let repeat = self
            .cleanup_delete_markers_page(&molecule, scope, None, 16, true)
            .await?;
        require(
            repeat["complete"] == true && repeat["deleted"] == 0,
            "marker repeat changed state",
        )?;
        Ok(json!({"scope": scope, "prepared_publication_guard": true,
            "cleanup_retry_before_acceptance": true, "private_stage_rolled_back": true,
            "active_generation": active, "deleted": deleted, "repeat": repeat}))
    }

    async fn ci_seed_markers(&self, molecule: &str, scope: Option<&str>) -> Result<()> {
        let shadow = AtomEntry::thin(uuid::Uuid::new_v4().to_string(), 1, "ci-generated".into());
        let marker = serde_json::to_value(MoleculeGenerationDelete { shadow })
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let rows = (0..40)
            .map(|n| {
                (
                    build_storage_key(
                        scope,
                        &molecule_key_codec::molecule_generation_delete_key(
                            molecule,
                            &format!("old-{n:04}"),
                            "",
                        ),
                    ),
                    marker.clone(),
                )
            })
            .collect();
        self.in_background_scope(async {
            self.commit_tip_changes(TipChangeSet {
                scope,
                rows,
                ..TipChangeSet::default()
            })
            .await?;
            Ok(())
        })
        .await
    }

    async fn ci_prepared_contention(
        &self,
        runtime: &Arc<PackRuntime>,
        molecule: &str,
        scope: Option<&str>,
    ) -> Result<()> {
        let pause = lifecycle_ci_pause::arm(runtime);
        let prepare = self.in_background_scope(async {
            let cut = self.prepare_molecule_generation(molecule, scope).await?;
            drop(cut);
            require(
                self.generation_cleanup_lock(molecule, scope)
                    .try_write_owned()
                    .is_err(),
                "prepared guard ended before publication",
            )?;
            self.ci_seed_markers(molecule, scope).await
        });
        let contend = async {
            pause.reached.notified().await;
            let cleanup = self.cleanup_delete_markers_page(molecule, scope, None, 16, true);
            let release = async {
                pause.cleanup_blocked.notified().await;
                pause.resume.notify_one();
            };
            let (page, ()) = tokio::join!(cleanup, release);
            let page = page?;
            require(
                page["blocked"] == "generation_preparation" && page["deleted"] == 0,
                "cleanup did not retain prepared-generation markers",
            )
        };
        let joined = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::try_join!(prepare, contend)
        })
        .await;
        pause.resume.notify_one();
        joined.map_err(|_| {
            SchemaError::InvalidData("prepared/cleanup publication deadlock".into())
        })??;
        let guard = tokio::time::timeout(
            Duration::from_secs(5),
            self.generation_cleanup_lock(molecule, scope).write_owned(),
        )
        .await
        .map_err(|_| SchemaError::InvalidData("prepared guard leaked after publication".into()))?;
        drop(guard);
        Ok(())
    }

    async fn ci_cleanup_contention(
        &self,
        runtime: &Arc<PackRuntime>,
        molecule: &str,
        scope: Option<&str>,
    ) -> Result<()> {
        let pause = lifecycle_ci_pause::arm(runtime);
        let probe = build_storage_key(scope, &format!("ci-refused:{molecule}"));
        let cleanup = self.cleanup_delete_markers_page(molecule, scope, None, 64, true);
        let contend = async {
            pause.reached.notified().await;
            let result = self
                .in_write_scope(async {
                    self.commit_tip_changes(TipChangeSet {
                        scope,
                        rows: vec![(probe.clone(), json!(true))],
                        ..TipChangeSet::default()
                    })
                    .await?;
                    self.prepare_molecule_generation(molecule, scope)
                        .await
                        .map(drop)
                })
                .await;
            pause.resume.notify_one();
            require(
                matches!(result, Err(SchemaError::WriteResourceLimit { .. })),
                "generation preparation waited instead of refusing before acceptance",
            )
        };
        let joined = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::try_join!(cleanup, contend)
        })
        .await;
        pause.resume.notify_one();
        let (page, ()) = joined.map_err(|_| {
            SchemaError::InvalidData("cleanup/generation publication deadlock".into())
        })??;
        require(
            page["deleted"] == 40,
            "cleanup failed after generation refusal",
        )?;
        require(
            self.main_store.get_item::<Value>(&probe).await?.is_none(),
            "refused private stage reached disk",
        )
    }
}
