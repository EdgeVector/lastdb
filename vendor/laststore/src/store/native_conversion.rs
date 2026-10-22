//! Explicit conversion, with a short final source-write freeze and a durable fence.

use super::*;
use crate::disk_pack::conversion_fence as fence;
use crate::disk_pack::home::{DiskHome, HomeBuilder, HomeDescriptor, HomeMutation};
use crate::disk_pack::lifetime::{DurableReference, ReferencePurpose};
use crate::disk_pack::MutationVersion;
use std::sync::MutexGuard;

pub(super) const SOURCE_FENCE: &str = fence::FORWARD_FENCE;
const COPY_PAGE: usize = 16;
const MAX_RECORDS: u64 = 1_000_000;
const MAX_CATCHUP_KEYS: usize = 10_000;

/// An explicit conversion boundary. Stored bodies remain byte-for-byte opaque.
#[derive(Debug, Clone)]
pub struct ConversionReceipt {
    /// Live source records encountered during the bounded copy.
    pub copied_records: u64,
    /// Distinct changed keys replayed at the final source boundary.
    pub catchup_keys: u64,
    /// Source commit counter while all mutation stripes are held.
    pub source_csn: u64,
    /// Complete native root after catchup publication.
    pub generation: u64,
    /// Last complete native snapshot batch after catchup.
    pub frontier: u64,
}

/// Source writes remain frozen until explicit cutover or cancellation.
/// Dropping this guard leaves the source authoritative and the target dormant.
pub struct ConversionCutover<'a> {
    source: &'a LastStore,
    destination: PathBuf,
    receipt: ConversionReceipt,
    reference: DurableReference,
    token: Uuid,
    _writer: crate::disk_pack::home::HomeSnapshot,
    _conversion: MutexGuard<'a, ()>,
    _guards: Vec<MutexGuard<'a, ()>>,
}

impl LastStore {
    /// Copy a supported legacy source while ordinary accepted writes continue.
    /// Capture changed keys, freeze all mutation stripes, then publish their final
    /// values in complete batches. Caller owns source-process and admin exclusion.
    pub fn prepare_native_conversion(&self, destination: &Path) -> Result<ConversionCutover<'_>> {
        let conversion = self
            .conversion_gate
            .try_lock()
            .map_err(|_| Error::Config("source conversion already active".into()))?;
        self.check_conversion_write()?;
        let layout = crate::describe_home(&self.root)?.ok_or_else(|| {
            Error::Config("conversion requires a durable layout descriptor".into())
        })?;
        let descriptor = HomeDescriptor::from_layout(&layout)?;
        let start_guards: Vec<_> = self
            .transaction_gates
            .iter()
            .map(|gate| gate.lock().expect("poison"))
            .collect();
        let floor = self.csn_high_water();
        drop(start_guards);
        let mut builder = HomeBuilder::create(destination, descriptor, 0)?;
        let token = fence::pending(destination)?;
        let copied_records = self.copy_native(&mut builder)?;
        builder.finish()?;
        let guards = self
            .transaction_gates
            .iter()
            .map(|gate| gate.lock().expect("poison"))
            .collect();
        self.check_conversion_write()?;
        let changed = self.conversion_changes(floor)?;
        self.flush()?;
        let home = DiskHome::open_conversion(destination)?;
        let mut writer = home.snapshot_conversion(token)?;
        self.register_native_namespaces(&mut writer)?;
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        for (collection, key) in &changed {
            let body = self.get(collection, key)?;
            let bytes = body.as_ref().map_or(0, Vec::len) + collection.len() + key.len() + 256;
            if !batch.is_empty() && (batch.len() == 4096 || batch_bytes + bytes > 64 * 1024 * 1024)
            {
                commit(&mut writer, std::mem::take(&mut batch))?;
                batch_bytes = 0;
            }
            batch_bytes += bytes;
            batch.push(HomeMutation {
                collection: collection.clone(),
                key: key.clone(),
                body,
            });
        }
        if !batch.is_empty() {
            commit(&mut writer, batch)?;
        }
        let current = writer.current().clone();
        let reference =
            DurableReference::create(destination, &current, ReferencePurpose::ConversionSource, 0)?;
        let receipt = ConversionReceipt {
            copied_records,
            catchup_keys: changed.len() as u64,
            source_csn: self.csn_high_water(),
            generation: current.generation,
            frontier: current.frontier,
        };
        Ok(ConversionCutover {
            source: self,
            destination: std::fs::canonicalize(destination)?,
            receipt,
            reference,
            token,
            _writer: writer,
            _conversion: conversion,
            _guards: guards,
        })
    }

    pub(super) fn check_conversion_write(&self) -> Result<()> {
        if self.conversion_sealed.load(Ordering::Acquire) {
            return Err(Error::Config(
                "legacy source is sealed after native conversion".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn open_pending_conversion(root: &Path) -> Result<Self> {
        let options = resolve_layout_options(root, LastStoreOptions::default(), false)?;
        Self::open_conversion(root.to_path_buf(), options, true)
    }

    fn register_native_namespaces(
        &self,
        writer: &mut crate::disk_pack::home::HomeSnapshot,
    ) -> Result<()> {
        let mut names = self.collections_on_disk()?;
        names.push("main".into());
        names.sort();
        names.dedup();
        if names.len() > 10_000 {
            return Err(Error::WriteCapacity("conversion namespace limit".into()));
        }
        for names in names.chunks(4096) {
            let operations = names
                .iter()
                .filter(|name| name.as_str() != "__native_home")
                .map(|name| HomeMutation {
                    collection: "__native_home".into(),
                    key: format!("namespaces\0{name}"),
                    body: Some(vec![1]),
                })
                .collect::<Vec<_>>();
            if !operations.is_empty() {
                commit(writer, operations)?;
            }
        }
        Ok(())
    }

    fn conversion_changes(&self, floor: u64) -> Result<BTreeSet<(String, String)>> {
        let meta = self.meta.lock().expect("poison");
        if meta.capture_suspended {
            return Err(Error::Config("conversion capture is suspended".into()));
        }
        let mut changed = BTreeSet::new();
        for event in meta.capture_log.iter().filter(|event| event.csn > floor) {
            changed.insert((event.collection.clone(), event.id.clone()));
            if changed.len() > MAX_CATCHUP_KEYS {
                return Err(Error::WriteCapacity(
                    "conversion catchup exceeds 10000 keys".into(),
                ));
            }
        }
        Ok(changed)
    }

    fn copy_native(&self, builder: &mut HomeBuilder) -> Result<u64> {
        let mut collections = self.collections_on_disk()?;
        collections.sort_by_cached_key(|name| {
            let mut bytes = (name.len() as u16).to_le_bytes().to_vec();
            bytes.extend_from_slice(name.as_bytes());
            bytes
        });
        let mut total = 0u64;
        for collection in collections {
            let mut handles = self.walk_all_group_handles(&collection, AllGroupsPurpose::Admin)?;
            handles.sort_by_key(|(_, group)| group.unwrap_or(0).to_le_bytes());
            for handle in handles {
                let group = handle
                    .1
                    .ok_or_else(|| Error::Config("conversion source has no hash group".into()))?;
                let mut writer = builder.group(
                    &collection,
                    group
                        .try_into()
                        .map_err(|_| Error::Config("source group exceeds native limit".into()))?,
                )?;
                let mut cursor = None;
                loop {
                    let page = self.walk_all_groups_in_handles(
                        &collection,
                        ("", None),
                        cursor.as_ref(),
                        COPY_PAGE,
                        1,
                        &[handle],
                    )?;
                    for (key, body) in page.rows {
                        total += 1;
                        if total > MAX_RECORDS {
                            return Err(Error::WriteCapacity(
                                "conversion exceeds one million records".into(),
                            ));
                        }
                        writer.put(
                            &key,
                            MutationVersion {
                                batch: 0,
                                operation: 0,
                            },
                            body.len() as u64,
                            &mut body.as_slice(),
                        )?;
                    }
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                writer.finish()?;
            }
        }
        Ok(total)
    }
}

impl ConversionCutover<'_> {
    /// Complete proposed boundary; the source remains frozen until this lease ends.
    pub fn receipt(&self) -> &ConversionReceipt {
        &self.receipt
    }

    /// Fence the old source before its write freeze ends. Never overwrite or
    /// delete source data. Reopen refuses this source; reverse conversion creates
    /// a new legacy home from the latest native generation.
    pub fn commit(self) -> Result<ConversionReceipt> {
        self.source.conversion_sealed.store(true, Ordering::Release);
        let value = serde_json::json!({
            "format":1,"destination":self.destination,"source_csn":self.receipt.source_csn,
            "generation":self.receipt.generation,"frontier":self.receipt.frontier,
            "token":self.token.to_string(),"reference":self.reference.info().identity.to_string(),
            "current_digest":fence::digest(&self._writer.current().encode()?)
        });
        fence::legacy_downgrade_fence(&self.source.root, &value)?;
        fence::publish(&self.source.root, SOURCE_FENCE, &value)?;
        fence::finish(&self.destination, self.token)?;
        self.reference.release()?;
        Ok(self.receipt)
    }
}

fn commit(
    writer: &mut crate::disk_pack::home::HomeSnapshot,
    operations: Vec<HomeMutation>,
) -> Result<()> {
    let reservation = writer.reserve_mutation(&operations)?;
    writer.commit_mutation(operations, 0, reservation)?;
    Ok(())
}
