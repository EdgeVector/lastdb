//! Retained backup inventory and capacity stay owned through the complete copy.

use super::manifest::{Entry, Manifest};
use super::{files, BackupReceipt};
use crate::disk_pack::home::{DiskHome, HomeSnapshot};
use crate::disk_pack::lifetime::{
    DurableReference, GenerationHold, GenerationInventory, ReferencePurpose,
};
use crate::disk_pack::maintenance::{SpacePolicy, SpaceReservation};
use crate::disk_pack::{corrupt, Current};
use crate::Result;
use std::path::{Path, PathBuf};

/// Exact generation files retained before a serving writer releases its gate.
/// Use `reserve` before copying. A rejected reservation releases its reference.
pub struct PreparedBackup {
    reference: DurableReference,
    hold: GenerationHold,
    current: Current,
    inventory: GenerationInventory,
    destination: PathBuf,
    required_bytes: u64,
}

/// A complete copy owns both its source-generation hold and output reservation.
pub struct ReservedBackup {
    prepared: PreparedBackup,
    _space: SpaceReservation,
    free_space_reserve_bytes: u64,
}

/// Capture one caller-owned backup reference and compute its bounded output cost.
/// Existing destinations, nested outputs, and a reference from another home fail.
pub fn prepare_referenced_backup(
    home: &DiskHome,
    reference: DurableReference,
    destination: &Path,
) -> Result<PreparedBackup> {
    let prepared = (|| {
        if std::fs::canonicalize(reference.home_path())? != home.path()
            || reference.info().purpose != ReferencePurpose::Backup
        {
            return Err(corrupt(
                "backup requires this home's owned backup reference",
            ));
        }
        let destination = files::destination(destination)?;
        if destination.starts_with(home.path()) {
            return Err(corrupt("backup destination is inside the source home"));
        }
        let current = reference.current().clone();
        let hold = reference.hold()?;
        let inventory = hold.inventory()?;
        if inventory.current.home_digest
            != crate::disk_pack::bytes::digest(&home.descriptor().encode()?)
        {
            return Err(corrupt(
                "backup reference belongs to a different native home",
            ));
        }
        let required_bytes = quote(&inventory)?;
        Ok((destination, current, hold, inventory, required_bytes))
    })();
    match prepared {
        Ok((destination, current, hold, inventory, required_bytes)) => Ok(PreparedBackup {
            reference,
            hold,
            current,
            inventory,
            destination,
            required_bytes,
        }),
        Err(error) => {
            reference.release()?;
            Err(error)
        }
    }
}

impl PreparedBackup {
    /// Conservative allocated output, including the manifest and directories.
    pub fn required_bytes(&self) -> u64 {
        self.required_bytes
    }

    /// Reserve through the serving writer before the source write gate releases.
    pub fn reserve(self, writer: &HomeSnapshot, policy: &SpacePolicy) -> Result<ReservedBackup> {
        let result = if writer.current().home_digest == self.current.home_digest {
            writer.reserve_space(self.required_bytes, policy)
        } else {
            Err(corrupt(
                "backup reservation writer belongs to a different home",
            ))
        };
        match result {
            Ok(space) => Ok(ReservedBackup {
                prepared: self,
                _space: space,
                free_space_reserve_bytes: policy.free_space_reserve_bytes,
            }),
            Err(error) => {
                self.reference.release()?;
                Err(error)
            }
        }
    }
}

impl ReservedBackup {
    /// Copy immutable bytes after the writer gate releases. Cancellation of the
    /// caller does not release resources owned by the running blocking operation.
    pub fn create(self) -> Result<BackupReceipt> {
        let prepared = self.prepared;
        let staging = (|| {
            files::reserve_output(
                &prepared.destination,
                prepared.required_bytes,
                self.free_space_reserve_bytes,
            )?;
            files::staging(&prepared.destination)
        })();
        let staging = match staging {
            Ok(staging) => staging,
            Err(error) => {
                prepared.reference.release()?;
                return Err(error);
            }
        };
        let result = super::export(
            prepared.reference.home_path(),
            &staging,
            &prepared.current,
            prepared.inventory.files,
        )
        .and_then(|receipt| files::publish(&staging, &prepared.destination).map(|()| receipt));
        let receipt = result.map_err(|error| {
            crate::Error::Config(format!(
                "backup incomplete at {}; retained reference {}: {error}",
                staging.display(),
                prepared.reference.info().identity
            ))
        })?;
        drop(prepared.hold);
        prepared.reference.release()?;
        Ok(receipt)
    }
}

fn quote(inventory: &GenerationInventory) -> Result<u64> {
    let mut entries: Vec<_> = inventory
        .files
        .iter()
        .map(|file| Entry {
            path: file.path.clone(),
            bytes: file.bytes,
            digest: [0; 32],
        })
        .collect();
    entries.push(Entry {
        path: "CURRENT".into(),
        bytes: 4096,
        digest: [0; 32],
    });
    Manifest {
        generation: inventory.current.generation,
        frontier: inventory.current.frontier,
        entries,
    }
    .output_bytes()
}
