//! Renewal uses only space left after every required warm copy is reserved.

use super::*;
use laststore::disk_pack::home::HomeRead;

pub(super) struct Refresh {
    witness: Option<u64>,
    records: u64,
    bytes: u64,
}

impl Refresh {
    pub(super) fn new(
        context: &ReadContext,
        call: &HomeRead,
        records: u64,
        bytes: u64,
        needed: bool,
    ) -> Option<Self> {
        if !needed {
            return None;
        }
        context.inherit_validated();
        let witness = context.witness()?;
        context.validated(witness)?;
        let costs = call.costs();
        let limits = call.budget();
        Some(Self {
            witness: Some(witness),
            records: limits
                .keys
                .checked_sub(costs.examined_keys)?
                .checked_sub(records)?,
            bytes: limits
                .bytes
                .checked_sub(costs.budget_bytes)?
                .checked_sub(bytes)?,
        })
    }

    pub(super) fn record(
        &mut self,
        context: &ReadContext,
        call: &mut HomeRead,
        logical: &mut LogicalResidentSet,
        id: &str,
        body: &mut PathBody,
    ) -> StorageResult<()> {
        let Some((version, value)) = body.provenance.as_mut() else {
            return Ok(());
        };
        if version.revision == context.version.revision
            && body
                .wire
                .as_ref()
                .is_none_or(|wire| wire.version.revision == context.version.revision)
        {
            return Ok(());
        }
        if !context.refresh_molecule_version(body.witness, value) {
            return Ok(());
        }
        *version = context.version;
        body.witness = self.witness;
        if let Some(wire) = body.wire.as_mut() {
            wire.version = context.version;
            wire.witness = self.witness;
        }
        let bytes = id.len().saturating_add(projection::encoded_len(body)?) as u64;
        if self.records == 0 || bytes > self.bytes {
            return Ok(());
        }
        let Some(encoded) = projection::encode_optional(call, id, body)? else {
            return Ok(());
        };
        self.records -= 1;
        self.bytes -= bytes;
        let epoch = logical.record_epoch(id);
        logical.admit_record(PATH_COLLECTION, id, Some(encoded), epoch);
        Ok(())
    }
}
