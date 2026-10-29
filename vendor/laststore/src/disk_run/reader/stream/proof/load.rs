use super::{corrupt, records, ProofBlock, ProofBounds, ProofHead, RunReader};
use crate::disk_run::bloom::Filter;
use crate::disk_run::footer::BlockRef;
use crate::disk_run::MAX_RUN_KEY_BYTES;
use crate::frame::VerifiedFrameReader;
use crate::{Error, Result};
use std::io::Read;

#[derive(Clone, Copy)]
struct Extent<'a> {
    reference: &'a BlockRef,
    following: Option<&'a [u8]>,
    last: Option<&'a [u8]>,
    metadata: u64,
    filter: &'a Filter<'a>,
}

impl RunReader {
    pub(crate) fn proof_head(
        &self,
        bounds: ProofBounds<'_>,
        allowance: u64,
        retained: u64,
    ) -> Result<Option<ProofHead>> {
        let start = self.index_for(bounds.lower).unwrap_or(0);
        for number in start..self.index_count() {
            let index = self.stream_index(number, allowance)?;
            let first = if number == start {
                index
                    .blocks
                    .partition_point(|b| b.first_key.as_slice() <= bounds.lower)
                    .saturating_sub(1)
            } else {
                0
            };
            let index_bytes = index.blocks.capacity() * std::mem::size_of::<BlockRef>()
                + index
                    .blocks
                    .iter()
                    .map(|block| block.first_key.capacity())
                    .sum::<usize>()
                + index.filter_bytes.capacity();
            let metadata = self.metadata_bytes().saturating_add(index_bytes as u64);
            let filter = Filter::read(&index.filter_bytes, &mut 0)?;
            for (slot, reference) in index.blocks.iter().enumerate().skip(first) {
                if bounds
                    .upper
                    .is_some_and(|upper| reference.first_key.as_slice() >= upper)
                {
                    return Ok(None);
                }
                let following = index
                    .blocks
                    .get(slot + 1)
                    .map(|block| block.first_key.as_slice())
                    .or_else(|| {
                        self.top
                            .indexes
                            .get(number + 1)
                            .map(|block| block.first_key.as_slice())
                    });
                let extent = Extent {
                    reference,
                    following,
                    last: following.is_none().then_some(self.top.last_key.as_slice()),
                    metadata,
                    filter: &filter,
                };
                let mut block =
                    self.proof_block(extent, allowance, retained.saturating_add(metadata))?;
                if let Some(key) = block.next_key(bounds)? {
                    return Ok(Some(ProofHead { key, block }));
                }
            }
        }
        Ok(None)
    }

    fn proof_block(&self, extent: Extent<'_>, allowance: u64, retained: u64) -> Result<ProofBlock> {
        let reference = extent.reference;
        let fixed = (std::mem::size_of::<ProofBlock>() + MAX_RUN_KEY_BYTES) as u64;
        let maximum = allowance
            .checked_sub(fixed)
            .ok_or(Error::ReadBudgetExceeded)?
            .min(self.block_limit() as u64);
        let maximum = usize::try_from(maximum).map_err(|_| Error::ReadBudgetExceeded)?;
        if maximum == 0 {
            return Err(Error::ReadBudgetExceeded);
        }
        if reference.len as usize
            > self
                .block_limit()
                .saturating_add(8 + crate::frame::min_encoded_len())
        {
            return Err(corrupt("stored block length over its limit"));
        }
        if reference.len as usize > maximum.saturating_add(8 + crate::frame::min_encoded_len()) {
            return Err(Error::ReadBudgetExceeded);
        }
        preflight(maximum, fixed, retained)?;
        let identity = self.file.metadata()?;
        let mut input = VerifiedFrameReader::open(
            &self.file,
            reference.offset,
            u64::from(reference.len),
            &self.data_key,
        )?;
        if input.header().chunk_uuid != self.footer.run_id
            || input.header().counter != reference.number
        {
            return Err(corrupt("proof block identity"));
        }
        if input
            .memory_bytes()
            .saturating_add(maximum as u64)
            .saturating_add(fixed)
            .saturating_add(retained)
            > lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes
        {
            return Err(Error::ReadBudgetExceeded);
        }
        let stream_workspace = input.memory_bytes().saturating_add(extent.metadata);
        let count = usize::try_from(input.payload_len()).map_err(|_| Error::ReadBudgetExceeded)?;
        if count > maximum {
            return Err(Error::ReadBudgetExceeded);
        }
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        if payload.capacity() > maximum {
            return Err(Error::ReadBudgetExceeded);
        }
        payload.resize(count, 0);
        input.read_exact(&mut payload)?;
        if input.read(&mut [0; 1])? != 0 {
            return Err(corrupt("proof block trailing bytes"));
        }
        drop(input);
        let key = records::validate(
            &payload,
            &reference.first_key,
            extent.following,
            extent.last,
            extent.filter,
        )?;
        let stream_workspace = stream_workspace.saturating_add(key.capacity() as u64);
        let block = ProofBlock {
            file: self.file.try_clone()?,
            identity,
            payload,
            at: 0,
            key,
            body: None,
            stream_workspace,
        };
        if block.memory_bytes() > allowance {
            return Err(Error::ReadBudgetExceeded);
        }
        block.check()?;
        Ok(block)
    }
}

fn preflight(maximum: usize, fixed: u64, retained: u64) -> Result<()> {
    let resources = lastdb_storage_policy::BACKUP_RESOURCES;
    // Keep the original accepted decoder window independent of cache payload
    // admission. Reserve its full workspace before frame authentication.
    let frame =
        4 * resources.stream_buffer_bytes + 16 + crate::frame::MAX_DECOMPRESSED_FRAME_BYTES as u64;
    if retained
        .saturating_add(frame)
        .saturating_add(maximum as u64)
        .saturating_add(fixed)
        > resources.working_memory_bytes
    {
        return Err(Error::ReadBudgetExceeded);
    }
    Ok(())
}
