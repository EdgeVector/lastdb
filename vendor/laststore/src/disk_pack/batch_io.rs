//! Exact adjacent frames share one bounded file read; gaps are never read.

use super::block_plan::{BlockId, BlockPlan};
use super::ReadCall;
use crate::Error;
use std::collections::BTreeMap;

const MAX_COALESCED_BYTES: usize = 64 * 1024;

impl ReadCall {
    pub(super) fn read_planned_blocks(
        &mut self,
        plans: Vec<BlockPlan>,
        failures: &mut BTreeMap<BlockId, Error>,
    ) {
        let mut first = 0;
        while first < plans.len() {
            let mut last = first + 1;
            let mut length = plans[first].stored;
            while let Some(next) = plans.get(last) {
                if next.path != plans[first].path
                    || next.offset != plans[first].offset + length as u64
                    || length + next.stored > MAX_COALESCED_BYTES
                {
                    break;
                }
                length += next.stored;
                last += 1;
            }
            self.read_block_run(&plans[first..last], length, failures);
            first = last;
        }
    }

    fn read_block_run(
        &mut self,
        plans: &[BlockPlan],
        length: usize,
        failures: &mut BTreeMap<BlockId, Error>,
    ) {
        let result = self.read_extent(
            &plans[0].path,
            plans[0].offset,
            length,
            false,
            self.payload_class,
        );
        self.costs.batch_read_runs += 1;
        crate::read_receipt::update(|receipt| {
            receipt.pack_batch_read_runs += 1;
            receipt.pack_batch_frames += plans.len() as u64;
        });
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => {
                for plan in plans {
                    failures.insert(plan.identity, super::batch_error::copy(&error));
                }
                return;
            }
        };
        let mut start = 0;
        for plan in plans {
            let end = start + plan.stored;
            if let Err(error) = self.decode_planned(plan, &bytes.bytes[start..end]) {
                failures.insert(plan.identity, error);
            }
            start = end;
        }
    }
}
