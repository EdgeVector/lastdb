//! Fixed-size generations fence cache admission across a concurrent write.

use super::*;

#[derive(Clone, Copy)]
pub struct ReadEpochs([u64; RECORD_EPOCH_STRIPES]);

impl LogicalResidentSet {
    pub fn read_epochs(&self) -> ReadEpochs {
        ReadEpochs(self.record_epochs)
    }

    pub fn read_epochs_match(&self, epochs: ReadEpochs) -> bool {
        self.record_epochs == epochs.0
    }

    pub(super) fn advance_read_epochs(&mut self) {
        for epoch in &mut self.record_epochs {
            *epoch = epoch.wrapping_add(1);
        }
    }
}
