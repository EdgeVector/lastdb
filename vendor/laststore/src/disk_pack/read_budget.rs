//! A larger finite metadata allowance exists only under the extended-key fence.

use super::{Current, ReadBudget, FEATURE_EXTENDED_KEYS};
use crate::Result;
use lastdb_storage_policy::native_read::{EAGER_BLOCKS, EAGER_WORK_BYTES};

impl ReadBudget {
    /// Validate the original artifact and legacy-home resource ceilings.
    pub fn validate(self) -> Result<()> {
        self.validate_for_features(0)
    }

    /// Administrative defaults for this exact captured format. No format is changed.
    pub fn for_current(current: &Current) -> Self {
        if current.required_features & FEATURE_EXTENDED_KEYS != 0 {
            Self {
                pages: 32768,
                blocks: EAGER_BLOCKS,
                bytes: EAGER_WORK_BYTES,
                keys: 10000,
            }
        } else {
            Self {
                pages: 4096,
                blocks: 16384,
                bytes: 256 * 1024 * 1024,
                keys: 10000,
            }
        }
    }

    pub(super) fn validate_for_features(self, features: u64) -> Result<()> {
        let pages = if features & FEATURE_EXTENDED_KEYS != 0 {
            32768
        } else {
            4096
        };
        if self.pages == 0
            || self.blocks == 0
            || self.bytes == 0
            || self.keys == 0
            || self.pages > pages
            || self.blocks > EAGER_BLOCKS
            || self.bytes > EAGER_WORK_BYTES
            || self.keys > 10000
        {
            return Err(crate::Error::Config(format!(
                "disk-pack read budget requires 1..{pages} pages, 1..{EAGER_BLOCKS} blocks, 1..{EAGER_WORK_BYTES} bytes, and 1..10000 keys"
            )));
        }
        Ok(())
    }
}
