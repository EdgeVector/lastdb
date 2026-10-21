use super::JournalWriter;
use crate::disk_pack::MAX_DEPENDENCY_PARTS;
use crate::Result;

impl JournalWriter {
    /// Dependency parts named by the receipt of the last complete directory
    /// boundary. The writer records the count at each publication, so reading
    /// it never touches the receipt file.
    pub fn dependency_parts(&self) -> u64 {
        self.dependencies
    }

    /// True when the last failed maintenance left the writer unable to admit work.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Validate bounded future directory growth before an accepted mutation.
    /// A native caller owns its exclusive complete-write gate through publish.
    pub(crate) fn check_dependency_capacity(&self, additional: u64) -> Result<()> {
        self.check_admission()?;
        if self.tail.last_batch != self.current.frontier {
            return Err(super::super::refuse(
                "complete admission requires a published suffix",
            ));
        }
        let needed = self.dependencies.saturating_add(additional);
        if needed > MAX_DEPENDENCY_PARTS {
            return Err(crate::Error::WriteCapacity(format!(
                "complete mutation exceeds dependency capacity: the receipt names {} parts, \
                 this write needs up to {additional} more, and the limit is {MAX_DEPENDENCY_PARTS}",
                self.dependencies
            )));
        }
        Ok(())
    }
}
