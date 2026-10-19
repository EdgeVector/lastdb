use super::HomeDescriptor;
use crate::disk_pack::{ArtifactBuilder, Current, GroupWriter, Location, MutationVersion};
use crate::Result;
use std::io::Read;
use std::path::Path;

/// Stream a new native home from a complete, held source boundary.
/// Conversion owns source ordering and its concurrent-write handoff separately.
pub struct HomeBuilder {
    descriptor: HomeDescriptor,
    inner: ArtifactBuilder,
}

/// Ordered native group writer; every key must match its derived destination.
pub struct HomeGroupWriter<'a> {
    descriptor: HomeDescriptor,
    group: u16,
    inner: GroupWriter<'a>,
    failed: bool,
}

impl HomeBuilder {
    /// Create an empty LastDB home fenced against writers that omit record-R
    /// reverse edges, and against writers that use another atom record layout.
    /// Generic conversion never grants this derived authority: opaque imported
    /// completeness bytes and atoms can originate from an older writer, and
    /// they sit at older atom keys.
    pub fn create_empty_with_schema_record_refs(
        root: &Path,
        descriptor: HomeDescriptor,
    ) -> Result<Current> {
        let mut builder = Self::create(root, descriptor, 0)?;
        builder.inner.required_features |=
            crate::disk_pack::FEATURE_SCHEMA_RECORD_REFS | crate::disk_pack::FEATURE_ATOM_RECORDS;
        builder.finish()
    }

    /// Create only at a nonexistent destination. Never modify the source home.
    pub fn create(root: &Path, descriptor: HomeDescriptor, frontier: u64) -> Result<Self> {
        descriptor.validate()?;
        let inner = ArtifactBuilder::create_native(root, &descriptor, frontier)?;
        Ok(Self { descriptor, inner })
    }

    /// Select a directory coordinate in complete encoded directory-key byte order.
    pub fn group(&mut self, collection: &str, group: u16) -> Result<HomeGroupWriter<'_>> {
        Ok(HomeGroupWriter {
            descriptor: self.descriptor.clone(),
            group,
            inner: self.inner.group(collection, group)?,
            failed: false,
        })
    }

    /// Publish the complete directory and its immutable dependency receipt.
    pub fn finish(self) -> Result<Current> {
        let current = self.inner.finish()?;
        self.descriptor.check_current(&current)?;
        Ok(current)
    }
}

impl HomeGroupWriter<'_> {
    fn check_group(&mut self, key: &str) -> Result<()> {
        if self.failed {
            return Err(crate::disk_pack::corrupt(
                "native source writer is poisoned",
            ));
        }
        self.failed = true;
        if self.descriptor.group(key)? != self.group {
            return Err(crate::Error::Config(
                "native source key differs from its held placement group".into(),
            ));
        }
        self.failed = false;
        Ok(())
    }

    /// Preserve the complete stored body, physical version, and encoded key.
    pub fn put(
        &mut self,
        key: &str,
        version: MutationVersion,
        body_bytes: u64,
        body: &mut dyn Read,
    ) -> Result<Location> {
        self.check_group(key)?;
        self.inner.put(key, version, body_bytes, body)
    }

    /// Preserve a versioned deletion under its derived group.
    pub fn tombstone(&mut self, key: &str, version: MutationVersion) -> Result<()> {
        self.check_group(key)?;
        self.inner.tombstone(key, version)
    }

    /// Complete this group before the directory can name it.
    pub fn finish(self) -> Result<()> {
        if self.failed {
            return Err(crate::disk_pack::corrupt(
                "refusing failed native source group",
            ));
        }
        self.inner.finish()
    }
}
