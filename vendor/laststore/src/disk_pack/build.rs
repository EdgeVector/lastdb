use super::pack_write::PackWriter;
use super::page_write::directory;
use super::tree_write::TreeWriter;
use super::{
    corrupt, Current, DirectoryEntry, Location, MutationVersion, ReadCall, Source, TreeKind,
};
use crate::{durability, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Streaming construction of a separate immutable artifact, never a LastStore home.
/// The artifact retains every file. No journal, online writer, or reclamation is enabled.
pub struct ArtifactBuilder {
    root: PathBuf,
    directory: TreeWriter,
    generation: u64,
    frontier: u64,
    placement_codec: u16,
    key_codec: u16,
    failed: bool,
}

/// Streaming writer for strictly ordered full keys in one collection and group.
pub struct GroupWriter<'a> {
    owner: &'a mut ArtifactBuilder,
    collection: String,
    group: u16,
    pack: Option<PackWriter>,
    locator: Option<TreeWriter>,
    finished: bool,
}

impl ArtifactBuilder {
    /// Create a new artifact at a nonexistent path whose parent already exists.
    /// Version values must originate from a complete held source boundary.
    pub fn create(
        root: &Path,
        placement_codec: u16,
        key_codec: u16,
        generation: u64,
        frontier: u64,
    ) -> Result<Self> {
        if generation == 0 {
            return Err(corrupt("artifact creation generation is zero"));
        }
        directory(root)?;
        let mut marker = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(root.join("disk-pack-artifact-v1"))?;
        marker.write_all(b"artifact-only=1\nformat=1\n")?;
        durability::sync_dirty_file(&marker)?;
        for name in ["directory", "locator", "packs", "journal"] {
            directory(&root.join(name))?;
        }
        let directory = TreeWriter::new(root, TreeKind::Directory, generation)?;
        Ok(Self {
            root: root.to_path_buf(),
            directory,
            generation,
            frontier,
            placement_codec,
            key_codec,
            failed: false,
        })
    }

    /// Start one addressed group. Completed groups must follow directory-key byte order.
    /// Only one group writer can borrow this builder at a time.
    pub fn group(&mut self, collection: &str, group: u16) -> Result<GroupWriter<'_>> {
        if self.failed {
            return Err(corrupt(
                "artifact builder requires a fresh identity after failure",
            ));
        }
        self.failed = true;
        ReadCall::directory_key(collection, group)?;
        let pack = PackWriter::new(&self.root, self.generation)?;
        let locator = TreeWriter::new(&self.root, TreeKind::Locator, self.generation)?;
        self.failed = false;
        Ok(GroupWriter {
            owner: self,
            collection: collection.to_string(),
            group,
            pack: Some(pack),
            locator: Some(locator),
            finished: false,
        })
    }

    /// Finish all dependencies and publish the initial CURRENT with file and directory barriers.
    /// Failed artifacts remain separate and never replace an existing readable artifact.
    pub fn finish(self) -> Result<Current> {
        if self.failed {
            return Err(corrupt(
                "refusing incomplete or failed artifact publication",
            ));
        }
        let directory = self.directory.finish()?;
        let current = Current {
            generation: self.generation,
            previous_generation: 0,
            frontier: self.frontier,
            journal_frontier: self.frontier,
            directory,
            placement_codec: self.placement_codec,
            key_codec: self.key_codec,
            journal_stream: Uuid::new_v4(),
            published_ms: unix_ms(),
            required_features: super::FEATURE_OBJECT_METADATA,
        };
        let temporary = self.root.join("CURRENT.pending");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&current.encode()?)?;
        durability::sync_dirty_file(&file)?;
        fs::rename(&temporary, self.root.join("CURRENT"))?;
        durability::sync_directory(&File::open(&self.root)?)?;
        Ok(current)
    }
}

impl GroupWriter<'_> {
    /// Copy unchanged stored body bytes into independent blocks; never create a new atom.
    pub fn put(
        &mut self,
        key: &str,
        version: MutationVersion,
        body_bytes: u64,
        body: &mut dyn Read,
    ) -> Result<Location> {
        let result = self.put_inner(key, version, body_bytes, body);
        if result.is_err() {
            self.owner.failed = true;
        }
        result
    }

    fn put_inner(
        &mut self,
        key: &str,
        version: MutationVersion,
        body_bytes: u64,
        body: &mut dyn Read,
    ) -> Result<Location> {
        self.check_version(version)?;
        let location = self.pack.as_mut().expect("active group").put(
            &self.collection,
            key,
            version,
            body_bytes,
            body,
        )?;
        self.locator
            .as_mut()
            .expect("active group")
            .add(key.as_bytes(), location.encode()?)?;
        Ok(location)
    }

    /// Preserve a deletion at its original physical mutation version.
    pub fn tombstone(&mut self, key: &str, version: MutationVersion) -> Result<()> {
        let result = self.tombstone_inner(key, version);
        if result.is_err() {
            self.owner.failed = true;
        }
        result
    }

    fn tombstone_inner(&mut self, key: &str, version: MutationVersion) -> Result<()> {
        self.check_version(version)?;
        let location = Location {
            source: Source::Tombstone,
            version,
            record_bytes: 0,
            object: Uuid::nil(),
            first_block: 0,
            first_position: 0,
            blocks: 0,
            digest: [0; 32],
        };
        self.locator
            .as_mut()
            .expect("active group")
            .add(key.as_bytes(), location.encode()?)
    }

    fn check_version(&self, version: MutationVersion) -> Result<()> {
        if self.owner.failed {
            return Err(corrupt(
                "artifact builder requires a fresh identity after failure",
            ));
        }
        if version.batch > self.owner.frontier {
            return Err(corrupt("source version exceeds held boundary"));
        }
        Ok(())
    }

    /// Make the complete group durable before its directory value can be published.
    pub fn finish(mut self) -> Result<()> {
        if self.owner.failed {
            return Err(corrupt("refusing failed group publication"));
        }
        self.pack.take().expect("active group").finish()?;
        let locator = self.locator.take().expect("active group").finish()?;
        let value = DirectoryEntry {
            group: self.group,
            locator,
            converted_frontier: self.owner.frontier,
        }
        .encode()?;
        let key = ReadCall::directory_key(&self.collection, self.group)?;
        self.owner.directory.add(&key, value)?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for GroupWriter<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.owner.failed = true;
        }
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
