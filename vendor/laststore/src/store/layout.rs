use super::*;

/// The durable physical layout a home is recorded with.
///
/// Read it with [`describe_home`], which never opens the store.
#[derive(Clone, Copy, Debug)]
pub struct LayoutDescriptor {
    /// Point-document layout mode.
    pub layout_mode: LayoutMode,
    /// High hash bits used for sharding.
    pub shard_bits: u8,
    /// Low hash bits used for hash-group placement.
    pub hash_group_bits: u8,
    /// Hash algorithm behind shard and group placement.
    pub hash_algo: HashAlgo,
    /// Which part of an id decides its shard and hash group.
    pub hash_group_key: HashGroupKey,
    /// How many groups one partition may occupy.
    pub hash_group_partition_fanout: u32,
    /// Layout epoch, bumped by each migration.
    pub layout_epoch: u32,
    /// On-disk packaging of segment and tail files.
    pub packaging: PackagingMode,
}

impl LayoutDescriptor {
    /// Total hash groups this home can place rows into.
    ///
    /// `0` for [`LayoutMode::SegmentLog`], which has no groups.
    pub fn group_count(&self) -> u32 {
        match self.layout_mode {
            LayoutMode::SegmentLog => 0,
            LayoutMode::HashGroup => 1u32 << self.hash_group_bits,
        }
    }

    /// Groups one HashRange partition read has to visit.
    ///
    /// Under [`HashGroupKey::PartitionPrefix`] a partition is pinned to its
    /// `hash_group_partition_fanout` groups and the walk resolves them
    /// directly. Under [`HashGroupKey::FullKey`] the partition's rows scatter
    /// across every group, so the walk must sweep all of them — the read
    /// amplification [`prunes_partition_reads`](Self::prunes_partition_reads)
    /// reports on.
    pub fn groups_per_partition_read(&self) -> u32 {
        match self.layout_mode {
            LayoutMode::SegmentLog => 0,
            LayoutMode::HashGroup => match self.hash_group_key {
                HashGroupKey::PartitionPrefix => {
                    self.hash_group_partition_fanout.min(self.group_count())
                }
                HashGroupKey::FullKey => self.group_count(),
            },
        }
    }

    /// Whether a HashRange partition read can resolve its groups instead of
    /// sweeping the whole collection.
    ///
    /// When this is `false` on a populated home, every sanctioned partition
    /// read pays a full-collection sweep regardless of its `limit`. Adopting
    /// partition-prefix placement is a migration, not a reopen — see the
    /// `migrate-hash-group` relayout.
    pub fn prunes_partition_reads(&self) -> bool {
        self.layout_mode == LayoutMode::HashGroup
            && self.hash_group_key == HashGroupKey::PartitionPrefix
    }
}

impl LayoutDescriptor {
    fn apply_to(self, opts: &mut LastStoreOptions) {
        opts.layout_mode = self.layout_mode;
        opts.shard_bits = self.shard_bits;
        opts.hash_group_bits = self.hash_group_bits;
        opts.hash_algo = self.hash_algo;
        opts.hash_group_key = self.hash_group_key;
        opts.hash_group_partition_fanout = self.hash_group_partition_fanout;
        opts.layout_epoch = self.layout_epoch;
        opts.packaging = self.packaging;
        // Frame AEAD homes keep a caller-supplied data_key; plain packaging
        // must never retain a leftover key from the request options.
        if self.packaging == PackagingMode::Plain {
            opts.data_key = None;
        }
    }

    fn matches(self, opts: &LastStoreOptions) -> bool {
        self.layout_mode == opts.layout_mode
            && self.shard_bits == opts.shard_bits
            && self.hash_group_bits == opts.hash_group_bits
            && self.hash_algo == opts.hash_algo
            && self.hash_group_key == opts.hash_group_key
            && self.hash_group_partition_fanout == opts.hash_group_partition_fanout
            && self.layout_epoch == opts.layout_epoch
            && self.packaging == opts.packaging
    }
}

pub(super) fn resolve_layout_options(
    root: &Path,
    mut requested: LastStoreOptions,
    explicit: bool,
) -> Result<LastStoreOptions> {
    apply_legacy_data_key_packaging(&mut requested);
    if let Some(recorded) = read_layout_descriptor(root)? {
        if explicit && !recorded.matches(&requested) {
            return Err(Error::Config(format!(
                "requested layout does not match durable descriptor at {}",
                root.join(LAYOUT_FILE).display()
            )));
        }
        recorded.apply_to(&mut requested);
        apply_hash_group_product_warm_default(&mut requested);
        return Ok(requested);
    }

    if let Some(detected) = detect_existing_layout(root)? {
        if explicit && requested.layout_mode != detected {
            return Err(Error::Config(format!(
                "requested {:?} layout conflicts with existing {:?} files at {}",
                requested.layout_mode,
                detected,
                root.display()
            )));
        }
        requested.layout_mode = detected;
    }
    if let Some(packaging) = detect_packaging_mode(root) {
        if explicit && requested.packaging != packaging {
            return Err(Error::Config(format!(
                "requested packaging {:?} conflicts with existing {:?} at {}",
                requested.packaging,
                packaging,
                root.display()
            )));
        }
        requested.packaging = packaging;
        if packaging == PackagingMode::Plain {
            requested.data_key = None;
        }
    }
    // Existing hash-group homes reopened with warm_bytes=0 would otherwise
    // disable eviction permanently. Product default warm budget is a runtime
    // policy, not stored in the descriptor.
    apply_hash_group_product_warm_default(&mut requested);
    Ok(requested)
}

/// When layout is hash-group and the caller left warm budget at 0, install the
/// product 256 MiB cap so Mini reopen never disables eviction.
pub(super) fn apply_hash_group_product_warm_default(opts: &mut LastStoreOptions) {
    if opts.layout_mode == LayoutMode::HashGroup && opts.hash_group_warm_bytes == 0 {
        opts.hash_group_warm_bytes = LastStoreOptions::hash_group().hash_group_warm_bytes;
    }
}

pub(super) fn apply_legacy_data_key_packaging(opts: &mut LastStoreOptions) {
    // Backward compatible: older callers selected encrypted frame packaging by
    // providing only a data key. Normalize before durable layout comparison so
    // explicit reopen sees the same effective options that fresh open wrote.
    if opts.data_key.is_some() {
        opts.packaging = PackagingMode::FrameAead;
    }
}

pub(super) fn detect_existing_layout(root: &Path) -> Result<Option<LayoutMode>> {
    let data = root.join("data");
    if !data.exists() {
        return Ok(None);
    }
    let mut saw_segment_log = false;
    let mut saw_hash_group = false;
    for collection in fs::read_dir(data)? {
        let collection = collection?;
        if !collection.file_type()?.is_dir() {
            continue;
        }
        for shard in fs::read_dir(collection.path())? {
            let shard = shard?;
            if !shard.file_type()?.is_dir() {
                continue;
            }
            let shard_path = shard.path();
            if shard_path.join("g").is_dir() {
                saw_hash_group = true;
            }
            if shard_path.join("tail").is_dir() || shard_path.join("chunks").is_dir() {
                saw_segment_log = true;
            }
            for entry in fs::read_dir(&shard_path)? {
                let entry = entry?;
                if entry.file_type()?.is_file()
                    && entry.path().extension().and_then(|value| value.to_str()) == Some("seg")
                {
                    saw_segment_log = true;
                }
            }
        }
    }
    match (saw_segment_log, saw_hash_group) {
        (false, false) => Ok(None),
        (true, false) => Ok(Some(LayoutMode::SegmentLog)),
        (false, true) => Ok(Some(LayoutMode::HashGroup)),
        (true, true) => Err(Error::Corrupt(format!(
            "mixed segment-log and hash-group layouts under {}",
            root.display()
        ))),
    }
}

pub(super) fn read_layout_descriptor(root: &Path) -> Result<Option<LayoutDescriptor>> {
    let path = root.join(LAYOUT_FILE);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let fields: BTreeMap<_, _> = contents
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect();
    if fields.get("version") != Some(&"1") {
        return Err(Error::Corrupt(format!(
            "unsupported layout descriptor at {}",
            path.display()
        )));
    }
    let layout_mode = match fields.get("layout_mode") {
        Some(&"segment_log") => LayoutMode::SegmentLog,
        Some(&"hash_group") => LayoutMode::HashGroup,
        _ => return Err(Error::Corrupt("invalid layout_mode in descriptor".into())),
    };
    let hash_algo = match fields.get("hash_algo") {
        Some(&"fnv1a64") => HashAlgo::Fnv1a64,
        _ => return Err(Error::Corrupt("invalid hash_algo in descriptor".into())),
    };
    let parse = |key: &str| {
        fields
            .get(key)
            .ok_or_else(|| Error::Corrupt(format!("missing {key} in layout descriptor")))
    };
    let shard_bits = parse("shard_bits")?
        .parse()
        .map_err(|_| Error::Corrupt("invalid shard_bits in layout descriptor".into()))?;
    let hash_group_bits = parse("hash_group_bits")?
        .parse()
        .map_err(|_| Error::Corrupt("invalid hash_group_bits in layout descriptor".into()))?;
    let layout_epoch = parse("layout_epoch")?
        .parse()
        .map_err(|_| Error::Corrupt("invalid layout_epoch in layout descriptor".into()))?;
    // Optional fields: descriptors written before partition-prefix placement
    // omit them, and their homes are full-key placed with no fan-out.
    let hash_group_key = match fields.get("hash_group_key").copied() {
        None | Some("full_key") => HashGroupKey::FullKey,
        Some("partition_prefix") => HashGroupKey::PartitionPrefix,
        Some(_) => {
            return Err(Error::Corrupt(
                "invalid hash_group_key in layout descriptor".into(),
            ));
        }
    };
    let hash_group_partition_fanout = match fields.get("hash_group_partition_fanout").copied() {
        None => 1,
        Some(raw) => raw.parse().map_err(|_| {
            Error::Corrupt("invalid hash_group_partition_fanout in layout descriptor".into())
        })?,
    };
    // Optional field: older descriptors omit packaging; default by sampling.
    let packaging = match fields.get("packaging").copied() {
        Some("plain") => PackagingMode::Plain,
        Some("frame_aead") => PackagingMode::FrameAead,
        Some(_) => {
            return Err(Error::Corrupt(
                "invalid packaging in layout descriptor".into(),
            ));
        }
        None => detect_packaging_mode(root).unwrap_or(PackagingMode::Plain),
    };
    Ok(Some(LayoutDescriptor {
        layout_mode,
        shard_bits,
        hash_group_bits,
        hash_algo,
        hash_group_key,
        hash_group_partition_fanout,
        layout_epoch,
        packaging,
    }))
}

pub(super) fn write_layout_descriptor(root: &Path, opts: &LastStoreOptions) -> Result<()> {
    let path = root.join(LAYOUT_FILE);
    if path.exists() {
        return Ok(());
    }
    if detect_existing_layout(root)?.is_some() {
        return Ok(());
    }
    fs::create_dir_all(root)?;
    let tmp = root.join(format!(".{LAYOUT_FILE}.{}.tmp", Uuid::new_v4()));
    let mode = match opts.layout_mode {
        LayoutMode::SegmentLog => "segment_log",
        LayoutMode::HashGroup => "hash_group",
    };
    let algo = match opts.hash_algo {
        HashAlgo::Fnv1a64 => "fnv1a64",
    };
    let packaging = match opts.packaging {
        PackagingMode::Plain => "plain",
        PackagingMode::FrameAead => "frame_aead",
    };
    let group_key = match opts.hash_group_key {
        HashGroupKey::FullKey => "full_key",
        HashGroupKey::PartitionPrefix => "partition_prefix",
    };
    let mut file = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
    write!(
        file,
        "version=1\nlayout_mode={mode}\nshard_bits={}\nhash_group_bits={}\nhash_algo={algo}\nlayout_epoch={}\npackaging={packaging}\nhash_group_key={group_key}\nhash_group_partition_fanout={}\n",
        opts.shard_bits, opts.hash_group_bits, opts.layout_epoch, opts.hash_group_partition_fanout
    )?;
    durability::sync_dirty_file(&file)?;
    drop(file);
    fs::rename(&tmp, &path)?;
    sync_dir(root)
}

/// Read the durable layout descriptor of the home at `root`.
///
/// Returns `Ok(None)` for a home written before descriptors existed, or one
/// that has never been opened.
///
/// This reads the descriptor file only: it opens no segments, takes no locks,
/// and does not create the home. That makes it safe to call against a home a
/// running daemon holds — which is the point, since the operator question it
/// answers ("is this home costing me a full sweep per partition read?") is
/// most urgent while the node is up and slow.
pub fn describe_home(root: impl AsRef<Path>) -> Result<Option<LayoutDescriptor>> {
    read_layout_descriptor(root.as_ref())
}

/// True when any sealed/open segment under `root` begins with the frame magic.
pub fn home_has_frame_aead_segments(root: impl AsRef<Path>) -> bool {
    detect_packaging_mode(root.as_ref()) == Some(PackagingMode::FrameAead)
}

pub(super) fn detect_packaging_mode(root: &Path) -> Option<PackagingMode> {
    let data = root.join("data");
    if !data.is_dir() {
        return None;
    }
    let mut saw_seg = false;
    let mut stack = vec![data];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|s| s.to_str()) != Some("seg") {
                continue;
            }
            saw_seg = true;
            let mut magic = [0u8; 4];
            if let Ok(mut f) = File::open(&path) {
                if f.read_exact(&mut magic).is_ok() && magic == frame::MAGIC {
                    return Some(PackagingMode::FrameAead);
                }
            }
        }
    }
    if saw_seg {
        Some(PackagingMode::Plain)
    } else {
        None
    }
}
