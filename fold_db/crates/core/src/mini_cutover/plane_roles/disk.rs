use super::*;

/// Walk `store_root/data` (LastStore collections root) and build a plane breakdown.
///
/// `store_root` is the LastStore home root (Mini: `$HOME/data`), whose child
/// `data/` holds collection directories. Missing dirs return an empty breakdown.
pub fn plane_breakdown_for_store_root(store_root: &std::path::Path) -> PlaneBreakdown {
    let collections_root = store_root.join("data");
    let Ok(rd) = std::fs::read_dir(&collections_root) else {
        return PlaneBreakdown::default();
    };
    let mut sizes = Vec::new();
    let mut total_apparent = 0u64;
    for entry in rd.filter_map(Result::ok) {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let DirSize {
            allocated,
            apparent,
        } = dir_size_bytes(&path).unwrap_or_default();
        total_apparent = total_apparent.saturating_add(apparent);
        sizes.push((name, allocated));
    }
    PlaneBreakdown::from_collection_sizes_with_apparent(sizes, total_apparent)
}

/// Bytes the filesystem has actually handed to a file, versus the length of the
/// records inside it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirSize {
    pub allocated: u64,
    pub apparent: u64,
}

/// Bytes the filesystem has actually allocated for `meta`.
///
/// [`std::fs::Metadata::len`] is the file's *apparent* length. It is the wrong
/// number for a capacity gauge: the store holds its hot segment logs open for
/// append, and the filesystem grants those files extents past their written end
/// which it does not release while the descriptor stays open. That space is
/// charged to the volume, so `du` counts it and `len()` cannot see it.
///
/// Measured on the primary 2026-08-03: 7.94 GiB apparent against 9.63 GiB
/// allocated — a 1.69 GiB (17.5%) gap, concentrated in `tips` (+861 MiB) and
/// `atoms` (+683 MiB) at up to 1 MiB per open segment. The write-once `.idx`
/// sidecars beside them, and the `indexes` segments the daemon does not hold
/// open, sit under 1%. Brain:
/// `lastdb-status-underreports-disk-footprint-by-apparent-size-20260803`.
#[must_use]
pub fn allocated_bytes(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        // st_blocks is defined in 512-byte units regardless of the fs block size.
        meta.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        meta.len()
    }
}

pub(crate) fn dir_size_bytes(path: &std::path::Path) -> std::io::Result<DirSize> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Ok(DirSize::default());
    }
    if meta.is_file() {
        return Ok(DirSize {
            allocated: allocated_bytes(&meta),
            apparent: meta.len(),
        });
    }
    let mut total = DirSize::default();
    if meta.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let child = dir_size_bytes(&entry.path()).unwrap_or_default();
            total.allocated = total.allocated.saturating_add(child.allocated);
            total.apparent = total.apparent.saturating_add(child.apparent);
        }
    }
    Ok(total)
}

/// Human-readable status lines for plane breakdown (offline-safe).
#[must_use]
pub fn plane_status_lines(breakdown: &PlaneBreakdown) -> Vec<String> {
    if breakdown.entries.is_empty() {
        return Vec::new();
    }
    let mut lines = vec![format!(
        "Planes: {} collections, {}",
        breakdown.entries.len(),
        format_bytes(breakdown.total_bytes)
    )];
    // Report the allocated total, but never bury the gap: a store whose files
    // hold materially more disk than their records are worth is a capacity fact
    // the operator has to be able to see.
    if let Some(overhang) = allocation_overhang(breakdown) {
        lines.push(format!(
            "  on-disk: {} allocated vs {} in records \
             (+{}, {:.1}% held past record length)",
            format_bytes(breakdown.total_bytes),
            format_bytes(breakdown.total_apparent_bytes),
            format_bytes(overhang.bytes),
            overhang.percent,
        ));
    }
    for (role, bytes) in &breakdown.by_role_bytes {
        if *bytes == 0 {
            continue;
        }
        let names: Vec<&str> = breakdown
            .entries
            .iter()
            .filter(|e| e.role == *role && e.bytes > 0)
            .map(|e| e.name.as_str())
            .collect();
        let sample = if names.len() <= 4 {
            names.join(", ")
        } else {
            format!(
                "{}, … (+{})",
                names[..3].join(", "),
                names.len().saturating_sub(3)
            )
        };
        lines.push(format!(
            "  {}: {}  [{}]",
            role.label(),
            format_bytes(*bytes),
            sample
        ));
    }
    // Explicit aside reclaim note when aside mass is present.
    if breakdown
        .by_role_bytes
        .iter()
        .any(|(r, b)| *r == CollectionPlaneRole::Aside && *b > 0)
    {
        lines.push(
            "  aside reclaim: Tom-gated after backup receipt — never auto-delete \
             (see fold_db/docs/aside-reclaim-procedure.md)"
                .to_string(),
        );
    }
    lines
}

/// Allocated space held past the end of the store's records.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AllocationOverhang {
    pub bytes: u64,
    pub percent: f64,
}

/// The allocated-vs-apparent gap, when it is large enough to be worth reporting.
///
/// Every filesystem rounds to block boundaries, so a small gap is noise. This
/// reports only a gap that is both ≥1% of the store and ≥1 MiB, which is the
/// band where the cause is retained extents rather than rounding.
#[must_use]
pub fn allocation_overhang(breakdown: &PlaneBreakdown) -> Option<AllocationOverhang> {
    const MIN_BYTES: u64 = 1024 * 1024;
    const MIN_PERCENT: f64 = 1.0;

    let bytes = breakdown
        .total_bytes
        .checked_sub(breakdown.total_apparent_bytes)?;
    if bytes < MIN_BYTES || breakdown.total_bytes == 0 {
        return None;
    }
    let percent = 100.0 * (bytes as f64) / (breakdown.total_bytes as f64);
    if percent < MIN_PERCENT {
        return None;
    }
    Some(AllocationOverhang { bytes, percent })
}

fn format_bytes(n: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let f = n as f64;
    if f >= GIB {
        format!("{:.2} GiB", f / GIB)
    } else if f >= MIB {
        format!("{:.1} MiB", f / MIB)
    } else if f >= KIB {
        format!("{:.1} KiB", f / KIB)
    } else {
        format!("{n} B")
    }
}
