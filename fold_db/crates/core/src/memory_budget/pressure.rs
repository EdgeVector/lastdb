//! Host memory pressure sampling and the pressure latch.

use super::*;

/// Pressure stop after `min(const, percent of RAM)`. Unknown RAM keeps the const.
#[must_use]
pub fn pressure_footprint_target_bytes(ram_bytes: u64) -> u64 {
    pressure_line_bytes(
        PRESSURE_FOOTPRINT_TARGET_BYTES,
        ram_bytes,
        PRESSURE_TARGET_RAM_PERCENT,
    )
}

/// Operating hard line after `min(const, percent of RAM)`. Unknown RAM keeps the const.
#[must_use]
pub fn pressure_footprint_hard_bytes(ram_bytes: u64) -> u64 {
    pressure_line_bytes(
        PRESSURE_FOOTPRINT_HARD_BYTES,
        ram_bytes,
        PRESSURE_HARD_RAM_PERCENT,
    )
}

pub(super) fn pressure_line_bytes(const_bytes: u64, ram_bytes: u64, percent: u64) -> u64 {
    if ram_bytes == 0 {
        return const_bytes;
    }
    const_bytes.min(ram_bytes.saturating_mul(percent) / 100)
}

/// One host-pressure sample. Counts come from the local host, not from a store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HostPressure {
    pub swap_used_bytes: u64,
    pub compressor_bytes: u64,
    pub free_inactive_bytes: u64,
    pub ram_bytes: u64,
}

impl HostPressure {
    /// Raw sample, before the 120 s clear hold.
    #[must_use]
    pub fn is_raw_high(self) -> bool {
        if self.swap_used_bytes > PRESSURE_SWAP_HIGH_BYTES
            || self.compressor_bytes > PRESSURE_COMPRESSOR_HIGH_BYTES
        {
            return true;
        }
        if self.ram_bytes == 0 {
            return false;
        }
        self.free_inactive_bytes.saturating_mul(100)
            < self
                .ram_bytes
                .saturating_mul(PRESSURE_FREE_INACTIVE_MIN_PERCENT)
    }
}

/// Pressure stays high until a raw-clear sample has held for
/// [`FOOTPRINT_LATCH_RECOVERY_HOLD_SECS`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PressureLatch {
    latched_high: bool,
    clear_since_epoch_secs: Option<u64>,
}

impl PressureLatch {
    #[must_use]
    pub fn observe(self, raw_high: bool, now_epoch_secs: u64) -> Self {
        if raw_high {
            return Self {
                latched_high: true,
                clear_since_epoch_secs: None,
            };
        }
        if !self.latched_high {
            return self;
        }
        match self.clear_since_epoch_secs {
            None => Self {
                latched_high: true,
                clear_since_epoch_secs: Some(now_epoch_secs),
            },
            Some(since)
                if now_epoch_secs.saturating_sub(since) >= FOOTPRINT_LATCH_RECOVERY_HOLD_SECS =>
            {
                Self {
                    latched_high: false,
                    clear_since_epoch_secs: None,
                }
            }
            Some(_) => self,
        }
    }

    #[must_use]
    pub fn is_high(self) -> bool {
        self.latched_high
    }
}

/// `measured - footprint_net` is the slack inside `phys_footprint`.
#[must_use]
pub fn purge_slack_ok(measured_footprint_bytes: u64, footprint_net_bytes: u64) -> bool {
    measured_footprint_bytes.saturating_sub(footprint_net_bytes) <= PURGE_SLACK_LIMIT_BYTES
}

/// Swap, compressor, and free/inactive pages. `None` when the host does not
/// report them. A failed read must not be treated as a clear sample.
#[cfg(target_os = "macos")]
#[must_use]
pub fn sample_host_pressure() -> Option<HostPressure> {
    let swap_used_bytes = read_swap_used_bytes()?;
    let vm = read_vm_page_counts()?;
    let page_size = read_page_size()?;
    let ram_bytes = read_ram_bytes()?;
    Some(HostPressure {
        swap_used_bytes,
        compressor_bytes: vm.compressor_pages.saturating_mul(page_size),
        free_inactive_bytes: vm
            .free_pages
            .saturating_add(vm.inactive_pages)
            .saturating_mul(page_size),
        ram_bytes,
    })
}

#[cfg(not(target_os = "macos"))]
#[must_use]
pub fn sample_host_pressure() -> Option<HostPressure> {
    None
}

#[cfg(target_os = "macos")]
pub(super) struct VmPageCounts {
    free_pages: u64,
    inactive_pages: u64,
    compressor_pages: u64,
}

#[cfg(target_os = "macos")]
pub(super) fn read_swap_used_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::xsw_usage>::uninit();
    let mut len = std::mem::size_of::<libc::xsw_usage>();
    let name = std::ffi::CString::new("vm.swapusage").ok()?;
    // SAFETY: sysctlbyname writes an `xsw_usage` into `usage` when `len` is
    // that struct's size. A non-zero return leaves the buffer unread.
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            usage.as_mut_ptr().cast::<libc::c_void>(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return None;
    }
    // SAFETY: rc == 0 means the kernel wrote the struct.
    let usage = unsafe { usage.assume_init() };
    Some(usage.xsu_used)
}

#[cfg(target_os = "macos")]
pub(super) fn read_ram_bytes() -> Option<u64> {
    let mut mem: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    let name = std::ffi::CString::new("hw.memsize").ok()?;
    // SAFETY: sysctlbyname writes a u64 when `len` is 8.
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            (&mut mem as *mut u64).cast::<libc::c_void>(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (rc == 0 && mem > 0).then_some(mem)
}

#[cfg(target_os = "macos")]
pub(super) fn read_page_size() -> Option<u64> {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page > 0).then_some(page as u64)
}

#[cfg(target_os = "macos")]
pub(super) fn read_vm_page_counts() -> Option<VmPageCounts> {
    // libc has no `mach_port_deallocate`. `mach_host_self` returns a send
    // right; leaving it leaks one port per governor tick.
    extern "C" {
        fn mach_port_deallocate(
            task: libc::mach_port_t,
            name: libc::mach_port_t,
        ) -> libc::kern_return_t;
    }

    // SAFETY: mach_host_self returns a host send right. host_statistics64
    // writes `vm_statistics64` when `count` starts at HOST_VM_INFO64_COUNT.
    // The port is deallocated on every path below.
    // libc deprecates these two calls in favor of the mach2 crate. This
    // tree does not depend on mach2; the calls are the documented ones.
    unsafe {
        let host = libc::mach_host_self();
        let mut count = libc::HOST_VM_INFO64_COUNT;
        let mut stats = std::mem::MaybeUninit::<libc::vm_statistics64>::uninit();
        let kr = libc::host_statistics64(
            host,
            libc::HOST_VM_INFO64,
            stats.as_mut_ptr().cast::<libc::integer_t>(),
            &mut count,
        );
        let _ = mach_port_deallocate(libc::mach_task_self(), host);
        if kr != libc::KERN_SUCCESS {
            return None;
        }
        let stats = stats.assume_init();
        // packed(8): copy the fields. A reference to one may be unaligned.
        Some(VmPageCounts {
            free_pages: std::ptr::addr_of!(stats.free_count).read_unaligned() as u64,
            inactive_pages: std::ptr::addr_of!(stats.inactive_count).read_unaligned() as u64,
            compressor_pages: std::ptr::addr_of!(stats.compressor_page_count).read_unaligned()
                as u64,
        })
    }
}

/// Physical footprint minus allocator-held-free bytes that fit inside it.
///
/// `phys_footprint` cannot tell live bytes from allocator retention. This
/// second signal shows the part that malloc statistics can attribute as live.
/// Measured on the live primary 2026-09-05:
/// `phys_footprint` 10.09 GiB just over the soft line while malloc held 22.57
/// GiB free against 3.09 GiB in use. Falls back to the raw footprint when the
/// platform has no malloc-zone accounting (only macOS reports one).
#[must_use]
pub fn footprint_net_bytes(measured_footprint_bytes: u64, malloc: Option<MallocZoneStats>) -> u64 {
    match malloc {
        Some(stats) => measured_footprint_bytes.saturating_sub(
            stats
                .bytes_held_free
                .min(measured_footprint_bytes.saturating_sub(stats.bytes_in_use)),
        ),
        None => measured_footprint_bytes,
    }
}
