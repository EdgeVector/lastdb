//! Process-local Linux page policy for the pinned mimalloc v3 allocator.

/// Option 43 is `mi_option_allow_thp` in libmimalloc-sys 0.1.49's v3 header.
const MI_OPTION_ALLOW_THP: libmimalloc_sys::mi_option_t = 43;
const ALLOW_THP_ENV: &str = "MIMALLOC_ALLOW_THP";

pub(super) fn configure() -> (Option<bool>, Option<i32>) {
    let mut errno = None;
    if std::env::var_os(ALLOW_THP_ENV).is_none() {
        // Sparse, temporary request allocations otherwise retain 2 MiB physical
        // pages, even with commit-on-demand and immediate allocator purges.
        // SAFETY: this option write runs before worker threads start.
        unsafe { libmimalloc_sys::mi_option_set(MI_OPTION_ALLOW_THP, 0) };
        // mimalloc initializes before main, so its one-time OS setup already
        // ran. Set the kernel policy too; changing the option alone is too late.
        // SAFETY: PR_SET_THP_DISABLE takes scalar arguments, affects only this
        // process, and inherits into workers. All variadic arguments use long.
        let result = unsafe {
            libc::prctl(
                libc::PR_SET_THP_DISABLE,
                1 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            )
        };
        if result == -1 {
            errno = std::io::Error::last_os_error().raw_os_error();
        }
    }
    // Respect an explicit MIMALLOC_ALLOW_THP override. Report the actual kernel
    // state, including a caller's inherited policy, rather than the desired one.
    // SAFETY: PR_GET_THP_DISABLE takes zero scalar arguments and writes no memory.
    let state = unsafe {
        libc::prctl(
            libc::PR_GET_THP_DISABLE,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
        )
    };
    if state == -1 {
        (
            None,
            errno.or_else(|| std::io::Error::last_os_error().raw_os_error()),
        )
    } else {
        (Some(state == 1), errno)
    }
}
