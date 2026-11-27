//! Local panic marker and Sentry panic reporting.

use std::io::Write;

/// Write the local panic marker to stderr. Always safe; never includes payload.
pub fn write_panic_marker() {
    let _ = writeln!(std::io::stderr().lock(), "LastDB panic");
}

/// Report the panic to Sentry when a client is live, then print the local marker.
#[allow(deprecated)] // PanicHookInfo arrives on newer rustc; PanicInfo stays portable.
pub fn on_panic(info: &std::panic::PanicInfo<'_>) {
    #[cfg(feature = "sentry")]
    crate::fault::capture_panic(info);
    write_panic_marker();
}

/// Install the process panic hook. Replaces any prior hook.
pub fn install_private_panic_hook() {
    std::panic::set_hook(Box::new(on_panic));
}
