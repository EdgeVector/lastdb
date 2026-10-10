use super::*;

/// `0` reads the env, `1` forces on, `2` forces off. Tests must not cache the env.
pub(super) static PRESSURE_SHED_INTERACTIVE: AtomicU8 = AtomicU8::new(0);

thread_local! {
    pub(super) static WARM_ADMIT_CODE: Cell<u8> = const { Cell::new(0) };
}

/// Run `work` with an admission code, then restore the previous code.
///
/// `0` is unspecified (evictable, the raw laststore default). `1` is
/// interactive. `2` is background (`lastgit`).
pub fn with_admit_code<T>(code: u8, work: impl FnOnce() -> T) -> T {
    struct Restore(u8);
    impl Drop for Restore {
        fn drop(&mut self) {
            WARM_ADMIT_CODE.with(|cell| cell.set(self.0));
        }
    }
    let previous = WARM_ADMIT_CODE.with(|cell| cell.replace(code));
    let _restore = Restore(previous);
    work()
}

/// Classify one schema owner for the warm set.
///
/// `lastgit` is background. A null owner and every other owner stay
/// interactive, including the factory ids `fbrain`, `fkanban`, `loom`,
/// `routines`, and `fsituations`. This does not read `X-LastDB-Client`.
pub fn with_schema_warm_owner<T>(owner: Option<&str>, work: impl FnOnce() -> T) -> T {
    let code = if owner == Some("lastgit") { 2 } else { 1 };
    with_admit_code(code, work)
}

/// Override `LASTDB_PRESSURE_SHED_INTERACTIVE` for this process.
///
/// `None` reads the env again. Unset defaults on. Falsy values are
/// `0`, `false`, `no`, and `off`.
pub fn set_pressure_shed_interactive_override(enabled: Option<bool>) {
    let value = match enabled {
        None => 0,
        Some(true) => 1,
        Some(false) => 2,
    };
    PRESSURE_SHED_INTERACTIVE.store(value, Ordering::Relaxed);
}

pub(super) fn pressure_shed_interactive() -> bool {
    match PRESSURE_SHED_INTERACTIVE.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => match std::env::var("LASTDB_PRESSURE_SHED_INTERACTIVE") {
            Ok(value) => {
                let value = value.trim().to_ascii_lowercase();
                !matches!(value.as_str(), "0" | "false" | "no" | "off")
            }
            Err(_) => true,
        },
    }
}

pub(super) fn current_admit_class() -> AdmitClass {
    match WARM_ADMIT_CODE.with(Cell::get) {
        2 => AdmitClass::Background,
        1 => AdmitClass::Interactive,
        _ => AdmitClass::Unspecified,
    }
}
