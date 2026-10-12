//! Process CPU sampling shared by the sync upload policy and the node's self
//! metrics: one `getrusage` read and one percent-since-last-sample probe.

use std::time::Instant;

/// User plus system CPU seconds this process has used so far.
pub fn process_cpu_secs() -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage struct on success.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: rc == 0 means getrusage initialized `usage`.
    let usage = unsafe { usage.assume_init() };
    Some(timeval_secs(usage.ru_utime) + timeval_secs(usage.ru_stime))
}

fn timeval_secs(tv: libc::timeval) -> f64 {
    tv.tv_sec as f64 + (tv.tv_usec as f64 / 1_000_000.0)
}

/// Remembers the previous sample so each call reports CPU percent over the
/// interval since the last one (0..=N×100 on N cores).
#[derive(Debug, Default)]
pub struct CpuProbe {
    last_wall: Option<Instant>,
    last_cpu_secs: Option<f64>,
}

impl CpuProbe {
    /// CPU percent since the previous call, rounded to hundredths so status
    /// JSON stays stable. `None` on the first call or when no time elapsed.
    pub fn sample_percent(&mut self) -> Option<f64> {
        let wall = Instant::now();
        let cpu = process_cpu_secs()?;
        let out = match (self.last_wall, self.last_cpu_secs) {
            (Some(prev_wall), Some(prev_cpu)) => {
                let wall_delta = wall.duration_since(prev_wall).as_secs_f64();
                let cpu_delta = cpu - prev_cpu;
                if wall_delta > 0.0 && cpu_delta >= 0.0 {
                    Some(((cpu_delta / wall_delta) * 100.0 * 100.0).round() / 100.0)
                } else {
                    None
                }
            }
            _ => None,
        };
        self.last_wall = Some(wall);
        self.last_cpu_secs = Some(cpu);
        out
    }
}
