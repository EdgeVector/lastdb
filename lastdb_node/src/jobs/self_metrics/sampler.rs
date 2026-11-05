use super::*;
use tracing::Instrument;

pub fn spawn_sampler(
    host: std::sync::Arc<Host>,
    interval: Duration,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            let mut cpu = CpuProbe::default();
            while !shutdown.load(std::sync::atomic::Ordering::Acquire) {
                match super::drain::record_sample(&host, &mut cpu) {
                    Ok(sampled_at) => {
                        host.self_metrics.record_success(sampled_at);
                    }
                    Err(e) => {
                        tracing::warn!(target: "lastdbd::self_metrics", error = %e, "self-metrics sample failed");
                        host.self_metrics.record_error(e);
                    }
                }
                if shutdown.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                crate::ttl_sweep::maybe_spawn_ttl_sweep(&host, &shutdown);
                let until = tokio::time::Instant::now() + interval;
                while !shutdown.load(std::sync::atomic::Ordering::Acquire)
                    && tokio::time::Instant::now() < until
                {
                    tokio::time::sleep(
                        std::time::Duration::from_millis(250).min(
                            until.saturating_duration_since(tokio::time::Instant::now()),
                        ),
                    )
                    .await;
                }
            }
        }
        .in_current_span(),
    )
}
