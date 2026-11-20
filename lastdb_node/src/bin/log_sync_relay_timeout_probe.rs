//! Prove `log_sync` RelayClient bounds: a hung peer must fail within the request timeout.
//!
//! Usage: `RELAY_URL=http://127.0.0.1:PORT log_sync_relay_timeout_probe`
//! Exit 0 when the append errors inside 45s; exit 1 otherwise.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let base = match std::env::var("RELAY_URL") {
        Ok(url) if !url.is_empty() => url,
        _ => {
            eprintln!("log_sync_relay_timeout_probe: RELAY_URL is not set");
            return ExitCode::from(1);
        }
    };
    match lastdb_node::log_sync::hung_relay_must_timeout(&base).await {
        Ok(elapsed) => {
            println!(
                "log_sync_relay_timeout_probe: hung peer failed in {:.3}s",
                elapsed.as_secs_f64()
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("log_sync_relay_timeout_probe: {error}");
            ExitCode::from(1)
        }
    }
}
