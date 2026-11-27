//! `upgrade/config.toml`: channel, auto policy, window, service label and limits
//! (contract section 8).

use std::fs;

use super::state::{atomic_write, Paths};

#[derive(Debug, Clone)]
pub(super) struct Config {
    pub(super) channel: String,
    pub(super) auto: String,
    pub(super) window: String,
    pub(super) service_label: Option<String>,
    pub(super) soak_secs: u64,
    pub(super) stop_timeout_secs: u64,
    pub(super) rss_limit_mb: u64,
}

impl Config {
    pub(super) fn defaults() -> Self {
        Self {
            channel: "stable".into(),
            auto: "apply".into(),
            window: "02:00-05:00".into(),
            service_label: None,
            soak_secs: 300,
            stop_timeout_secs: 600,
            rss_limit_mb: 16384,
        }
    }

    /// The config and whether the file exists. A missing file gives the defaults.
    pub(super) fn load(paths: &Paths) -> Result<(Self, bool), String> {
        let path = paths.dir().join("config.toml");
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok((Self::defaults(), false));
            }
            Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
        };
        let mut cfg = Self::defaults();
        for (n, raw) in text.lines().enumerate() {
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("{}:{}: expected key = value", path.display(), n + 1))?;
            let (key, value) = (key.trim(), value.trim());
            let text_value = || value.trim_matches('"').to_owned();
            let number = || {
                value
                    .parse::<u64>()
                    .map_err(|_| format!("{}:{}: {key} must be a number", path.display(), n + 1))
            };
            match key {
                "channel" => cfg.channel = text_value(),
                "auto" => cfg.auto = text_value(),
                "window" => cfg.window = text_value(),
                "service_label" => cfg.service_label = Some(text_value()),
                "soak_secs" => cfg.soak_secs = number()?,
                "stop_timeout_secs" => cfg.stop_timeout_secs = number()?,
                "rss_limit_mb" => cfg.rss_limit_mb = number()?,
                other => {
                    return Err(format!(
                        "{}:{}: unknown setting {other}",
                        path.display(),
                        n + 1
                    ));
                }
            }
        }
        cfg.validate()?;
        Ok((cfg, true))
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        if !matches!(self.channel.as_str(), "stable" | "nightly") {
            return Err("channel must be stable or nightly".into());
        }
        if !matches!(self.auto.as_str(), "off" | "check" | "apply") {
            return Err("auto must be off, check or apply".into());
        }
        parse_window(&self.window)?;
        if let Some(label) = &self.service_label {
            if !valid_label(label) {
                return Err("service_label has characters a job label cannot hold".into());
            }
        }
        if !(1..=86_400).contains(&self.soak_secs)
            || !(1..=86_400).contains(&self.stop_timeout_secs)
        {
            return Err("soak_secs and stop_timeout_secs must be 1 to 86400".into());
        }
        if self.rss_limit_mb == 0 {
            return Err("rss_limit_mb must be above 0".into());
        }
        Ok(())
    }

    pub(super) fn save(&self, paths: &Paths) -> Result<(), String> {
        self.validate()?;
        let mut text = format!(
            "channel = \"{}\"            # stable | nightly\nauto = \"{}\"              # off | check | apply\nwindow = \"{}\"        # local time\n",
            self.channel, self.auto, self.window
        );
        if let Some(label) = &self.service_label {
            text.push_str(&format!("service_label = \"{label}\"\n"));
        }
        text.push_str(&format!(
            "soak_secs = {}\nstop_timeout_secs = {}\nrss_limit_mb = {}\n",
            self.soak_secs, self.stop_timeout_secs, self.rss_limit_mb
        ));
        atomic_write(&paths.dir().join("config.toml"), text.as_bytes(), 0o600)
    }

    /// The label of the service, or the refusal that explains none is set.
    pub(super) fn label(&self) -> Result<&str, String> {
        self.service_label.as_deref().ok_or_else(|| {
            "no service is configured; run 'lastdb service install' or 'lastdb service adopt --label <label>' first"
                .to_string()
        })
    }
}

/// A job label or unit name: letters, digits, dot, dash and underscore.
pub(super) fn valid_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 128
        && !label.starts_with('.')
        && label
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
}

/// `HH:MM-HH:MM` as minutes since midnight.
pub(super) fn parse_window(window: &str) -> Result<(u32, u32), String> {
    let bad = || format!("window '{window}' must look like 02:00-05:00");
    let (a, b) = window.split_once('-').ok_or_else(bad)?;
    let minutes = |t: &str| -> Option<u32> {
        let (h, m) = t.split_once(':')?;
        let (h, m) = (h.parse::<u32>().ok()?, m.parse::<u32>().ok()?);
        (h < 24 && m < 60).then_some(h * 60 + m)
    };
    match (minutes(a), minutes(b)) {
        (Some(start), Some(end)) if start != end => Ok((start, end)),
        _ => Err(bad()),
    }
}

/// True when local time is inside the window. A window may cross midnight.
pub(super) fn in_window(window: &str) -> Result<bool, String> {
    use chrono::Timelike as _;
    let (start, end) = parse_window(window)?;
    let now = chrono::Local::now();
    let minute = now.hour() * 60 + now.minute();
    Ok(if start < end {
        (start..end).contains(&minute)
    } else {
        minute >= start || minute < end
    })
}
