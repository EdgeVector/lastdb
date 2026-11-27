//! Which release an install takes: contract section 7.

use sha2::{Digest as _, Sha256};

use super::list::{parse_time, NodeList, Release};

/// What runs now: the program folder name that `current` names, and the list entry
/// kept in `programs/<version>/release.json` (absent for a `--pair` install).
pub(super) struct Running {
    pub(super) version: String,
    pub(super) release: Option<Release>,
}

pub(super) enum Mode {
    /// The hourly check: share, `newer` and `rejected` all apply.
    Auto,
    /// `lastdb upgrade` by hand: the share does not apply.
    Manual,
    /// `--version` or `--channel`: one named entry, or the channel's newest.
    Named(Option<String>),
}

/// `u32_be(sha256(install_id_hex + ":" + version)[0..4]) % 100`.
pub(super) fn share_slot(install_id: &str, version: &str) -> u64 {
    let digest = Sha256::digest(format!("{install_id}:{version}").as_bytes());
    u64::from(u32::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3],
    ])) % 100
}

pub(super) fn in_share(install_id: &str, release: &Release) -> bool {
    share_slot(install_id, &release.version) < release.rollout.percent
}

/// An entry is newer than the running pair when its `published_at` is later than the
/// running pair's. Version strings are never compared.
pub(super) fn is_newer(release: &Release, running: &Running) -> bool {
    let Some(have) = running
        .release
        .as_ref()
        .and_then(|r| parse_time(&r.published_at))
    else {
        return false;
    };
    parse_time(&release.published_at).is_some_and(|at| at > have)
}

/// The chosen entry, `Ok(None)` when nothing applies, or the refusal for a named
/// version that cannot be taken.
pub(super) fn pick<'a>(
    list: &'a NodeList,
    mode: &Mode,
    running: &Running,
    install_id: &str,
    rejected: &[String],
) -> Result<Option<&'a Release>, String> {
    match mode {
        Mode::Named(Some(version)) => {
            let release = list
                .releases
                .iter()
                .find(|r| &r.version == version)
                .ok_or_else(|| format!("version {version} is not in the {} list", list.channel))?;
            if release.active() {
                Ok(Some(release))
            } else {
                Err(format!(
                    "version {version} is withdrawn and cannot be installed"
                ))
            }
        }
        Mode::Named(None) => Ok(list.releases.iter().find(|r| r.active())),
        Mode::Manual => Ok(list.releases.iter().find(|r| {
            r.active()
                && !rejected.contains(&r.version)
                && (running.release.is_none() || is_newer(r, running))
        })),
        Mode::Auto => {
            if running.release.is_none() {
                return Ok(None);
            }
            Ok(list.releases.iter().find(|r| {
                r.active()
                    && is_newer(r, running)
                    && in_share(install_id, r)
                    && !rejected.contains(&r.version)
            }))
        }
    }
}

/// The list entry for the running version when the verified list names it withdrawn.
pub(super) fn withdrawn_running<'a>(list: &'a NodeList, running: &Running) -> Option<&'a Release> {
    list.releases
        .iter()
        .find(|r| r.version == running.version && !r.active())
}
