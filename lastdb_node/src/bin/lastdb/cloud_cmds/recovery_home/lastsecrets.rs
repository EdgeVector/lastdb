//! Native LastSecrets transfer. No shell, ambient route, or plaintext output.

use std::io::Seek;
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};

#[path = "lastsecrets/files.rs"]
mod files;
#[path = "lastsecrets/process.rs"]
mod process;

use files::{account_home, require_no_runtime_config, require_private_config, ProtectedFile};

#[derive(Clone, Default, serde::Serialize)]
pub(super) struct Receipt {
    locator: Option<String>,
    executable_sha256: Option<String>,
    default_config_sha256: Option<String>,
    pub(super) api_key_stored: bool,
    pub(super) exact_key_readback_matches: bool,
    pub(super) write_may_have_committed: bool,
    exclusive_create: bool,
    local_default_route_preserved: bool,
    child_environment_cleared: bool,
}

pub(super) struct Failure {
    pub(super) code: &'static str,
    pub(super) receipt: Receipt,
}

pub(super) struct LastSecrets {
    slug: String,
    home: PathBuf,
    executable: ProtectedFile,
    config: ProtectedFile,
    socket: PathBuf,
    receipt: Receipt,
}

impl LastSecrets {
    pub(super) fn preflight(slug: &str, expected_user_hash: &str) -> Result<Self, &'static str> {
        validate_slug(slug, expected_user_hash)?;
        let home = account_home()?;
        require_no_runtime_config(&home)?;
        let executable = ProtectedFile::executable(&home.join(".local/bin/lastsecrets"), &home)?;
        let config = ProtectedFile::config(&home.join(".lastsecrets/config.json"))?;
        let socket = home.join(".lastdb/data/folddb.sock");
        require_private_config(&config.bytes, &socket)?;
        files::require_owner_socket(&socket)?;
        let receipt = Receipt {
            locator: Some(format!("lastsecrets://{slug}")),
            executable_sha256: Some(executable.sha256.clone()),
            default_config_sha256: Some(config.sha256.clone()),
            local_default_route_preserved: true,
            child_environment_cleared: true,
            ..Receipt::default()
        };
        Ok(Self {
            slug: slug.to_owned(),
            home,
            executable,
            config,
            socket,
            receipt,
        })
    }

    pub(super) fn receipt(&self) -> Receipt {
        self.receipt.clone()
    }

    pub(super) fn store(mut self, key: &str) -> Result<Receipt, Failure> {
        let result = self.store_inner(key);
        result
            .map(|()| self.receipt.clone())
            .map_err(|code| Failure {
                code,
                receipt: self.receipt.clone(),
            })
    }

    fn store_inner(&mut self, key: &str) -> Result<(), &'static str> {
        if key.is_empty()
            || key.len() > 4096
            || key.bytes().any(|byte| matches!(byte, 0 | b'\n' | b'\r'))
        {
            return Err("unsupported_registered_api_key");
        }
        self.verify()?;
        // Conservative before spawn/write: a failed result must never authorize
        // another registration or claim that the secret record is absent.
        self.receipt.write_may_have_committed = true;
        process::run(self, Some(key.as_bytes()))?;
        self.receipt.api_key_stored = true;
        self.verify()?;
        let stored = process::run(self, None)?;
        if stored != format!("{key}\n").as_bytes() {
            return Err("lastsecrets_stored_key_mismatch");
        }
        self.receipt.exact_key_readback_matches = true;
        self.verify()
    }

    fn verify(&self) -> Result<(), &'static str> {
        require_no_runtime_config(&self.home)?;
        self.executable.verify()?;
        self.config.verify()?;
        files::require_owner_socket(&self.socket)
    }

    fn command(&self, put: bool) -> Result<std::process::Command, &'static str> {
        // The child receives the checked read-only descriptor, not a second
        // path open that could follow a replaced default configuration.
        self.config
            .descriptor
            .try_clone()
            .map_err(|_| "cannot_retain_lastsecrets_config")?
            .rewind()
            .map_err(|_| "cannot_rewind_lastsecrets_config")?;
        let descriptor = self.config.descriptor.as_raw_fd();
        let config_route = format!("/dev/fd/{descriptor}");
        let mut command = std::process::Command::new(&self.executable.path);
        command
            .arg(if put { "put" } else { "get" })
            .arg(&self.slug)
            .arg("--config")
            .arg(&config_route)
            .env_clear()
            .env("HOME", &self.home)
            .env("PATH", "/usr/bin:/bin")
            .env("LASTSECRETS_CONFIG", &config_route)
            .env("LASTSECRETS_NODE_URL", "http://127.0.0.1")
            .env("LASTSECRETS_SOCKET_PATH", &self.socket)
            .env("LASTDB_SOCKET_PATH", &self.socket)
            .env("NODE_ENV", "production")
            .env("OBS_SENTRY_DSN", "")
            .env("DO_NOT_TRACK", "1")
            .env("BUN_OPTIONS", "")
            .env("BUN_RUNTIME_TRANSPILER_CACHE_PATH", "0")
            .current_dir(Path::new("/"));
        unsafe {
            command.pre_exec(move || {
                let flags = libc::fcntl(descriptor, libc::F_GETFD);
                if flags < 0
                    || libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        if put {
            command.args([
                "--label",
                "LastDB disposable DEV recovery",
                "--provider",
                "exemem",
                "--purpose",
                "Disposable DEV cloud cleanup",
                "--env",
                "dev",
                "--value-stdin",
            ]);
        }
        Ok(command)
    }
}

fn validate_slug(slug: &str, expected_user_hash: &str) -> Result<(), &'static str> {
    let prefix = format!("lastdb-dev-recovery-api-key-{expected_user_hash}-");
    let suffix = slug
        .strip_prefix(&prefix)
        .ok_or("invalid_dev_lastsecrets_slug")?;
    let uuid = uuid::Uuid::parse_str(suffix).map_err(|_| "invalid_dev_lastsecrets_slug")?;
    if uuid.get_version() != Some(uuid::Version::Random) || uuid.to_string() != suffix {
        return Err("invalid_dev_lastsecrets_slug");
    }
    Ok(())
}
