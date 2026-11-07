//! Groups of parsed `lastdb app` CLI fields that travel together, so the
//! handlers take one value instead of a long positional list.

/// Where `install` / `upgrade` resolve an app from on the signed public index.
#[derive(Clone)]
pub(crate) struct RegistrySource {
    pub(crate) channel: Option<String>,
    pub(crate) index: Option<String>,
    pub(crate) trust_key: Option<String>,
}

/// `lastdb app list APP`: the index query for one app on this node.
pub(crate) struct AppResolveQuery<'a> {
    pub(crate) app_id: &'a str,
    pub(crate) channel: Option<&'a str>,
    pub(crate) index: Option<&'a str>,
    pub(crate) trust_key: Option<&'a str>,
    pub(crate) json: bool,
}

/// `lastdb app install` after the CLI parse.
pub(crate) struct AppInstall {
    pub(crate) accept_install: bool,
    pub(crate) inactive: bool,
    pub(crate) app_ids: Vec<String>,
    pub(crate) source: RegistrySource,
}

/// `lastdb app upgrade` after the CLI parse.
pub(crate) struct AppUpgrade {
    pub(crate) all: bool,
    pub(crate) accept_install: bool,
    pub(crate) app_ids: Vec<String>,
    pub(crate) source: RegistrySource,
}
