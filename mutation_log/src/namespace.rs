use crate::Namespace;

/// Whether a namespace may travel in a shareable database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceKind {
    /// A standard schema namespace the shareable database may store.
    Official,
    /// An app-private namespace. It belongs in the personal database only.
    AppPrivate,
}

/// Interim namespaces a shareable database accepts.
///
/// This is the only list. A namespace that is not here is app-private.
/// Named databases reserve official names later. The old schema-prefix
/// reservation plan is superseded.
///
/// - `lastdb-files` — share files publishes `FileCatalog` and `FileChunk`
///   (`lastdb_node/src/bin/lastdb/share_cmds/files/mod.rs`, `publisher.rs`).
///   `scripts/operations/record_share_files.py` writes the same schemas.
/// - `social` — lastdb-std 1.0, shareable (`spec/index.json`,
///   `spec/README.md`, `spec/social/`).
/// - `social-graph` — lastdb-std 1.0 name for the decision's `graph`
///   (`spec/index.json`, `spec/social-graph/`). `spec/README.md` marks it
///   personal-only; that prefix split is superseded by this list.
///
/// Share snapshots, contributor rules, and the shared-writer CLI carry the
/// caller's schema. Cloud share proofs publish under a generated
/// `cloud-proof-<database>` namespace. `CloudProofLocalOnly` stays on the
/// device. A generated name is not a stable entry.
pub const SHAREABLE_NAMESPACES: &[&str] = &["lastdb-files", "social", "social-graph"];

/// Classify with [`SHAREABLE_NAMESPACES`].
///
/// A listed namespace is [`NamespaceKind::Official`]. Every other namespace
/// is [`NamespaceKind::AppPrivate`].
#[must_use]
pub fn classify_shareable(namespace: &Namespace) -> NamespaceKind {
    if SHAREABLE_NAMESPACES.contains(&namespace.as_str()) {
        NamespaceKind::Official
    } else {
        NamespaceKind::AppPrivate
    }
}

/// Classifies namespaces.
///
/// [`classify_shareable`] is the interim production list. A production
/// caller passes that function. A namespace on the list is
/// [`NamespaceKind::Official`]. Every other namespace is
/// [`NamespaceKind::AppPrivate`].
pub trait NamespacePolicy {
    /// Classify one namespace.
    fn classify(&self, namespace: &Namespace) -> NamespaceKind;
}

impl<F> NamespacePolicy for F
where
    F: Fn(&Namespace) -> NamespaceKind,
{
    fn classify(&self, namespace: &Namespace) -> NamespaceKind {
        self(namespace)
    }
}
