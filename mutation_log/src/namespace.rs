use crate::Namespace;

/// Whether a namespace may travel in a shareable database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceKind {
    /// A standard schema namespace the shareable database may store.
    Official,
    /// An app-private namespace. It belongs in the personal database only.
    AppPrivate,
}

/// Classifies namespaces. The shell supplies the real list.
///
/// The held official-namespace module is that list. This crate does not
/// embed one. A production policy returns [`NamespaceKind::Official`] only
/// for a namespace on that list, and [`NamespaceKind::AppPrivate`] for
/// every other namespace.
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
