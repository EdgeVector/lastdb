use crate::Namespace;
use database_policy::{admit_namespace, NamespaceClass, SpecBinding};

/// Whether a namespace may travel in a shareable database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceKind {
    /// A standard schema namespace the shareable database may store.
    Official,
    /// An app-private namespace. It belongs in the personal database only.
    AppPrivate,
}

/// Union of provisioned share namespaces.
///
/// This is not per-database admission. A named database passes
/// [`BoundPolicy`] built from its registry spec binding. `social-graph`
/// stays personal-only. `lastdb-files` is the explicit file publication.
/// Contributor rules carry the caller's schema. Cloud share proofs publish
/// under a generated `cloud-proof-<database>` namespace.
/// `CloudProofLocalOnly` stays on the device. A generated name is not a
/// stable entry.
pub use database_policy::SHAREABLE_NAMESPACES;

/// Namespace admission for one database spec binding.
///
/// [`BoundPolicy::absent`] classifies every namespace as app-private, so a
/// shareable log rejects the append until a binding is present.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundPolicy {
    binding: Option<SpecBinding>,
}

impl BoundPolicy {
    /// No spec binding. Share appends fail closed.
    pub fn absent() -> Self {
        Self { binding: None }
    }

    /// Admit namespaces through this database binding.
    pub fn new(binding: SpecBinding) -> Self {
        Self {
            binding: Some(binding),
        }
    }

    /// Build the policy from the stored `{ "name", "version" }` object.
    ///
    /// Built-in versions use the catalog. An app-defined spec names the
    /// namespace of the database's content.
    pub fn admitted(name: &str, version: u32) -> Result<Self, database_policy::PolicyError> {
        let binding = SpecBinding::new(name, version)?;
        binding.app_namespace()?;
        Ok(Self::new(binding))
    }

    /// Borrow the binding, when one was supplied.
    pub fn binding(&self) -> Option<&SpecBinding> {
        self.binding.as_ref()
    }
}

impl NamespacePolicy for BoundPolicy {
    fn classify(&self, namespace: &Namespace) -> NamespaceKind {
        if let Some(app) = self
            .binding
            .as_ref()
            .and_then(|binding| binding.app_namespace().ok().flatten())
        {
            return if app == namespace.as_str() {
                NamespaceKind::Official
            } else {
                NamespaceKind::AppPrivate
            };
        }
        if admit_namespace(self.binding.as_ref(), namespace.as_str())
            == Ok(NamespaceClass::Official)
        {
            NamespaceKind::Official
        } else {
            NamespaceKind::AppPrivate
        }
    }
}

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
