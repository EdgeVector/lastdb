//! Spec catalog and share admission for named LastDB databases.
//!
//! The crate reads no clock and opens no file. [`admit_share`] and
//! [`admit_namespace`] are the narrow policy. A missing binding is
//! [`PolicyError::BindingAbsent`]. Callers deny that result.
//!
//! The registry, the publication marker, and the daemon share one binding.
//! [`SpecBinding::admitted`] is that binding. [`record_is_shareable`] is the
//! LastDB check for a schema, its fields, and a file. A service that stores
//! the log does not call it. The payload stays opaque there.
//!
//! `spec/index.json` records the same catalog. Social schemas live in
//! `spec/social/`. `social-graph` stays in `personal` and is not the
//! reserved `graph` database.

mod catalog;

/// Union of provisioned share namespaces, for callers that do not yet
/// carry a per-database binding. Named databases use [`admit_namespace`].
pub const SHAREABLE_NAMESPACES: &[&str] = &["lastdb-files", "social"];

const PERSONAL: &str = "personal";

/// One published or personal-only schema and the fields it declares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaSpec {
    /// Namespace that owns the schema.
    pub namespace: &'static str,
    /// Schema name inside that namespace.
    pub name: &'static str,
    /// Fields the schema declares. Unlisted fields stay in `personal`.
    pub fields: &'static [&'static str],
}

/// Versioned spec binding stored beside a database id, name, and kind.
///
/// This is registry contract A's `spec` object: `{ "name", "version" }`
/// with an integer version of at least 1. Catalog label `1.0` is version `1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpecBinding {
    name: String,
    version: u32,
}

impl SpecBinding {
    /// Accept a syntactically valid spec name and version. Catalog membership
    /// is checked by the admit functions.
    pub fn new(name: &str, version: u32) -> Result<Self, PolicyError> {
        if !valid_label(name) {
            return Err(PolicyError::InvalidName);
        }
        if version >= 1 {
            Ok(Self {
                name: name.to_owned(),
                version,
            })
        } else {
            Err(PolicyError::UnsupportedVersion)
        }
    }

    /// The provisioned social spec. Registry version `1` is catalog `1.0`.
    pub fn social_v1() -> Self {
        Self {
            name: "social".to_owned(),
            version: 1,
        }
    }

    /// Explicit `lastdb-files` version 1. A missing spec is not this binding.
    pub fn legacy_files_publication() -> Self {
        Self {
            name: "lastdb-files".to_owned(),
            version: 1,
        }
    }

    /// Borrow the spec name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Registry spec version. `1` is the current catalog.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The binding the registry, the publication marker, and the daemon share.
    ///
    /// `name` and `version` are the stored `{ "name", "version" }` object.
    /// A missing spec is not this call. An unknown catalog entry is an error.
    pub fn admitted(name: &str, version: u32) -> Result<Self, PolicyError> {
        let binding = Self::new(name, version)?;
        admit_share(Some(&binding))?;
        Ok(binding)
    }
}

/// Whether the database is the builtin personal database or a shareable one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseClass {
    /// The non-shareable personal database.
    Personal,
    /// A database whose binding publishes one catalog spec.
    Shareable,
}

/// What a reserved database name means in the catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reservation {
    /// The name is reserved and create refuses it. `graph` is this case.
    Unprovisioned,
    /// The name can be stored only with `spec.name` equal to the database name.
    /// Only `social` has a schema body. The others are [`PolicyError::UnknownSpec`].
    RequiresOwnSpec,
}

/// A database the catalog accepts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// Builtin `personal`. The binding is absent.
    Personal,
    /// A shareable database bound to one spec.
    Share(ShareScope),
}

/// Namespaces a shareable binding may store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareScope {
    binding: SpecBinding,
    namespaces: &'static [&'static str],
}

impl ShareScope {
    /// Borrow the binding that produced this scope.
    pub fn binding(&self) -> &SpecBinding {
        &self.binding
    }

    /// Exact namespaces this binding publishes.
    pub fn namespaces(&self) -> &'static [&'static str] {
        self.namespaces
    }
}

/// Whether a namespace may enter the bound share database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceClass {
    /// The bound spec lists this namespace.
    Official,
    /// The namespace stays in `personal`.
    AppPrivate,
}

/// Why admission refused the database, binding, or namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// Share admission was asked for no binding.
    BindingAbsent,
    /// The spec name is not in the catalog.
    UnknownSpec,
    /// The spec name exists and this version does not.
    UnsupportedVersion,
    /// The name is reserved and has no spec.
    Unprovisioned,
    /// A reserved provisioned name was given a different binding.
    ReservedBindingMismatch,
    /// Personal data was offered as a share, or a second personal database was asked for.
    PersonalNotShareable,
    /// The database name or spec name is outside the selector alphabet.
    InvalidName,
    /// The namespace is outside the selector alphabet.
    InvalidNamespace,
}

impl PolicyError {
    /// Stable denial text. It contains no database contents.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BindingAbsent => "share creation requires a spec binding",
            Self::UnknownSpec => "unknown database spec",
            Self::UnsupportedVersion => "unsupported database spec version",
            Self::Unprovisioned => "database name is reserved and unprovisioned",
            Self::ReservedBindingMismatch => "reserved database name requires its catalog spec",
            Self::PersonalNotShareable => "personal database is not shareable",
            Self::InvalidName => "invalid database or spec name",
            Self::InvalidNamespace => "invalid namespace",
        }
    }
}

/// `Some` when `name` is one of the reserved database names.
pub fn reserved_name(name: &str) -> Option<Reservation> {
    match name {
        "graph" => Some(Reservation::Unprovisioned),
        "social" | "medical" | "finance" | "work" => Some(Reservation::RequiresOwnSpec),
        _ => None,
    }
}

/// Admit one named database. `personal` takes no binding. A share without a
/// binding is [`PolicyError::BindingAbsent`].
pub fn admit_database(
    name: &str,
    class: DatabaseClass,
    binding: Option<&SpecBinding>,
) -> Result<Admission, PolicyError> {
    if !valid_label(name) {
        return Err(PolicyError::InvalidName);
    }
    if name.eq_ignore_ascii_case(PERSONAL) {
        return admit_personal(name, class, binding);
    }
    match reserved_name(name) {
        Some(Reservation::Unprovisioned) => Err(PolicyError::Unprovisioned),
        Some(Reservation::RequiresOwnSpec) => admit_own_spec(name, class, binding),
        None => admit_named_instance(class, binding),
    }
}

/// Admit a share binding. `None` is [`PolicyError::BindingAbsent`].
pub fn admit_share(binding: Option<&SpecBinding>) -> Result<ShareScope, PolicyError> {
    let Some(binding) = binding else {
        return Err(PolicyError::BindingAbsent);
    };
    let spec = catalog::share_spec(binding.name(), binding.version())?;
    Ok(ShareScope {
        binding: binding.clone(),
        namespaces: spec.namespaces,
    })
}

/// Classify one namespace for a share binding. A missing or unknown binding
/// is an error, not an allow.
pub fn admit_namespace(
    binding: Option<&SpecBinding>,
    namespace: &str,
) -> Result<NamespaceClass, PolicyError> {
    if !valid_label(namespace) {
        return Err(PolicyError::InvalidNamespace);
    }
    let scope = admit_share(binding)?;
    if scope.namespaces.contains(&namespace) {
        Ok(NamespaceClass::Official)
    } else {
        Ok(NamespaceClass::AppPrivate)
    }
}

/// Schemas and fields the binding publishes.
pub fn schemas(binding: &SpecBinding) -> Result<&'static [SchemaSpec], PolicyError> {
    Ok(catalog::share_spec(binding.name(), binding.version())?.schemas)
}

/// True when the bound spec lists this schema. This does not make a file eligible.
pub fn schema_is_shareable(
    binding: &SpecBinding,
    namespace: &str,
    schema: &str,
) -> Result<bool, PolicyError> {
    let listed = schemas(binding)?;
    Ok(listed
        .iter()
        .any(|item| item.namespace == namespace && item.name == schema))
}

/// True only for eligible file bytes on a listed schema.
///
/// Social `Photo` and `lastdb-files` `FileChunk` qualify. A listed schema
/// with no file, and every private schema, is `Ok(false)`. A bad binding
/// is an error. A missing spec is not classified here.
pub fn file_is_shareable(
    binding: &SpecBinding,
    namespace: &str,
    schema: &str,
) -> Result<bool, PolicyError> {
    let listed = schema_is_shareable(binding, namespace, schema)?;
    Ok(listed && catalog::carries_file(namespace, schema))
}

/// True when LastDB may publish this record on the binding.
///
/// The schema and every named field must be listed. `file` is true only when
/// the mutation carries the record's file bytes. Those bytes are eligible for
/// social `Photo` and `lastdb-files` `FileChunk`. A private schema, an unlisted
/// field, or any other file is `Ok(false)`. A bad binding is an error.
/// A missing spec is not classified here.
pub fn record_is_shareable(
    binding: &SpecBinding,
    namespace: &str,
    schema: &str,
    fields: &[&str],
    file: bool,
) -> Result<bool, PolicyError> {
    if !schema_is_shareable(binding, namespace, schema)? {
        return Ok(false);
    }
    for field in fields {
        if !field_is_shareable(binding, namespace, schema, field)? {
            return Ok(false);
        }
    }
    if file {
        file_is_shareable(binding, namespace, schema)
    } else {
        Ok(true)
    }
}

/// True only when the bound spec lists this field. Other fields stay in `personal`.
pub fn field_is_shareable(
    binding: &SpecBinding,
    namespace: &str,
    schema: &str,
    field: &str,
) -> Result<bool, PolicyError> {
    let listed = schemas(binding)?;
    Ok(listed.iter().any(|item| {
        item.namespace == namespace && item.name == schema && item.fields.contains(&field)
    }))
}

/// Standard schemas that are personal-only. App-private namespaces are every
/// namespace a share binding does not list, including these.
pub fn personal_schemas() -> &'static [SchemaSpec] {
    catalog::PERSONAL_SCHEMAS
}

fn admit_personal(
    name: &str,
    class: DatabaseClass,
    binding: Option<&SpecBinding>,
) -> Result<Admission, PolicyError> {
    if name == PERSONAL && class == DatabaseClass::Personal && binding.is_none() {
        Ok(Admission::Personal)
    } else {
        Err(PolicyError::PersonalNotShareable)
    }
}

fn admit_own_spec(
    name: &str,
    class: DatabaseClass,
    binding: Option<&SpecBinding>,
) -> Result<Admission, PolicyError> {
    if class != DatabaseClass::Shareable {
        return Err(PolicyError::PersonalNotShareable);
    }
    let Some(binding) = binding else {
        return Err(PolicyError::BindingAbsent);
    };
    if binding.name() != name {
        return Err(PolicyError::ReservedBindingMismatch);
    }
    admit_share(Some(binding)).map(Admission::Share)
}

fn admit_named_instance(
    class: DatabaseClass,
    binding: Option<&SpecBinding>,
) -> Result<Admission, PolicyError> {
    if class != DatabaseClass::Shareable {
        return Err(PolicyError::PersonalNotShareable);
    }
    admit_share(binding).map(Admission::Share)
}

fn valid_label(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}
