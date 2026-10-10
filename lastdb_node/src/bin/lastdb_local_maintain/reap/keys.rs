//! Key classes of the tip plane and the rules for one molecule token.
//!
//! A token is one spelling of a molecule id. The builders come from
//! `molecule_key_codec` wherever production has one. The few keys with no
//! public builder (`mcc`, `ref`, `rdel:v1`) are written here, and the tests
//! pin their exact bytes.

use std::borrow::Cow;

use fold_db::atom::molecule_key_codec as codec;
use fold_db::atom::molecule_uuid::parse_molecule_uuid_bytes;
use fold_db::hex::{hex_decode, hex_lower};
use fold_db::kind_partition::{anchored, form_twin, split_org_storage_prefix};
use sha2::{Digest, Sha256};

/// The 32-byte identity of a molecule id.
///
/// A digest spelling (43 base64url or 64 hex characters) gives its digest.
/// Any other id gives the SHA-256 of its text. Compare ids by this value, never
/// by spelling.
pub(crate) type MolKey = [u8; 32];

pub(crate) fn mol_key(id: &str) -> MolKey {
    parse_molecule_uuid_bytes(id).unwrap_or_else(|| Sha256::digest(id.as_bytes()).into())
}

/// The key classes whose rows belong to one molecule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Class {
    Mk,
    Mh,
    MordColon,
    MordSparse,
    MordAnchored,
    MocColon,
    MocAnchored,
    Mgp,
    Mgr,
    Mgd,
    HistoryColon,
    HistoryAnchored,
    ConflictColon,
    ConflictAnchored,
    RdelV1,
    RdelV2,
    Mcc,
    Ref,
    /// A key with an org or share scope in front. Never deleted.
    Scoped,
    /// A key of any other class.
    Other,
}

impl Class {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Mk => "mk",
            Self::Mh => "mh",
            Self::MordColon => "mord_colon",
            Self::MordSparse => "mord_sparse",
            Self::MordAnchored => "mord_anchored",
            Self::MocColon => "moc_colon",
            Self::MocAnchored => "moc_anchored",
            Self::Mgp => "mgp",
            Self::Mgr => "mgr",
            Self::Mgd => "mgd",
            Self::HistoryColon => "history_colon",
            Self::HistoryAnchored => "history_anchored",
            Self::ConflictColon => "conflict_colon",
            Self::ConflictAnchored => "conflict_anchored",
            Self::RdelV1 => "rdel_v1",
            Self::RdelV2 => "rdel_v2",
            Self::Mcc => "mcc",
            Self::Ref => "ref",
            Self::Scoped => "scoped",
            Self::Other => "other",
        }
    }
}

/// The class of a key and the molecule token in it, when it has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Classified<'a> {
    pub class: Class,
    pub token: Option<Cow<'a, str>>,
}

fn upto<'a>(rest: &'a str, stops: &[char]) -> &'a str {
    rest.find(stops).map_or(rest, |at| &rest[..at])
}

fn by(class: Class, token: &str) -> Classified<'_> {
    Classified {
        class,
        token: Some(Cow::Borrowed(token)),
    }
}

/// Classify a key by its head. The classifier and the rules are two
/// separate readings of the same key shapes. The tip pass compares them.
pub(crate) fn classify(key: &str) -> Classified<'_> {
    if split_org_storage_prefix(key).is_some() || key.starts_with("from:") {
        return Classified {
            class: Class::Scoped,
            token: None,
        };
    }
    if let Some(rest) = key.strip_prefix("mk:") {
        return by(Class::Mk, upto(rest, &[':']));
    }
    if let Some(rest) = key.strip_prefix("mh:") {
        return by(Class::Mh, rest);
    }
    if let Some(rest) = key.strip_prefix("mord:") {
        let token = upto(rest, &[':', '\0']);
        let sparse = rest.as_bytes().get(token.len()) == Some(&0);
        return by(
            if sparse {
                Class::MordSparse
            } else {
                Class::MordColon
            },
            token,
        );
    }
    if let Some(rest) = key.strip_prefix("mord\0") {
        return by(Class::MordAnchored, upto(rest, &[':']));
    }
    if let Some(rest) = key.strip_prefix("moc:") {
        return by(Class::MocColon, rest);
    }
    if let Some(rest) = key.strip_prefix("moc\0") {
        return by(Class::MocAnchored, rest);
    }
    if let Some(rest) = key.strip_prefix(codec::MOLECULE_GENERATION_POINTER_PREFIX) {
        return by(Class::Mgp, rest);
    }
    if let Some(rest) = key.strip_prefix(codec::MOLECULE_GENERATION_RECORD_PREFIX) {
        return by(Class::Mgr, upto(rest, &[':']));
    }
    if let Some(rest) = key.strip_prefix(codec::MOLECULE_GENERATION_DELETE_PREFIX) {
        return by(Class::Mgd, upto(rest, &[':']));
    }
    if let Some(rest) = key.strip_prefix("history:") {
        return by(Class::HistoryColon, upto(rest, &[':']));
    }
    if let Some(rest) = key.strip_prefix("history\0") {
        return by(Class::HistoryAnchored, upto(rest, &[':']));
    }
    if let Some(rest) = key.strip_prefix("conflict:") {
        return by(Class::ConflictColon, upto(rest, &[':']));
    }
    if let Some(rest) = key.strip_prefix("conflict\0") {
        return by(Class::ConflictAnchored, upto(rest, &[':']));
    }
    if let Some(rest) = key.strip_prefix("rdel:v1:") {
        return by(Class::RdelV1, upto(rest, &['\0']));
    }
    if let Some(rest) = key.strip_prefix("rdel:v2:") {
        return classify_rdel_v2(rest);
    }
    if let Some(rest) = key.strip_prefix("mcc:") {
        return by(Class::Mcc, rest);
    }
    if let Some(rest) = key.strip_prefix("ref:") {
        return by(Class::Ref, rest);
    }
    Classified {
        class: Class::Other,
        token: None,
    }
}

/// `rdel:v2:{hex(mk key through its first NUL)}\0{len}:{hex(mk key)}`.
fn classify_rdel_v2(rest: &str) -> Classified<'_> {
    let token = hex_decode(upto(rest, &['\0']))
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .and_then(|text| {
            text.strip_prefix("mk:")
                .map(|tail| upto(tail, &[':']).to_string())
        });
    Classified {
        class: Class::RdelV2,
        token: token.map(Cow::Owned),
    }
}

/// Segments of a key that look like a molecule token (43 or 64 characters).
///
/// This is the needle reader for keys the classifier does not know.
pub(crate) fn token_shaped_segments(key: &str) -> impl Iterator<Item = &str> {
    key.split([':', '\0'])
        .filter(|segment| segment.len() == 43 || segment.len() == 64)
}

/// The rules of one molecule token in the tip plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenRules {
    pub prefixes: Vec<Vec<u8>>,
    pub exacts: Vec<Vec<u8>>,
}

/// The rules of one spelling of a molecule id.
pub(crate) fn token_rules(token: &str) -> TokenRules {
    let colon_twin =
        |anchored_key: &str| form_twin(anchored_key).unwrap_or_else(|| anchored_key.to_string());
    let conflict = anchored("conflict", &format!("{token}:"));
    let prefixes = [
        codec::molecule_record_prefix(token),
        colon_twin(&codec::order_log_prefix(token)),
        codec::sparse_order_log_prefix(token),
        codec::order_log_prefix(token),
        format!("{}{token}:", codec::MOLECULE_GENERATION_RECORD_PREFIX),
        codec::molecule_generation_delete_prefix(token),
        colon_twin(&codec::history_molecule_prefix(token)),
        codec::history_molecule_prefix(token),
        colon_twin(&conflict),
        conflict,
        format!("rdel:v1:{token}\0"),
        format!(
            "rdel:v2:{}",
            hex_lower(codec::molecule_record_prefix(token))
        ),
    ];
    let exacts = [
        codec::header_key(token),
        colon_twin(&codec::order_count_key(token)),
        codec::order_count_key(token),
        codec::molecule_generation_pointer_key(token),
        format!("mcc:{token}"),
        format!("ref:{token}"),
    ];
    TokenRules {
        prefixes: prefixes.into_iter().map(String::into_bytes).collect(),
        exacts: exacts.into_iter().map(String::into_bytes).collect(),
    }
}

/// The per-molecule manifest keys of the compact edge plane.
pub(crate) fn aref_manifest_keys(token: &str) -> [Vec<u8>; 2] {
    [
        codec::atom_ref_v2_molecule_manifest_key(token).into_bytes(),
        codec::atom_ref_v2_history_upgrade_key(token).into_bytes(),
    ]
}
