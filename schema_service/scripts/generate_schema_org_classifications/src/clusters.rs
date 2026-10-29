//! Cluster definitions for hand-curated Schema.org classification.
//!
//! Each cluster matches a pattern in the Schema.org property name
//! (snake_case form) and assigns a `(sensitivity_level, data_domain,
//! interest_category)`. Order matters — the first cluster to match a
//! property name wins. Put the most specific / most-sensitive clusters
//! first.
//!
//! Unmatched properties fall back to the fail-closed baseline
//! (`sensitivity=4, domain="general", interest_category=None`) in the
//! generator's output, so they are still explicit in the JSON overlay —
//! the runtime loader trusts the overlay completely and never makes a
//! classification decision of its own.
//!
//! ## Classification scale (fold_db DataClassification)
//!
//! - 0 = Public — fine to share (URLs, titles, tag labels)
//! - 1 = Internal — rarely sensitive metadata (timestamps, hashes)
//! - 2 = Confidential — business/topic content (biographies, body text)
//! - 3 = Restricted — identifying/PII (email, phone, names, DOB)
//! - 4 = Highly Restricted — most sensitive (medical, financial,
//!   credentials, ethnicity, sexual/political)
//!
//! ## Data domains (must match DataClassification allowed domains)
//!
//! `general`, `identity`, `financial`, `medical`, `location`,
//! `communication`, `content`, `temporal`, `commerce`, `media`,
//! `social`, `document`.

/// How a cluster matches a snake_case property name.
#[derive(Debug, Clone, Copy)]
pub enum MatchRule {
    /// Exact field-name match (post-snake_case).
    Exact(&'static [&'static str]),
    /// Name starts with any of these (post-snake_case).
    Prefix(&'static [&'static str]),
    /// Name contains any of these (post-snake_case).
    Contains(&'static [&'static str]),
    /// Name ends with any of these (post-snake_case).
    Suffix(&'static [&'static str]),
}

/// One classification cluster.
#[derive(Debug, Clone, Copy)]
pub struct Cluster {
    pub name: &'static str,
    pub rule: MatchRule,
    pub sensitivity: u8,
    pub data_domain: &'static str,
    pub interest_category: Option<&'static str>,
}

impl Cluster {
    pub fn matches(&self, snake_name: &str) -> bool {
        match &self.rule {
            MatchRule::Exact(names) => names.contains(&snake_name),
            MatchRule::Prefix(prefixes) => prefixes.iter().any(|&p| snake_name.starts_with(p)),
            MatchRule::Contains(needles) => needles.iter().any(|&n| snake_name.contains(n)),
            MatchRule::Suffix(suffixes) => suffixes.iter().any(|&s| snake_name.ends_with(s)),
        }
    }
}

const SENSITIVE: &[Cluster] = &include!("clusters/sensitive.rs");
const COMMUNICATION: &[Cluster] = &include!("clusters/communication.rs");
const MEDIA: &[Cluster] = &include!("clusters/media.rs");
const CLUSTER_COUNT: usize = SENSITIVE.len() + COMMUNICATION.len() + MEDIA.len();

const fn concat_clusters() -> [Cluster; CLUSTER_COUNT] {
    let mut out = [Cluster {
        name: "",
        rule: MatchRule::Exact(&[]),
        sensitivity: 0,
        data_domain: "",
        interest_category: None,
    }; CLUSTER_COUNT];
    let mut index = 0;
    let mut part = 0;
    while part < SENSITIVE.len() {
        out[index] = SENSITIVE[part];
        index += 1;
        part += 1;
    }
    part = 0;
    while part < COMMUNICATION.len() {
        out[index] = COMMUNICATION[part];
        index += 1;
        part += 1;
    }
    part = 0;
    while part < MEDIA.len() {
        out[index] = MEDIA[part];
        index += 1;
        part += 1;
    }
    out
}

/// All clusters in priority order. First match wins.
///
/// Organizing principle: most-specific → least-specific, most-sensitive
/// → least-sensitive. A property like `patientEmail` should hit the
/// medical cluster (sensitivity=4) before the email cluster
/// (sensitivity=3), so medical clusters are listed first.
pub const CLUSTERS: &[Cluster] = &concat_clusters();
