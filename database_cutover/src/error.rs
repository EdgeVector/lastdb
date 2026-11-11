use std::fmt;

/// One reason a cutover decision was refused. The previous record stays as it was.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reason {
    /// The evidence file is larger than [`crate::MAX_EVIDENCE_BYTES`] or is not the schema.
    Unreadable {
        /// Evidence file name.
        file: &'static str,
    },
    /// `environment` is not `dev`.
    NotDev {
        /// Evidence file name.
        file: &'static str,
    },
    /// `schema` is not [`crate::SCHEMA`].
    Schema {
        /// Evidence file name.
        file: &'static str,
    },
    /// `kind` is not the value this file must carry.
    Kind {
        /// Evidence file name.
        file: &'static str,
    },
    /// `database_id` does not match the database the operator named.
    Database {
        /// Evidence file name.
        file: &'static str,
    },
    /// `database_kind` is not `personal` or `shareable`, or the files disagree.
    DatabaseKind {
        /// Evidence file name.
        file: &'static str,
    },
    /// The operator's database id is not a selector label.
    InvalidDatabaseId,
    /// The snapshot is not a fresh full snapshot of the final position.
    Snapshot,
    /// Legacy position, snapshot coverage, and the drained fence are not the same decimal.
    Position,
    /// Writer tips do not match an empty log or a non-empty log.
    WriterTips,
    /// Node-key grants are duplicated or the wrong shape for the database kind.
    Grants,
    /// Lane E has not promised to honor the fence after a future service commit.
    Daemon,
    /// Lane I has not promised to close legacy bearer reads after a future service commit.
    Delivery,
    /// An inactive request already exists and this evidence is not that request.
    RequestDiffers,
    /// The stored request disagrees with its own evidence digest.
    RecordInconsistent,
    /// The proof names the same device that is migrating.
    SameDevice,
    /// The second device did not ack the final legacy position, or keyed reads differ.
    SecondDevice,
    /// The stored file is not an inactive DEV request, or it activates or allows retirement.
    RecordRejected,
}

impl Reason {
    /// Stable code for an operator log. The text does not include evidence values.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unreadable { .. } => "unreadable",
            Self::NotDev { .. } => "not_dev",
            Self::Schema { .. } => "schema",
            Self::Kind { .. } => "kind",
            Self::Database { .. } => "database",
            Self::DatabaseKind { .. } => "database_kind",
            Self::InvalidDatabaseId => "invalid_database_id",
            Self::Snapshot => "snapshot",
            Self::Position => "position",
            Self::WriterTips => "writer_tips",
            Self::Grants => "grants",
            Self::Daemon => "daemon",
            Self::Delivery => "delivery",
            Self::RequestDiffers => "request_differs",
            Self::RecordInconsistent => "record_inconsistent",
            Self::SameDevice => "same_device",
            Self::SecondDevice => "second_device",
            Self::RecordRejected => "record_rejected",
        }
    }

    fn detail(&self) -> String {
        match self {
            Self::Unreadable { file } => {
                format!("{file} is missing, too large, or not this schema")
            }
            Self::NotDev { file } => format!("{file} environment is not dev"),
            Self::Schema { file } => format!("{file} schema is not {}", crate::SCHEMA),
            Self::Kind { file } => format!("{file} kind is not the required value"),
            Self::Database { file } => {
                format!("{file} database_id does not match the named database")
            }
            Self::DatabaseKind { file } => format!("{file} database_kind disagrees or is unknown"),
            Self::InvalidDatabaseId => "the named database_id is not a selector label".to_owned(),
            Self::Snapshot => {
                "snapshot is not a fresh full snapshot with bytes that match the position"
                    .to_owned()
            }
            Self::Position => {
                "snapshot, legacy position, and drained fence are not one canonical decimal"
                    .to_owned()
            }
            Self::WriterTips => "writer tips do not match the final legacy position".to_owned(),
            Self::Grants => "node-key grants are incomplete for this database kind".to_owned(),
            Self::Daemon => {
                "daemon sync has not promised to honor a future service commit and the fence"
                    .to_owned()
            }
            Self::Delivery => {
                "delivery has not promised to close legacy bearer reads after a service commit"
                    .to_owned()
            }
            Self::RequestDiffers => {
                "a request already exists for this database and the evidence differs".to_owned()
            }
            Self::RecordInconsistent => {
                "the stored request does not match its evidence digest".to_owned()
            }
            Self::SameDevice => "the second device is the migrating device".to_owned(),
            Self::SecondDevice => {
                "second-device ack or keyed reads do not match the final position".to_owned()
            }
            Self::RecordRejected => {
                "the file is not an inactive DEV request, or it would change access".to_owned()
            }
        }
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.detail())
    }
}

/// Every problem found in one decision. Empty is not a refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    reasons: Vec<Reason>,
}

impl Refusal {
    pub(crate) fn new(reasons: Vec<Reason>) -> Option<Self> {
        if reasons.is_empty() {
            None
        } else {
            Some(Self { reasons })
        }
    }

    pub(crate) fn one(reason: Reason) -> Self {
        Self {
            reasons: vec![reason],
        }
    }

    /// Reasons in check order.
    #[must_use]
    pub fn reasons(&self) -> &[Reason] {
        &self.reasons
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, reason) in self.reasons.iter().enumerate() {
            if index > 0 {
                writeln!(formatter)?;
            }
            write!(formatter, "{reason}")?;
        }
        Ok(())
    }
}
