//! Clap subcommands for schema inspection, liveness, name claims and retention.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum SchemaInspectCommand {
    /// Inspect preserved retired intent history on a closed owned temporary copy.
    RetiredIntent {
        #[command(subcommand)]
        command: RetiredIntentCommand,
    },
    /// Diagnose exact durable schema authority on a closed owned temporary copy.
    /// The schema name comes only from stdin; no names or stored values are output.
    ReplayAuthorityProbe {
        #[arg(long, required = true)]
        schema_name_stdin: bool,
        #[arg(long, required = true)]
        closed_owned_copy: bool,
    },
    /// Show bounded logical-current storage for one schema.
    ///
    /// This reads only the schema catalog and its declared molecule counters.
    /// Shared molecules and atoms intentionally appear in every schema that
    /// references them; this is not a partition of physical node disk bytes.
    Storage {
        /// Installed schema name or identity hash.
        schema: String,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Show labelled logical-current storage for all installed schemas.
    ///
    /// This reads the stored schema catalog and molecule counters. It does
    /// not scan atoms, tips, or filesystem planes.
    #[command(name = "storage-report", alias = "storage_report")]
    StorageReport {
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Print identity, key fields, field list, and legal query shapes.
    Show {
        /// Schema identity hash, `app/Name`, or descriptive name.
        name: String,
        /// Emit the raw `schema` object.
        #[arg(long)]
        json: bool,
    },
    /// Removes one installed schema identity from the catalog.
    ///
    /// After ACK, `schema show` and product reads for that identity miss.
    /// Product tips stay until a later janitor. Drop of a missing identity
    /// succeeds unless `--must-exist`. `--owner-app` drops every identity
    /// owned by that app as N catalog point writes, not a row scan.
    #[command(group(
        clap::ArgGroup::new("drop_target")
            .required(true)
            .args(["schema", "owner_app"])
    ))]
    Drop {
        /// Canonical name or identity hash of one installed schema.
        #[arg(long)]
        schema: Option<String>,
        /// Drop every installed identity owned by this app id.
        #[arg(long)]
        owner_app: Option<String>,
        /// Refuse if nothing was dropped.
        #[arg(long)]
        must_exist: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug, Clone, Copy)]
pub(crate) enum RetiredIntentCommand {
    /// Send the exact private source ID only through a FIFO to the proof command.
    LastSource {
        #[arg(long, required = true)]
        closed_owned_copy: bool,
    },
    /// Verify one exact source ID from stdin; output only fixed facts and counts.
    Proof {
        #[arg(long, required = true)]
        source_id_stdin: bool,
        #[arg(long, required = true)]
        closed_owned_copy: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum LivenessCommand {
    /// Explain why one atom, molecule, or blob cannot be reclaimed.
    Explain {
        /// Target class: atom, molecule, or blob.
        class: String,
        /// Target atom UUID, molecule UUID, or blob reference.
        id: String,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Rebuild derived liveness edges on an isolated-copy daemon.
    Bootstrap {
        /// Confirm that this daemon serves only an isolated data copy.
        #[arg(long)]
        isolated_copy: bool,
        /// Optional organization storage prefix.
        #[arg(long)]
        storage_prefix: Option<String>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum SchemaNameClaimCommand {
    /// Stop this schema answering `descriptive_name` resolution.
    Retire {
        /// Canonical schema name or identity hash of the claimant to retire.
        #[arg(long)]
        schema: String,
        #[arg(long)]
        json: bool,
    },
    /// Put a retired claim back.
    Restore {
        /// Canonical schema name or identity hash of the claimant to restore.
        #[arg(long)]
        schema: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum SchemaRetentionCommand {
    /// Read the current policy for one installed schema.
    ///
    /// Prints the TTL and its source: `manifest` (declared by an app manifest)
    /// or `operator` (set with `set`).
    Get {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        json: bool,
    },
    /// Set a positive human-readable duration such as `15m`, `24h`, or `7d`.
    ///
    /// A policy deletes records for good. Range and HashRange records expire
    /// by the entry time at the front of the range key: unix seconds,
    /// zero-padded to 20 digits. A range key without that prefix never
    /// expires. A Range schema loses every old record at the next sweep, those
    /// written before the policy included. A HashRange schema does the same in
    /// each partition that receives a later write. Hash and Single records
    /// expire by the time since the last write after the policy is set; older
    /// records are not tracked.
    ///
    /// This sets an OPERATOR policy. It wins over a TTL that an app manifest
    /// declared (`retention` key) until `clear` removes it. A schema declared
    /// from a manifest with `retention` has two installed names that share one
    /// policy: setting it through either name covers both. For any other
    /// schema the policy covers only the schema name given here. Every change
    /// writes a row to `<home>/logs/retention-audit.jsonl`.
    Set {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        ttl: String,
        /// Legacy HashRange partitions covered by this policy (repeatable).
        #[arg(long = "hash")]
        hash_partitions: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Clear the current policy for one installed schema.
    ///
    /// An operator policy that overrides a manifest policy is removed and the
    /// manifest policy is back in force. Any other policy is removed with its
    /// age tracking; a manifest policy then returns at the next declare.
    Clear {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        json: bool,
    },
}
