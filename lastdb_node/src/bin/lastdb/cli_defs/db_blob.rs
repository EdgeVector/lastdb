//! Clap subcommands for `lastdb db`: file-blob transfer verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbBlobCommand {
    /// Fetch exactly one `$lastdb_file` pointer's remote CAS object on demand.
    ///
    /// The pointer JSON must carry file blob access metadata. The daemon caches
    /// the verified bytes locally; this command writes the bytes to --out, or
    /// to stdout when --out is omitted. A blob stored with `put-blob-local` is
    /// read from the local plane; a node without cloud sync answers 404 when
    /// the blob is not stored on it.
    FetchFileBlob {
        /// JSON file containing the `$lastdb_file` field value.
        #[arg(long)]
        pointer_json: PathBuf,
        /// Write fetched bytes to this path instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Emit the daemon's JSON report instead of raw bytes / human summary.
        #[arg(long)]
        json: bool,
        /// Request the verified plaintext as a binary socket response.
        #[arg(long)]
        raw: bool,
    },
    /// Upload one personal file blob through the app-facing DB route.
    ///
    /// The running daemon must be cloud-sync capable and connected. The command
    /// stores the returned `$lastdb_file` pointer in the requested schema field.
    PutFileBlob {
        /// Schema name to mutate, for example `files/File`.
        #[arg(long)]
        schema: String,
        /// Field that should receive the `$lastdb_file` pointer.
        #[arg(long)]
        field: String,
        /// Hash key for the target record.
        #[arg(long)]
        key_hash: String,
        /// Optional range key for the target record.
        #[arg(long)]
        key_range: Option<String>,
        /// Plaintext file bytes to upload.
        #[arg(long)]
        bytes: PathBuf,
        /// Mutation type to write: create, update, delete, or purge.
        #[arg(long, default_value = "update")]
        mutation_type: String,
        /// Optional display filename for the pointer.
        #[arg(long)]
        name: Option<String>,
        /// Optional media type for the pointer.
        #[arg(long)]
        media_type: Option<String>,
        /// Keep a local plaintext CAS copy after upload.
        #[arg(long)]
        cache_local_plaintext: bool,
        /// Optional JSON object merged into the mutation fields.
        #[arg(long)]
        additional_fields_json: Option<PathBuf>,
        /// Write the returned `$lastdb_file` pointer JSON to this path.
        #[arg(long)]
        pointer_out: Option<PathBuf>,
        /// Send raw bytes over the socket instead of base64 in a JSON body.
        #[arg(long)]
        raw: bool,
    },
    /// Store one blob in this node's own `cas_blobs` plane and print its pointer.
    ///
    /// Needs no cloud sync. Writes no record: put the printed `$lastdb_file`
    /// pointer, as the whole value of a field of type `Any`, into a record of
    /// your own. A pointer inside a JSON string is not a reference: the blob is
    /// reclaimed once its row is older than 600 s. Identical bytes give an
    /// identical pointer. Reads the bytes from --file, or from stdin when stdin
    /// is a pipe. The row is on disk before the pointer is printed. A blob is at
    /// most 16 MiB unless the daemon's owner sets
    /// `LASTDB_LOCAL_FILE_BLOB_MAX_BYTES`; the daemon answers 413 above it, so
    /// store larger files as slabs. Read the bytes back with `db fetch-file-blob`.
    PutBlobLocal(PutBlobLocalArgs),
    /// Fork a shared `$lastdb_file` pointer into this node's personal blob scope.
    ///
    /// The daemon fetches the source pointer bytes, uploads them with a fresh
    /// personal file-blob access record, and rewrites the selected local field.
    ForkFileBlob {
        /// Schema containing the local file field to rewrite.
        #[arg(long)]
        schema: String,
        /// Field to rewrite with the forked `$lastdb_file` pointer.
        #[arg(long)]
        field: String,
        /// Hash-key value for simple hash-key schemas.
        #[arg(long, conflicts_with = "key_json")]
        key: Option<String>,
        /// JSON file containing a KeyValue object, e.g. {"hash":"id","range":null}.
        #[arg(long)]
        key_json: Option<PathBuf>,
        /// JSON file containing the source `$lastdb_file` field value.
        #[arg(long)]
        pointer_json: PathBuf,
        /// Override the pointer's file name in the rewritten local field.
        #[arg(long)]
        name: Option<String>,
        /// Override the pointer's media type in the rewritten local field.
        #[arg(long)]
        media_type: Option<String>,
        /// Seed local CAS with plaintext after the fork write.
        #[arg(long)]
        cache_local_plaintext: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    // NOTE: offline freelist compact was a sled-only tool and is gone with
    // the sled engine. Live `lastdb db *` verbs talk to the running daemon;
    // use Last Store tooling (`lastdb status`, restore, cloud heal) instead.
}
