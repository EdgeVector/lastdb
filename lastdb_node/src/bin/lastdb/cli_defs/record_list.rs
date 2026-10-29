//! Shared list/get-keys input; a named alternate always has a selected hash.

#[derive(clap::Args, Debug)]
pub(crate) struct RecordListArgs {
    pub schema: String,
    #[arg(long)]
    pub key_hash: Option<String>,
    /// Named alternate key. The default uses the schema's primary key.
    #[arg(long, requires = "key_hash")]
    pub key_name: Option<String>,
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
    #[arg(long)]
    pub cursor: Option<String>,
    #[arg(long)]
    pub json: bool,
}
