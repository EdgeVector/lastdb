use crate::share_file::FileShareSchemas;
use crate::{DatabaseTarget, ShareFilesCommand};
use cloud_backup_protocol::Id;
use std::path::PathBuf;

mod publisher;
mod selected;

const SCHEMAS: FileShareSchemas<'static> = FileShareSchemas {
    catalog: "lastdb-files/FileCatalog",
    chunks: "lastdb-files/FileChunk",
};

pub(super) fn run(
    data_dir: Option<PathBuf>,
    db: Option<&str>,
    action: ShareFilesCommand,
) -> Result<(), String> {
    let selector = db
        .filter(|name| *name != "personal")
        .ok_or("share files requires --db with a Files database ID or name")?;
    if let ShareFilesCommand::Add { file } = action {
        return publisher::add(data_dir, selector, &file);
    }
    let target = DatabaseTarget::open(data_dir, Some(selector))?;
    match action {
        ShareFilesCommand::List => selected::list(&target),
        ShareFilesCommand::Save { file, out } => selected::save(&target, file, &out),
        _ => unreachable!("add and receive have dedicated routing"),
    }
}

fn id(value: String) -> Result<Id, String> {
    Id::try_from(value).map_err(|_| "invalid file UUID".into())
}
