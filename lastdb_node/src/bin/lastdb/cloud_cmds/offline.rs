use super::*;

mod signed;
use cloud_backup_protocol::v2::{context::ContextReadRequest, Version};
use cloud_backup_protocol::Id;

pub(super) fn run_byte_backup(home: &Path, action: ByteBackupCommand) -> Result<(), String> {
    let ByteBackupCommand::Restore(args) = action else {
        return Err("this home command is not a signed restore".into());
    };
    let phrase = super::read_stdin_secret(4096, "recovery phrase")?;
    super::block_on_cloud("signed restore", async {
        let account_id = lastdb_node::backup_access::account_from_phrase(&phrase)?;
        let client = lastdb_node::cloud::client_from_home(home)?;
        let scope = client
            .context(&ContextReadRequest {
                version: Version::V2,
                account_id,
                required_destinations: Vec::new(),
            })
            .await
            .map_err(|error| error.to_string())?
            .scope;
        signed::restore(home, &scope, &phrase, args).await
    })
}

pub(super) fn run_setup(source: Option<&Path>, action: &ByteBackupCommand) -> Result<(), String> {
    if let ByteBackupCommand::Connect(a) = action {
        if a.existing {
            if !a.api_key_stdin || a.dev || a.invite_code_stdin || a.restored {
                return Err(
                    "existing connection requires only an environment and API key stdin".into(),
                );
            }
            let home = source.ok_or("existing connection requires an explicit --data-dir")?;
            let env = a
                .env
                .as_deref()
                .ok_or("existing connection requires --env")?;
            return super::recovery_home::run_existing(
                home,
                env,
                a.expected_user_hash.as_deref(),
                a.json,
            );
        }
    }
    match action {
        ByteBackupCommand::Init(_) if source.is_some() => {
            Err("native backup init uses --into without --data-dir".into())
        }
        ByteBackupCommand::Init(a) => initialize(&a.into, &a.recovery_phrase_out, a.json),
        ByteBackupCommand::Connect(a) => {
            if !a.dev || a.env.is_some() || a.api_key_stdin || a.expected_user_hash.is_some() {
                return Err("native backup connect requires --dev".into());
            }
            if a.restored && a.invite_code_stdin {
                return Err("restored account reconnect does not accept a new invite".into());
            }
            super::connect(
                source.ok_or("native backup connect requires an explicit --data-dir")?,
                a.invite_code_stdin,
                a.restored,
                a.json,
            )
        }
        ByteBackupCommand::Restore(_) => Err("signed restore requires an explicit home".into()),
    }
}

fn initialize(destination: &Path, phrase_path: &Path, json: bool) -> Result<(), String> {
    if phrase_path != destination.join(PHRASE_FILE) {
        return Err("--recovery-phrase-out must name <into>/recovery-phrase.txt".into());
    }
    let target = FreshHome::preflight_empty(destination)?;
    let pair = Ed25519KeyPair::generate().map_err(|_| "cannot create fresh native identity")?;
    let seed = Zeroizing::new(pair.secret_key_bytes());
    let phrase = Zeroizing::new(
        bip39::Mnemonic::from_entropy(seed.as_ref())
            .map_err(|_| "cannot encode fresh native recovery phrase")?
            .to_string(),
    );
    let account = lastdb_node::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    let mut target = target.create()?;
    target.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    target.write(PHRASE_FILE, phrase.as_bytes())?;
    target.commit()?;
    print_backup(
        &serde_json::json!({
            "schema": "lastdb.native-backup-init.v1", "ok": true, "environment": "dev",
            "account_id": account, "home": destination, "recovery_phrase_file": phrase_path,
            "cloud_sync_off": true, "local_store_opened": false, "network_request": false,
            "identity_seed_output": false, "recovery_phrase_output": false,
        }),
        json,
    )
}

fn with_gen(
    scope: &cloud_backup_protocol::CloudScope,
    generation: Option<u64>,
) -> cloud_backup_protocol::CloudScope {
    let mut next = scope.clone();
    if let Some(generation) = generation {
        next.account_generation = cloud_backup_protocol::DecimalU64::new(generation);
    }
    next
}

fn id(value: String) -> Result<Id, String> {
    Id::try_from(value).map_err(|_| "signed restore requires a canonical database id".into())
}
