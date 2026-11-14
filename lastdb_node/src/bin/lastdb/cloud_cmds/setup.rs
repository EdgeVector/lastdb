//! Native backup home initialization, self-serve cloud enrollment, and existing connections.
use super::enrollment_home::{PreparedHome, PENDING_FILE};
use super::snapshot_wait::print_backup;
use super::{block_on_cloud, read_stdin_no_control, BackupConnectArgs, BackupInitArgs, FreshHome};
use cloud_backup_protocol::{AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{
    register_native_device, SavedConnection, SavedCredentials, CLOUD_CREDENTIALS_FILE,
};
use lastdb_node::host::{user_hash_from_pubkey, IDENTITY_KEY_FILE};
use lastdb_node::offline_home::Directory;
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use std::path::Path;
use zeroize::Zeroizing;

pub(super) const PHRASE_FILE: &str = "recovery-phrase.txt";

pub(super) fn run_init(data_dir: Option<&Path>, args: &BackupInitArgs) -> Result<(), String> {
    if data_dir.is_some() {
        return Err("native backup init uses --into without --data-dir".into());
    }
    let env = Environment::try_from_str(&args.env)
        .map_err(|_| "native backup init environment must be dev or prod")?;
    initialize(&args.into, &args.recovery_phrase_out, env, args.json)
}

pub(super) fn run_connect(data_dir: Option<&Path>, args: &BackupConnectArgs) -> Result<(), String> {
    if args.existing {
        if !args.api_key_stdin || args.dev || args.invite_code_stdin {
            return Err(
                "existing connection requires only an environment and API key stdin".into(),
            );
        }
        let home = data_dir.ok_or("existing connection requires an explicit --data-dir")?;
        let env = args
            .env
            .as_deref()
            .ok_or("existing connection requires --env")?;
        return connect_existing(home, env, args.expected_user_hash.as_deref(), args.json);
    }
    if args.api_key_stdin || args.expected_user_hash.is_some() {
        return Err("native backup connect requires --env <dev|prod> or --dev".into());
    }
    let env = match (args.dev, args.env.as_deref()) {
        (true, None) | (false, Some("dev")) => Environment::Dev,
        (false, Some("prod")) => Environment::Prod,
        _ => return Err("native backup connect requires --env <dev|prod> or --dev".into()),
    };
    let home = data_dir.ok_or("native backup connect requires an explicit --data-dir")?;
    connect(home, env, args.invite_code_stdin, args.json)
}

fn initialize(
    destination: &Path,
    phrase_path: &Path,
    env: Environment,
    json: bool,
) -> Result<(), String> {
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
    let account = user_hash_from_pubkey(&pair.public_key_base64())?;
    let mut target = target.create()?;
    target.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    target.write(PHRASE_FILE, phrase.as_bytes())?;
    target.commit()?;
    print_backup(
        &serde_json::json!({
            "schema": "lastdb.native-backup-init.v1", "ok": true, "environment": env.as_str(),
            "account_id": account, "home": destination, "recovery_phrase_file": phrase_path,
            "cloud_sync_off": true, "local_store_opened": false, "network_request": false,
            "identity_seed_output": false, "recovery_phrase_output": false,
        }),
        json,
    )
}

pub(super) fn canonical_backup_endpoint(env: Environment, api_key: &str) -> Result<String, String> {
    let auth = lastdb_node::cloud_backup_client::AuthClient::new(
        exemem_api_url_for(env).to_owned(),
        api_key.to_owned(),
    );
    lastdb_node::cloud_backup_client::byte_backup::ByteBackupClient::from_auth(auth)
        .map_err(|e| e.to_string())
        .map(|c| c.canonical_endpoint())
}

pub(super) fn encode_connected_credentials(
    env: Environment,
    account_id: &AccountId,
    api_key: String,
    existing_home: Option<Id>,
) -> Result<Zeroizing<Vec<u8>>, String> {
    let endpoint = canonical_backup_endpoint(env, &api_key)?;
    let source_home = if let Some(id) = existing_home {
        id
    } else {
        Id::try_from(uuid::Uuid::new_v4().to_string())
            .map_err(|_| "cannot name this home".to_owned())?
    };
    let mut creds = SavedCredentials::new(exemem_api_url_for(env), api_key);
    creds.connection = Some(SavedConnection {
        account_id: account_id.clone(),
        source_home,
        environment: env.as_str().to_owned(),
        endpoint,
    });
    creds
        .to_json()
        .map_err(|_| "cannot encode cloud credentials".into())
}

fn account_from_seed_bytes(seed: &[u8]) -> Result<(Ed25519KeyPair, AccountId), String> {
    let pair = Ed25519KeyPair::from_secret_key(seed).map_err(|_| "invalid native identity")?;
    let account_id = AccountId::try_from(user_hash_from_pubkey(&pair.public_key_base64())?)
        .map_err(|_| "invalid native account identity".to_owned())?;
    Ok((pair, account_id))
}

fn connect(home: &Path, env: Environment, invite_stdin: bool, json: bool) -> Result<(), String> {
    let dir = PreparedHome::open(home)?;
    let seed = dir.seed()?;
    let (pair, account_id) = account_from_seed_bytes(seed.as_ref())?;
    let emit = |registered: bool| -> Result<(), String> {
        print_backup(
            &serde_json::json!({
                "schema": "lastdb.native-backup-connect.v1", "ok": true, "environment": env.as_str(),
                "account_id": account_id, "home": home, "credential_file": CLOUD_CREDENTIALS_FILE,
                "cloud_sync_off": false, "local_store_opened": false,
                "local_store_read_only": false, "database_writer_opened": false,
                "registration_attempted": registered, "credentials_saved": true,
                "api_key_output": false, "restored_home": false, "automatic_backup_enabled": true,
            }),
            json,
        )
    };
    if dir.finish_saved_credentials(env, &account_id)? {
        return emit(false);
    }
    let invite = invite_stdin
        .then(|| read_stdin_no_control(4096, "native enrollment invite"))
        .transpose()?;
    let device_id = dir.device_id()?;
    let fut = register_native_device(
        env,
        &pair,
        &device_id,
        invite.as_deref().map(String::as_str),
    );
    let (api_key, user_hash) = block_on_cloud("native enrollment", fut).map_err(|e| {
        format!("{e}; registration may have committed; retry only by an explicit request")
    })?;
    if user_hash != account_id.as_str()
        || dir.seed()?.as_ref() != seed.as_ref()
        || dir.device_id()? != device_id
    {
        return Err("native enrollment identity changed; registration may have committed".into());
    }
    let creds = encode_connected_credentials(env, &account_id, api_key, None)?;
    dir.write_new(PENDING_FILE, &creds)
        .and_then(|()| dir.publish())
        .map_err(|e| format!("{e}; registration may have committed; retain this home"))?;
    emit(true)
}

fn connect_existing(
    home: &Path,
    env_str: &str,
    expected: Option<&str>,
    json: bool,
) -> Result<(), String> {
    let env = Environment::try_from_str(env_str)
        .map_err(|_| "existing connection requires an absolute home and dev or prod environment")?;
    let expected = expected
        .map(|v| AccountId::try_from(v.to_owned()).map_err(|_| "invalid expected account identity"))
        .transpose()?;
    let api_key = read_stdin_no_control(4096, "existing account API key")?;
    let dir = Directory::open(home).map_err(|e| e.to_string())?;
    let seed = Zeroizing::new(
        dir.read(IDENTITY_KEY_FILE)
            .map_err(|e| e.to_string())?
            .ok_or("missing home identity")?,
    );
    if seed.len() != 32 {
        return Err("identity must contain exactly 32 bytes".into());
    }
    let (_, account_id) = account_from_seed_bytes(&seed)?;
    if expected.as_ref().is_some_and(|id| id != &account_id) {
        return Err("expected account identity does not match this home".into());
    }
    let (api_url, endpoint) = (
        exemem_api_url_for(env),
        canonical_backup_endpoint(env, api_key.as_str())?,
    );
    let prev = dir
        .read(CLOUD_CREDENTIALS_FILE)
        .map_err(|e| e.to_string())?;
    let existing_home = if let Some(bytes) = &prev {
        let saved =
            SavedCredentials::parse(bytes).map_err(|_| "invalid source cloud credentials")?;
        if saved.api_url != api_url || saved.api_key.as_str() != api_key.as_str() {
            return Err("saved cloud credentials do not match this connection".into());
        }
        if let Some(conn) = &saved.connection {
            if conn.account_id != account_id
                || conn.environment != env.as_str()
                || conn.endpoint != endpoint
            {
                return Err("saved cloud connection does not match this account".into());
            }
        }
        saved.connection.map(|c| c.source_home)
    } else {
        None
    };
    let encoded =
        encode_connected_credentials(env, &account_id, api_key.as_str().to_owned(), existing_home)?;
    dir.publish(
        CLOUD_CREDENTIALS_FILE,
        prev.as_deref().map(Vec::as_slice),
        &encoded,
    )
    .map_err(|e| e.to_string())?;
    print_backup(
        &serde_json::json!({
            "ok": true, "environment": env.as_str(), "account_id": account_id,
            "credentials_saved": true, "registration_attempted": false,
        }),
        json,
    )
}
