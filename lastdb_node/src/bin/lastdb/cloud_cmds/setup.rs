//! Native backup home initialization, self-serve cloud enrollment, and existing connections.
use super::enrollment_home::{resolve_existing_home_path, FreshHome, PreparedHome, PENDING_FILE};
use super::{print_backup, read_stdin_no_control, BackupConnectArgs, BackupInitArgs};
use cloud_backup_protocol::{AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use lastdb_node::cloud::{
    register_native_device, SavedConnection, SavedCredentials, CLOUD_CREDENTIALS_FILE,
};
use lastdb_node::host::{user_hash_from_pubkey, IDENTITY_KEY_FILE};
use lastdb_node::offline_home::Directory;
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

pub(super) const PHRASE_FILE: &str = "recovery-phrase.txt";

pub(super) fn run_init(data_dir: Option<&Path>, args: &BackupInitArgs) -> Result<(), String> {
    if data_dir.is_some() {
        return Err("native backup init uses --into without --data-dir".into());
    }
    let env = Environment::try_from_str(&args.env)
        .map_err(|_| "native backup init environment must be dev or prod")?;
    let into = lastdb_node::profile::paths::expand_tilde_path(&args.into)?;
    let phrase_out = lastdb_node::profile::paths::expand_tilde_path(&args.recovery_phrase_out)?;
    initialize(&into, &phrase_out, env, args.json)
}

fn resolve_connect_env(args: &BackupConnectArgs) -> Result<Environment, String> {
    let cli_env = match (args.dev, args.env.as_deref()) {
        (true, None) | (false, Some("dev")) => Some(Environment::Dev),
        (false, Some("prod")) => Some(Environment::Prod),
        (false, None) => None,
        _ => return Err("native backup connect requires --env <dev|prod> or --dev".into()),
    };
    cli_env
        .or_else(Environment::explicit_from_env)
        .ok_or_else(|| {
            if args.existing {
                "existing connection requires --env <dev|prod> or LASTDB_ENV".into()
            } else {
                "native backup connect requires --env <dev|prod>, --dev, or LASTDB_ENV".into()
            }
        })
}

pub(super) fn run_connect(
    data_dir: Option<PathBuf>,
    args: &BackupConnectArgs,
) -> Result<(), String> {
    let data_dir_explicit = data_dir.is_some();
    if args.existing {
        if !args.api_key_stdin || args.dev {
            return Err(
                "existing connection requires only an environment and API key stdin".into(),
            );
        }
        let env = resolve_connect_env(args)?;
        let home = lastdb_node::host::resolve_home(data_dir)?;
        return connect_existing(
            &home,
            data_dir_explicit,
            env,
            args.expected_user_hash.as_deref(),
            args.json,
        );
    }
    if args.api_key_stdin || args.expected_user_hash.is_some() {
        return Err("native backup connect requires --env <dev|prod> or --dev".into());
    }
    let env = resolve_connect_env(args)?;
    let home = lastdb_node::host::resolve_home(data_dir)?;
    connect(&home, data_dir_explicit, env, args.json)
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
    lastdb_node::cloud_backup_client::canonical_backup_endpoint(auth).map_err(|e| e.to_string())
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

pub(super) fn poke_running_daemon(data_dir_explicit: bool, home: &Path) {
    let socket = (!data_dir_explicit)
        .then(|| std::env::var_os("LASTDB_SOCKET_PATH"))
        .flatten()
        .filter(|value| !value.is_empty())
        .and_then(|value| lastdb_node::profile::paths::expand_tilde_path(value).ok())
        .unwrap_or_else(|| lastdb_node::lastdb_uds::socket_path(&home.join("data")));
    let _ = crate::post_json_with_timeout(
        &socket,
        lastdb_node::lastdb_uds::CLOUD_WAKE_PATH,
        &serde_json::json!({}),
        std::time::Duration::from_millis(500),
    );
}

fn connect(
    home: &Path,
    data_dir_explicit: bool,
    env: Environment,
    json: bool,
) -> Result<(), String> {
    let resolved = resolve_existing_home_path(home)?;
    let home = resolved.as_path();
    let dir = PreparedHome::open(home)?;
    let seed = dir.seed()?;
    let (pair, account_id) = account_from_seed_bytes(seed.as_ref())?;
    let emit = |registered: bool, api_key: Option<Zeroizing<String>>| -> Result<(), String> {
        lastdb_node::log_sync::request_wake(home);
        poke_running_daemon(data_dir_explicit, home);
        let mut out = serde_json::json!({
            "schema": "lastdb.native-backup-connect.v1", "ok": true, "environment": env.as_str(),
            "account_id": account_id, "home": home, "credential_file": CLOUD_CREDENTIALS_FILE,
            "cloud_sync_off": false, "local_store_opened": false,
            "local_store_read_only": false, "database_writer_opened": false,
            "registration_attempted": registered, "credentials_saved": true,
            "api_key_output": false, "restored_home": false, "automatic_backup_enabled": true,
        });
        attach_prod_payment_guidance(env, &account_id, api_key, &mut out, json);
        print_backup(&out, json)
    };
    if dir.finish_saved_credentials(env, &account_id)? {
        let saved_key = lastdb_node::cloud::load_saved_credentials(home)
            .ok()
            .flatten()
            .map(|c| c.api_key);
        return emit(false, saved_key);
    }
    let device_id = dir.device_id()?;
    let fut = register_native_device(env, &pair, &device_id);
    let (api_key, user_hash) = crate::block_on("native enrollment", fut).map_err(|e| {
        format!("{e}; registration may have committed; retry only by an explicit request")
    })?;
    if user_hash != account_id.as_str()
        || dir.seed()?.as_ref() != seed.as_ref()
        || dir.device_id()? != device_id
    {
        return Err("native enrollment identity changed; registration may have committed".into());
    }
    let probe_key = Zeroizing::new(api_key.clone());
    let creds = encode_connected_credentials(env, &account_id, api_key, None)?;
    dir.mark_signed_history_intent()
        .and_then(|()| dir.write_new(PENDING_FILE, &creds))
        .and_then(|()| dir.publish())
        .map_err(|e| format!("{e}; registration may have committed; retain this home"))?;
    emit(true, Some(probe_key))
}

fn connect_existing(
    home: &Path,
    data_dir_explicit: bool,
    env: Environment,
    expected: Option<&str>,
    json: bool,
) -> Result<(), String> {
    let resolved = resolve_existing_home_path(home)?;
    let home = resolved.as_path();
    let expected = expected
        .map(|v| AccountId::try_from(v.to_owned()).map_err(|_| "invalid expected account identity"))
        .transpose()?;
    let api_key = read_stdin_no_control(4096, "existing account API key")?;
    let dir = Directory::open(home).map_err(|e| e.to_string())?;
    let seed = dir.identity_seed()?.ok_or("missing home identity")?;
    let (_, account_id) = account_from_seed_bytes(seed.as_ref())?;
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
    if prev.is_none() {
        lastdb_node::log_sync::publish_signed_history_intent(&dir)?;
    }
    dir.publish(
        CLOUD_CREDENTIALS_FILE,
        prev.as_deref().map(Vec::as_slice),
        &encoded,
    )
    .map_err(|e| e.to_string())?;
    lastdb_node::log_sync::request_wake(home);
    poke_running_daemon(data_dir_explicit, home);
    let mut out = serde_json::json!({
        "ok": true, "environment": env.as_str(), "account_id": account_id,
        "credentials_saved": true, "registration_attempted": false,
    });
    attach_prod_payment_guidance(env, &account_id, Some(api_key), &mut out, json);
    print_backup(&out, json)
}

fn attach_prod_payment_guidance(
    env: Environment,
    account_id: &AccountId,
    api_key: Option<Zeroizing<String>>,
    output: &mut serde_json::Value,
    json: bool,
) {
    if env != Environment::Prod {
        return;
    }
    let status = api_key.and_then(|api_key| {
        let session = super::account::CloudSession {
            env,
            api_url: exemem_api_url_for(env),
            api_key,
            expected_account_id: Some(account_id.as_str().to_owned()),
        };
        let (code, body) = crate::block_on(
            "payment status",
            super::account::exchange_json(
                &session,
                reqwest::Method::GET,
                "/api/auth/payments/status",
                None,
            ),
        )
        .ok()?;
        if !(200..300).contains(&code)
            || body.get("ok").and_then(serde_json::Value::as_bool) != Some(true)
        {
            return None;
        }
        let sub = body
            .get("status")
            .and_then(serde_json::Value::as_str)?
            .trim()
            .to_owned();
        let configured = body
            .get("configured")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        Some((sub, configured))
    });
    let (sub_status, payment_required) = status.map_or((None, true), |(sub, configured)| {
        let active = !configured || sub == "active";
        (Some(sub), !active)
    });
    if let Some(obj) = output.as_object_mut() {
        if let Some(sub) = sub_status {
            obj.insert("subscription_status".into(), serde_json::json!(sub));
        }
        obj.insert(
            "payment_required".into(),
            serde_json::json!(payment_required),
        );
        if payment_required {
            obj.insert(
                "next_step".into(),
                serde_json::json!(
                    "run 'lastdb cloud payment checkout' to activate prod cloud writes"
                ),
            );
        }
    }
    if !json && payment_required {
        println!(
            "Note: {} before prod cloud writes can sync.",
            lastdb_node::cloud::PAYMENT_REQUIRED_MESSAGE
        );
    }
}
