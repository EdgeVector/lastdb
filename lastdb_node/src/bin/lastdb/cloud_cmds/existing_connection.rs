use super::{
    open_directory, read_private_file, require_owned_directory, user_hash_from_pubkey, AccountId,
    Ed25519KeyPair, Environment, IDENTITY_KEY_FILE,
};
use lastdb_node::cloud::{SavedConnection, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::profile::endpoints::exemem_api_url_for;
use std::path::Path;
use zeroize::Zeroizing;

pub(super) fn home_account(home: &Path) -> Result<AccountId, String> {
    let directory = open_directory(home)?;
    require_owned_directory(&directory)?;
    let seed = read_private_file(&directory, IDENTITY_KEY_FILE, 32)?;
    if seed.len() != 32 {
        return Err("identity must contain exactly 32 bytes".into());
    }
    let pair = Ed25519KeyPair::from_secret_key(&seed).map_err(|_| "invalid identity")?;
    let account = user_hash_from_pubkey(&pair.public_key_base64())?;
    AccountId::try_from(account).map_err(|_| "invalid account identity".into())
}

pub(super) fn save(
    home: &Path,
    env: &str,
    account_id: &AccountId,
    api_key: &str,
) -> Result<(), String> {
    let environment =
        Environment::try_from_str(env).map_err(|_| "recovery environment must be dev or prod")?;
    let api_url = exemem_api_url_for(environment);
    let endpoint = lastdb_node::cloud_backup_client::byte_backup::ByteBackupClient::from_auth(
        lastdb_node::cloud_backup_client::AuthClient::new(api_url.to_owned(), api_key.to_owned()),
    )
    .map_err(|error| error.to_string())?
    .canonical_endpoint();
    let directory = lastdb_node::offline_home::Directory::open(home).map_err(|e| e.to_string())?;
    let previous = directory
        .read(CLOUD_CREDENTIALS_FILE)
        .map_err(|e| e.to_string())?;
    let mut creds = saved_credentials(&previous, api_url, api_key)?;
    let connection = SavedConnection {
        account_id: account_id.clone(),
        source_home: source_home(&creds)?,
        environment: env.to_owned(),
        endpoint,
    };
    if creds
        .connection
        .as_ref()
        .is_some_and(|saved| saved != &connection)
    {
        return Err("saved cloud connection does not match this account".into());
    }
    creds.connection = Some(connection);
    let encoded = creds
        .to_json()
        .map_err(|_| "cannot encode cloud credentials")?;
    directory
        .publish(
            CLOUD_CREDENTIALS_FILE,
            previous.as_ref().map(|bytes| bytes.as_slice()),
            &encoded,
        )
        .map_err(|e| e.to_string())
}

fn saved_credentials(
    previous: &Option<Zeroizing<Vec<u8>>>,
    api_url: &str,
    api_key: &str,
) -> Result<SavedCredentials, String> {
    match previous {
        Some(bytes) => {
            let saved =
                SavedCredentials::parse(bytes).map_err(|_| "invalid source cloud credentials")?;
            if saved.api_url != api_url || saved.api_key.as_str() != api_key {
                return Err("saved cloud credentials do not match this connection".into());
            }
            Ok(saved)
        }
        None => Ok(SavedCredentials::new(api_url, api_key.to_owned())),
    }
}

fn source_home(creds: &SavedCredentials) -> Result<cloud_backup_protocol::Id, String> {
    match &creds.connection {
        Some(saved) => Ok(saved.source_home.clone()),
        None => cloud_backup_protocol::Id::try_from(uuid::Uuid::new_v4().to_string())
            .map_err(|_| "cannot name this home".into()),
    }
}
