//! CI-only real cryptography and local registration on disposable generated homes.
//! This does not qualify cloud publication, guest recovery, or live Share invite.
use chrono::{Duration, SecondsFormat, Utc};
use cloud_backup_client::byte_backup::{GuestReadPermission, ShareEnvironment};
use cloud_backup_protocol::{v2::Stream, AccountId, CloudScope, DecimalU64, Digest, Id};
use fold_db::security::Ed25519KeyPair;
use lastdb_node::{
    registered_share,
    share_invitation::{seal_invitation, ShareContact, ShareRecipient},
};
use rand::{rngs::OsRng, RngCore};
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Home(PathBuf);

impl Home {
    fn create() -> Result<Self> {
        if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true") {
            return Err("sharing qualification runs only in GitHub CI".into());
        }
        let runner = PathBuf::from(std::env::var("RUNNER_TEMP")?);
        if !runner.is_absolute() || !runner.is_dir() {
            return Err("RUNNER_TEMP must be an existing absolute directory".into());
        }
        let path = runner
            .canonicalize()?
            .join(format!("share-ci-{}", uuid::Uuid::new_v4()));
        DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }

    fn recipient(&self, name: &str, seed: &[u8; 32]) -> Result<PathBuf> {
        let home = self.0.join(name);
        DirBuilder::new().mode(0o700).create(&home)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(home.join(lastdb_identity::IDENTITY_KEY_FILE))?;
        file.write_all(seed)?;
        file.sync_all()?;
        Ok(home)
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        // Only the fresh, uniquely named directory created by this process.
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn seed() -> [u8; 32] {
    let mut seed = [0; 32];
    OsRng.fill_bytes(&mut seed);
    seed
}

fn permission(publisher: &Ed25519KeyPair, id: Id, hours: i64) -> Result<GuestReadPermission> {
    let public = publisher.public_key_bytes();
    let account = lastdb_identity::user_hash_from_pubkey(&publisher.public_key_base64())?;
    let stream = Stream {
        scope: CloudScope {
            account_id: AccountId::try_from(account)?,
            account_generation: DecimalU64::new(1),
        },
        stream_id: id,
    };
    let expiry = (Utc::now() + Duration::hours(hours)).to_rfc3339_opts(SecondsFormat::Secs, true);
    Ok(GuestReadPermission::sign(
        stream,
        ShareEnvironment::Dev,
        &expiry,
        public,
        |bytes| publisher.sign(bytes).to_bytes(),
    )?)
}

fn id() -> Result<Id> {
    Ok(Id::try_from(uuid::Uuid::new_v4().to_string())?)
}

fn cryptography(publisher: &Ed25519KeyPair, recipient: &ShareRecipient) -> Result<Vec<u8>> {
    let contact = ShareContact::parse(&recipient.contact().encode())?;
    assert!(contact.identity_public_key() == recipient.contact().identity_public_key());
    let permission = permission(publisher, id()?, 1)?;
    let publication_key = seed();
    let sealed = seal_invitation(&contact, &permission, &publication_key, publisher)?;
    let opened = recipient.open(&sealed, permission.publisher_public_key())?;
    assert!(*opened.publication_key == publication_key);
    assert!(opened.permission.stream() == permission.stream());
    assert!(opened.permission.environment() == ShareEnvironment::Dev);
    assert!(opened.permission.expires_ms() == permission.expires_ms());
    assert!(ShareRecipient::from_identity_seed(&seed())?
        .open(&sealed, permission.publisher_public_key())
        .is_err());
    let wrong_publisher = Ed25519KeyPair::from_secret_key(&seed())?;
    let wrong_key = Digest::try_from(hex::encode(wrong_publisher.public_key_bytes()))?;
    assert!(recipient.open(&sealed, &wrong_key).is_err());
    assert!(seal_invitation(&contact, &permission, &publication_key, &wrong_publisher).is_err());
    for offset in [0, sealed.len() / 2, sealed.len() - 1] {
        let mut tampered = sealed.clone();
        tampered[offset] ^= 1;
        assert!(recipient
            .open(&tampered, permission.publisher_public_key())
            .is_err());
    }
    let mut truncated = sealed.clone();
    truncated.pop();
    assert!(recipient
        .open(&truncated, permission.publisher_public_key())
        .is_err());
    let expired = permission_for_expiry(publisher)?;
    assert!(seal_invitation(&contact, &expired, &publication_key, publisher).is_err());
    Ok(sealed)
}

fn permission_for_expiry(publisher: &Ed25519KeyPair) -> Result<GuestReadPermission> {
    permission(publisher, id()?, -1)
}

fn registrations(
    home: &Path,
    publisher: &Ed25519KeyPair,
    recipient: &ShareRecipient,
    sealed: &[u8],
) -> Result<()> {
    let pinned = Digest::try_from(hex::encode(publisher.public_key_bytes()))?;
    let first = registered_share::accept(home, sealed, &pinned, Some("shared-files"))?;
    assert!(first.snapshot.is_none());
    assert!(!first.has_unexpired_contributor_grant());
    assert!(first.reader().is_err()); // No invented or fetched snapshot.
    let repeated = registered_share::accept(home, sealed, &pinned, None)?;
    assert!(first.id == repeated.id && repeated.name.as_deref() == Some("shared-files"));
    assert!(registered_share::list(home)?.len() == 1);
    for selector in [first.id.as_str(), "shared-files"] {
        let persisted = registered_share::resolve(home, selector)?.ok_or("registration missing")?;
        assert!(persisted.id == first.id && persisted.publisher == pinned);
        assert!(persisted.expires_ms == first.expires_ms && persisted.snapshot.is_none());
    }
    let contact = recipient.contact();
    let other = permission(publisher, id()?, 1)?;
    let other_sealed = seal_invitation(&contact, &other, &seed(), publisher)?;
    assert!(registered_share::accept(home, &other_sealed, &pinned, Some("shared-files")).is_err());
    for invalid in ["", "personal", "Personal", "../escape", first.id.as_str()] {
        assert!(registered_share::accept(home, &other_sealed, &pinned, Some(invalid)).is_err());
    }
    let changed = permission(publisher, first.id.clone(), 1)?;
    let changed_key = seal_invitation(&contact, &changed, &seed(), publisher)?;
    assert!(registered_share::accept(home, &changed_key, &pinned, None).is_err());
    assert!(registered_share::list(home)?.len() == 1);
    let second = registered_share::accept(home, &other_sealed, &pinned, Some("second"))?;
    assert!(first.id != second.id && registered_share::list(home)?.len() == 2);
    assert!(registered_share::resolve(home, "missing")?.is_none());
    Ok(())
}

fn run() -> Result<()> {
    let root = Home::create()?;
    let recipient_seed = seed();
    let home = root.recipient("recipient", &recipient_seed)?;
    let wrong_home = root.recipient("wrong-recipient", &seed())?;
    let recipient = ShareRecipient::from_identity_seed(&recipient_seed)?;
    let publisher = Ed25519KeyPair::from_secret_key(&seed())?;
    let sealed = cryptography(&publisher, &recipient)?;
    let pinned = Digest::try_from(hex::encode(publisher.public_key_bytes()))?;
    assert!(registered_share::accept(&wrong_home, &sealed, &pinned, None).is_err());
    assert!(registered_share::list(&wrong_home)?.is_empty());
    registrations(&home, &publisher, &recipient, &sealed)?;
    fs::remove_dir_all(&root.0)?;
    println!("Share invitation cryptography and local registration passed; cloud publication not exercised.");
    Ok(())
}

fn main() {
    if run().is_err() {
        // Never print credentials, invitation contents, keys, or file bodies.
        eprintln!("Share invitation/local registration qualification failed");
        std::process::exit(1);
    }
}
