use super::{
    backend::DefinitionBackend,
    proxy::{Faults, Proxy},
};
use aws_sdk_s3::{
    config::{
        retry::RetryConfig, timeout::TimeoutConfig, Credentials, Region, RequestChecksumCalculation,
    },
    Client,
};
use schema_service_core::{
    types::CanonicalField, Clock, DisabledEmbeddingModel, SchemaConfig, SchemaServiceState,
    WallTime,
};
use schema_service_s3::S3BlobPersistence;
use schema_service_server_shared::DedicatedThreadRuntime;
use schema_types::{DeclarativeSchemaType, FieldValueType, KeyConfig, Schema};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub fn run<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

pub struct Harness {
    pub direct: Client,
    pub bucket: String,
    data: Client,
    ddb: aws_sdk_dynamodb::Client,
    proxy: Option<Proxy>,
    owner: Option<String>,
}

fn bounded(builder: aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder {
    builder
        .retry_config(RetryConfig::disabled())
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(30))
                .build(),
        )
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
}

fn loopback_address(endpoint: &str) -> SocketAddr {
    let port = endpoint
        .strip_prefix("http://127.0.0.1:")
        .or_else(|| endpoint.strip_prefix("http://localhost:"))
        .expect("protocol checks require an explicitly configured loopback HTTP endpoint")
        .parse::<u16>()
        .expect("endpoint port only, without a path");
    assert_ne!(port, 0);
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

impl Harness {
    pub async fn protocol(label: &str) -> Self {
        let endpoint = std::env::var("SCHEMA_S3_TEST_ENDPOINT").expect("job-local MinIO endpoint");
        let upstream = loopback_address(&endpoint);
        let credentials = Credentials::new(
            std::env::var("AWS_ACCESS_KEY_ID").expect("disposable MinIO access key"),
            std::env::var("AWS_SECRET_ACCESS_KEY").expect("disposable MinIO secret key"),
            None,
            None,
            "job-local-minio",
        );
        let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(credentials)
            .load()
            .await;
        let direct = Client::from_conf(
            bounded(aws_sdk_s3::config::Builder::from(&shared))
                .endpoint_url(&endpoint)
                .force_path_style(true)
                .build(),
        );
        let number = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let bucket = format!("schema-prop-{}-{number}-{label}", std::process::id());
        direct
            .create_bucket()
            .bucket(&bucket)
            .send()
            .await
            .expect("create isolated bucket");
        let proxy = Proxy::new(upstream);
        let data = Client::from_conf(
            bounded(aws_sdk_s3::config::Builder::from(&shared))
                .endpoint_url(proxy.endpoint())
                .force_path_style(true)
                .build(),
        );
        Self {
            direct,
            bucket,
            data,
            ddb: aws_sdk_dynamodb::Client::new(&shared),
            proxy: Some(proxy),
            owner: None,
        }
    }

    pub async fn aws() -> Self {
        assert_eq!(std::env::var("GITHUB_ACTIONS").as_deref(), Ok("true"));
        assert_eq!(
            std::env::var("GITHUB_REPOSITORY").as_deref(),
            Ok("EdgeVector/lastdb")
        );
        assert_eq!(
            std::env::var("GITHUB_REF").as_deref(),
            Ok("refs/heads/main")
        );
        assert_eq!(
            std::env::var("SCHEMA_S3_TEST_MODE").as_deref(),
            Ok("isolated-aws-bucket")
        );
        assert!(
            std::env::var_os("SCHEMA_S3_TEST_ENDPOINT").is_none(),
            "AWS checks use the normal AWS endpoint"
        );
        for variable in ["AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"] {
            assert!(
                std::env::var_os(variable).is_none(),
                "AWS checks reject endpoint overrides"
            );
        }
        let bucket = std::env::var("SCHEMA_S3_TEST_BUCKET").expect("fresh dedicated bucket");
        let run_id = std::env::var("GITHUB_RUN_ID").expect("owning CI run");
        assert!(run_id.bytes().all(|byte| byte.is_ascii_digit()) && !run_id.is_empty());
        let run_attempt = std::env::var("GITHUB_RUN_ATTEMPT").expect("owning CI attempt");
        assert!(run_attempt.bytes().all(|byte| byte.is_ascii_digit()) && !run_attempt.is_empty());
        let owner = std::env::var("SCHEMA_S3_TEST_EXPECTED_ACCOUNT").expect("expected AWS account");
        assert_eq!(owner, "152335099025");
        assert_eq!(
            bucket,
            format!("lastdb-schema-ci-{owner}-{run_id}-{run_attempt}")
        );
        let region = std::env::var("AWS_REGION").expect("dedicated bucket region");
        let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region))
            .load()
            .await;
        let direct = Client::from_conf(bounded(aws_sdk_s3::config::Builder::from(&shared)).build());
        let existing = direct
            .list_objects_v2()
            .bucket(&bucket)
            .expected_bucket_owner(&owner)
            .max_keys(1)
            .send()
            .await
            .expect("owned isolated bucket");
        assert!(
            existing.contents().is_empty(),
            "AWS bucket must start empty"
        );
        Self {
            data: direct.clone(),
            direct,
            bucket,
            ddb: aws_sdk_dynamodb::Client::new(&shared),
            proxy: None,
            owner: Some(owner),
        }
    }

    pub fn backend(&self) -> Arc<DefinitionBackend> {
        Arc::new(DefinitionBackend(Arc::new(S3BlobPersistence::new(
            self.data.clone(),
            self.bucket.clone(),
            self.ddb.clone(),
            "unused-embedding-cache".into(),
        ))))
    }

    pub async fn state(&self) -> SchemaServiceState {
        SchemaServiceState::new_with_external(
            self.backend(),
            Arc::new(DisabledEmbeddingModel),
            SchemaConfig::default(),
            process_clock(),
            Arc::new(DedicatedThreadRuntime),
        )
        .await
        .expect("independent service state")
    }

    pub fn faults(&self) -> &Arc<Faults> {
        &self.proxy.as_ref().expect("protocol proxy").faults
    }

    pub async fn cleanup(mut self) {
        for key in ["definitions.json", "direct-cas"] {
            self.direct
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .set_expected_bucket_owner(self.owner.clone())
                .send()
                .await
                .expect("owned object cleanup");
        }
        let faults = self.proxy.as_ref().map(|proxy| proxy.faults.clone());
        drop(self.proxy.take());
        if let Some(faults) = faults {
            faults.assert_clean();
        }
        if self.owner.is_none() {
            self.direct
                .delete_bucket()
                .bucket(&self.bucket)
                .send()
                .await
                .expect("isolated bucket cleanup");
        }
    }
}

pub fn field(description: &str) -> CanonicalField {
    CanonicalField {
        description: description.into(),
        field_type: FieldValueType::String,
        version: 1,
        name_policy_version: 1,
        classification: None,
        interest_category: None,
    }
}

pub fn schema() -> Schema {
    let mut schema = Schema::new(
        "Parcels".into(),
        DeclarativeSchemaType::Hash,
        Some(KeyConfig::new(Some("parcel_identifier".into()), None)),
        Some(vec![
            "parcel_identifier".into(),
            "parcel_delivery_status".into(),
        ]),
        None,
        None,
    );
    schema.descriptive_name = Some("Parcels".into());
    schema.purpose_statement = Some("Parcels tracked by a delivery carrier".into());
    schema.owner_app_id = Some("parcel-app".into());
    for (name, description) in [
        (
            "parcel_identifier",
            "Parcel identifier assigned by the carrier",
        ),
        (
            "parcel_delivery_status",
            "Parcel delivery status code from the carrier",
        ),
    ] {
        schema
            .field_types
            .insert(name.into(), FieldValueType::String);
        schema
            .field_descriptions
            .insert(name.into(), description.into());
    }
    schema
}

fn process_clock() -> Arc<dyn Clock> {
    Arc::new(ProcessClock)
}

struct ProcessClock;

impl Clock for ProcessClock {
    fn wall_time(&self) -> WallTime {
        schema_types::clock::wall_time()
    }

    fn unix_secs(&self) -> u64 {
        schema_types::clock::unix_secs()
    }

    fn monotonic(&self) -> Instant {
        schema_types::clock::monotonic()
    }
}
