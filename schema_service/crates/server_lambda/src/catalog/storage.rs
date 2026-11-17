//! Private S3 objects for pushed schemas.
//!
//! The catalog record is `schema-catalog-v2/catalog.json`. Each definition is
//! its own immutable object at `schema-catalog-v2/definitions/<definition_id>.json`.
//! The retired `advisory-catalog-v1/catalog.json` feed image is not read or converted.

use std::time::{Duration, SystemTime};

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
    UriPathNormalizationMode,
};
use aws_sigv4::sign::v4;
use http::{header, Method, StatusCode};
use reqwest::{Client, Response};

use super::Image;

const CATALOG_KEY: &str = "schema-catalog-v2/catalog.json";
const DEFINITION_PREFIX: &str = "schema-catalog-v2/definitions";
const MAX_IMAGE_BYTES: usize = 16 * 1024 * 1024;
const MAX_DEFINITION_BYTES: usize = 2 * 1024 * 1024;

pub(super) struct Store {
    endpoint: String,
    region: String,
}

pub(super) enum WriteOutcome {
    Stored,
    Conflict,
}

pub(super) async fn bounded_body(mut response: Response, limit: usize) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        return Err("response exceeds the configured byte limit".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if bytes.len() + chunk.len() > limit {
            return Err("response exceeds the configured byte limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn signed_request(
    client: &Client,
    region: &str,
    url: &str,
    method: Method,
    body: Option<(Vec<u8>, Option<&str>)>,
) -> Result<reqwest::Request, String> {
    let mut request = client
        .request(method, url)
        .build()
        .map_err(|error| error.to_string())?;
    let payload = if let Some((bytes, etag)) = body {
        let (name, value) =
            etag.map_or((header::IF_NONE_MATCH, "*"), |tag| (header::IF_MATCH, tag));
        request
            .headers_mut()
            .insert(name, value.parse().map_err(|_| "invalid catalog ETag")?);
        request.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        bytes
    } else {
        Vec::new()
    };
    let credentials = Credentials::new(
        std::env::var("AWS_ACCESS_KEY_ID").map_err(|_| "Lambda role access key is unavailable")?,
        std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| "Lambda role signing key is unavailable")?,
        Some(std::env::var("AWS_SESSION_TOKEN").map_err(|_| "Lambda role session is unavailable")?),
        None,
        "Lambda execution role",
    )
    .into();
    let mut settings = SigningSettings::default();
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    let params = v4::SigningParams::builder()
        .identity(&credentials)
        .region(region)
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .map_err(|_| "invalid catalog signing parameters")?;
    let headers = request
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str(), value.to_str().unwrap_or_default()));
    let signable = SignableRequest::new(
        request.method().as_str(),
        url,
        headers,
        SignableBody::Bytes(&payload),
    )
    .map_err(|_| "catalog request could not be signed")?;
    let (instructions, _) = sign(signable, &params.into())
        .map_err(|_| "catalog signing failed")?
        .into_parts();
    let (signed_headers, _) = instructions.into_parts();
    for field in signed_headers {
        let name = field
            .name()
            .parse::<header::HeaderName>()
            .map_err(|_| "invalid signed header")?;
        let mut value = field
            .value()
            .parse::<header::HeaderValue>()
            .map_err(|_| "invalid signed header")?;
        value.set_sensitive(field.sensitive());
        request.headers_mut().insert(name, value);
    }
    if !payload.is_empty() {
        *request.body_mut() = Some(payload.into());
    }
    Ok(request)
}

impl Store {
    pub(super) fn new(bucket: &str, region: String) -> Result<Self, String> {
        if bucket.len() > 63
            || bucket.is_empty()
            || !bucket
                .bytes()
                .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'-')
            || region.is_empty()
            || region.len() > 24
            || !region
                .bytes()
                .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'-')
        {
            return Err("catalog S3 endpoint is invalid".into());
        }
        Ok(Self {
            endpoint: format!("https://{bucket}.s3.{region}.amazonaws.com"),
            region,
        })
    }

    fn object_url(&self, key: &str) -> String {
        format!("{}/{key}", self.endpoint)
    }

    pub(super) async fn read(
        &self,
        client: &Client,
        timeout: Duration,
    ) -> Result<Option<(Image, String)>, String> {
        let url = self.object_url(CATALOG_KEY);
        let mut request = signed_request(client, &self.region, &url, Method::GET, None)?;
        *request.timeout_mut() = Some(timeout);
        let response = client
            .execute(request)
            .await
            .map_err(|_| "catalog storage read failed")?;
        match response.status() {
            StatusCode::OK => {
                let etag = response
                    .headers()
                    .get(header::ETAG)
                    .and_then(|value| value.to_str().ok())
                    .ok_or("catalog storage response has no ETag")?
                    .to_owned();
                let body = bounded_body(response, MAX_IMAGE_BYTES).await?;
                let image: Image = serde_json::from_slice(&body)
                    .map_err(|_| "catalog storage holds an invalid image")?;
                image.validate()?;
                Ok(Some((image, etag)))
            }
            // Without ListBucket, S3 uses AccessDenied for a missing object.
            // A writer starts from this state with If-None-Match: *, so it
            // cannot replace an existing image that it did not read.
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => Ok(None),
            status => Err(format!("catalog storage read returned HTTP {status}")),
        }
    }

    pub(super) async fn write(
        &self,
        client: &Client,
        image: &Image,
        etag: Option<&str>,
        timeout: Duration,
    ) -> Result<WriteOutcome, String> {
        let body = serde_json::to_vec(image).map_err(|_| "catalog image could not be encoded")?;
        if body.len() > MAX_IMAGE_BYTES {
            return Err("catalog image exceeds the configured byte limit".into());
        }
        let url = self.object_url(CATALOG_KEY);
        let mut request =
            signed_request(client, &self.region, &url, Method::PUT, Some((body, etag)))?;
        *request.timeout_mut() = Some(timeout);
        let response = client
            .execute(request)
            .await
            .map_err(|_| "catalog storage write failed")?;
        match response.status() {
            status if status.is_success() => Ok(WriteOutcome::Stored),
            StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT => Ok(WriteOutcome::Conflict),
            status => Err(format!("catalog storage write returned HTTP {status}")),
        }
    }

    /// Write one definition object. `If-None-Match: *` makes a lost race,
    /// where the same id already exists, success. The same id is the same bytes.
    pub(super) async fn put_definition(
        &self,
        client: &Client,
        definition_id: &str,
        body: &[u8],
        timeout: Duration,
    ) -> Result<(), String> {
        if !valid_definition_id(definition_id) {
            return Err("invalid definition id".into());
        }
        if body.len() > MAX_DEFINITION_BYTES {
            return Err("definition object exceeds the configured byte limit".into());
        }
        let url = self.object_url(&format!("{DEFINITION_PREFIX}/{definition_id}.json"));
        let mut request = signed_request(
            client,
            &self.region,
            &url,
            Method::PUT,
            Some((body.to_vec(), None)),
        )?;
        *request.timeout_mut() = Some(timeout);
        let response = client
            .execute(request)
            .await
            .map_err(|_| "definition storage write failed")?;
        match response.status() {
            status if status.is_success() => Ok(()),
            // If-None-Match: * lost the race. The object for this id is already stored.
            StatusCode::PRECONDITION_FAILED => Ok(()),
            status => Err(format!("definition storage write returned HTTP {status}")),
        }
    }

    /// Read one definition object. Without ListBucket, S3 uses AccessDenied
    /// for a missing key, so forbidden and not-found are both absence.
    pub(super) async fn get_definition(
        &self,
        client: &Client,
        definition_id: &str,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>, String> {
        if !valid_definition_id(definition_id) {
            return Err("invalid definition id".into());
        }
        let url = self.object_url(&format!("{DEFINITION_PREFIX}/{definition_id}.json"));
        let mut request = signed_request(client, &self.region, &url, Method::GET, None)?;
        *request.timeout_mut() = Some(timeout);
        let response = client
            .execute(request)
            .await
            .map_err(|_| "definition storage read failed")?;
        match response.status() {
            StatusCode::OK => Ok(Some(bounded_body(response, MAX_DEFINITION_BYTES).await?)),
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => Ok(None),
            status => Err(format!("definition storage read returned HTTP {status}")),
        }
    }
}

pub(super) fn valid_definition_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}
