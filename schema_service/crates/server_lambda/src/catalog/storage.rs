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

const KEY: &str = "advisory-catalog-v1/catalog.json";
const MAX_IMAGE_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct Store {
    url: String,
    region: String,
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
            url: format!("https://{bucket}.s3.{region}.amazonaws.com/{KEY}"),
            region,
        })
    }

    pub(super) async fn read(
        &self,
        client: &Client,
        timeout: Duration,
    ) -> Result<Option<(Image, String)>, String> {
        let mut request = signed_request(client, &self.region, &self.url, Method::GET, None)?;
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
    ) -> Result<(), String> {
        let body = serde_json::to_vec(image).map_err(|_| "catalog image could not be encoded")?;
        if body.len() > MAX_IMAGE_BYTES {
            return Err("catalog image exceeds the configured byte limit".into());
        }
        let request = signed_request(
            client,
            &self.region,
            &self.url,
            Method::PUT,
            Some((body, etag)),
        )?;
        let response = client
            .execute(request)
            .await
            .map_err(|_| "catalog storage write failed")?;
        if !response.status().is_success() {
            return Err(format!(
                "catalog storage write returned HTTP {}",
                response.status()
            ));
        }
        Ok(())
    }
}
