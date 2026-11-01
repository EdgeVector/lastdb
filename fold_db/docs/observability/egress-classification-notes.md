# Cloud transport egress

`AuthClient` holds the endpoint and one immutable API key. Its constructor
starts no request and accepts no HTTP client, token mode, or refresh callback.

`BackupTransport::from_auth` creates the bounded HTTPS client for V2 control
requests and signed provider grants. It disables redirects and automatic
decompression. The control request and connection deadline stay at 30 seconds.
Transfer byte limits, stream limits, and provider-grant expiry checks remain.

The control request in `cloud_backup_client/src/backup_transport/http.rs` uses
`observability::propagation::inject_w3c` before the request leaves the client.
The provider upload, download, and feed requests preserve the signed grant
headers. They add no trace header that can invalidate the grant.

The account registration path uses `cloud_backup_client::build_shared_http_client` separately.
It keeps HTTPS, the redirect refusal, and the connection timeout.

The egress source comments use these classes:

- `propagate`: a request to an owned service carries the W3C trace context.
- `loopback`: a local request carries the trace context when it emits a request.
- `skip-s3`: a signed provider request preserves its approved headers.
- `skip-3p`: a third-party request omits unsupported trace headers.

Place the classification comment immediately before the HTTP construction.
Keep propagation inside the common request helper so each control endpoint
uses the same rule.
