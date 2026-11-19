# App API version 1

The app API is what an installed app calls: `POST /v1/read`, `POST /v1/write`,
and `POST /v1/x/<name>`. The wire contract is `docs/app-api.md` and
`lastdb_node/app-api.json`. This note records where a registry listing is filed.

## Listing version

A registry listing is filed under the app API version. The app API is version 1.

`APP_API_VERSION` and `MIN_SUPPORTED_APP_API_VERSION` live in
`lastdb_node/src/version.rs`. They change only when `/v1/read`, `/v1/write`, or
the extension mechanism changes in a way that breaks an installed app. Adding
an extension does not change them. An owner-side change does not change them.

The listing's signed `api_version` member, the list member `api_version`, and
the list path `<channel>/api/<n>.json` keep those names. The value is the app
API version. `lastdb app publish` sends the running node's `app_api_version`.
A node reads the lists from `min_supported_app_api_version` through
`app_api_version` and merges them. Today that range is the single list
`api/1.json`.

`GET /api/version` reports `app_api_version` and `min_supported_app_api_version`
beside `api_version` and `min_supported_api_version`. Those two existing members
stay the socket version, for the CLI and the SDK.

`"*"` in `access.read` and `access.write` is allowed at every app API version.

Listings already filed under a socket number stay unread by this build until the app is republished under app API version 1.
