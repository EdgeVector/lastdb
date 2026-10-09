#!/usr/bin/env bash
#
# Build the schema_service Lambda zip for deployment via schema-infra.
#
# Output: target/lambda/server_lambda/bootstrap.zip
#
# Requires `cargo-lambda` (install via `brew install cargo-lambda` or
# `cargo install cargo-lambda`).
#
# `--compiler cargo` makes cargo-lambda use the native toolchain rather
# than its zig bundler. Zig cannot link `ort-sys` (fastembed's ONNX
# Runtime dependency), which fold_db pulls in transitively. On Linux CI
# the native compiler is the Lambda runtime target; on macOS, set the
# env var LAMBDA_USE_DOCKER=1 to build inside Amazon Linux 2023 (see
# the schema-infra Docker build for the full pattern).
#
# Usage:
#   ./build.sh                            # release build
#   BUILD_PROFILE=dev-release ./build.sh  # faster iteration profile
set -euo pipefail

PROFILE="${BUILD_PROFILE:-release}"

echo "==> Building schema_service Lambda (profile=${PROFILE}, target=x86_64-unknown-linux-gnu)"

if ! command -v cargo-lambda >/dev/null 2>&1; then
    echo "ERROR: cargo-lambda is not installed." >&2
    echo "  Install: brew install cargo-lambda" >&2
    echo "  Or:      cargo install cargo-lambda" >&2
    exit 1
fi

cargo lambda build \
    --"${PROFILE}" \
    --output-format zip \
    --target x86_64-unknown-linux-gnu \
    --compiler cargo \
    -p schema_service_server_lambda

ZIP_PATH="target/lambda/server_lambda/bootstrap.zip"
if [[ ! -f "${ZIP_PATH}" ]]; then
    echo "ERROR: expected Lambda zip at ${ZIP_PATH} but it was not produced" >&2
    exit 1
fi

SIZE_MB=$(( $(stat -f%z "${ZIP_PATH}" 2>/dev/null || stat -c%s "${ZIP_PATH}") / 1024 / 1024 ))
echo "==> Built ${ZIP_PATH} (${SIZE_MB}MB)"
