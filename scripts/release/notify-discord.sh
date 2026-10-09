#!/usr/bin/env bash
#
# Post a short release/canary status message to Discord.
#
# Soft no-op when DISCORD_WEBHOOK (or DISCORD_WEBHOOK_RELEASE) is unset — so
# local runs and forks without the secret never fail the pipeline.
#
# Usage:
#   scripts/release/notify-discord.sh \
#     --status success|failure|skipped \
#     --title "LastDB Mini canary v0.22.8-canary.20260714" \
#     --body "Built from main@abc1234; prerelease on homebrew-lastdb."
#
# Optional env:
#   DISCORD_WEBHOOK / DISCORD_WEBHOOK_RELEASE — webhook URL
#   DISCORD_USERNAME — bot display name (default: LastDB Release)
#
set -euo pipefail

STATUS=""
TITLE=""
BODY=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --status) STATUS="$2"; shift 2 ;;
    --title) TITLE="$2"; shift 2 ;;
    --body) BODY="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

[ -n "$STATUS" ] || { echo "notify-discord: --status required" >&2; exit 2; }
[ -n "$TITLE" ] || { echo "notify-discord: --title required" >&2; exit 2; }

WEBHOOK="${DISCORD_WEBHOOK_RELEASE:-${DISCORD_WEBHOOK:-}}"
if [ -z "$WEBHOOK" ]; then
  echo "notify-discord: no DISCORD_WEBHOOK(_RELEASE); skipping"
  exit 0
fi

case "$STATUS" in
  success) COLOR=3066993; EMOJI="✅" ;;   # green
  failure) COLOR=15158332; EMOJI="❌" ;;  # red
  skipped) COLOR=9807270; EMOJI="⏭️" ;;  # grey
  *) COLOR=3447003; EMOJI="ℹ️" ;;        # blue
esac

USERNAME="${DISCORD_USERNAME:-LastDB Release}"
# Discord embed description max ~4096; keep payload small.
BODY_TRIMMED="$(printf '%s' "$BODY" | head -c 3500)"

payload="$(jq -nc \
  --arg username "$USERNAME" \
  --arg title "${EMOJI} ${TITLE}" \
  --arg desc "$BODY_TRIMMED" \
  --argjson color "$COLOR" \
  --arg status "$STATUS" \
  '{
    username: $username,
    embeds: [{
      title: $title,
      description: $desc,
      color: $color,
      footer: { text: ("status=" + $status) }
    }]
  }')"

# Never print the webhook URL; only status code.
response_file="$(mktemp "${TMPDIR:-/tmp}/lastdb-discord-notify.XXXXXX")"
trap 'rm -f "$response_file"' EXIT
code="$(curl -sS -o "$response_file" -w '%{http_code}' \
  -H 'Content-Type: application/json' \
  --data "$payload" \
  "$WEBHOOK" || true)"

if [ "$code" != "204" ] && [ "$code" != "200" ]; then
  echo "notify-discord: webhook returned HTTP $code" >&2
  head -c 400 "$response_file" >&2 || true
  echo >&2
  # Soft-fail: never block a green release on Discord outage.
  exit 0
fi
echo "notify-discord: posted ($STATUS)"
