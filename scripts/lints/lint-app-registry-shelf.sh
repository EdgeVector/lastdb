#!/usr/bin/env bash
# lint-app-registry-shelf.sh
#
# Keeps apps/registry.json as a durable local shelf, not a junk drawer for
# dogfood/e2e/test app namespace reservations. Throwaway rows may be kept only
# as explicit inactive tombstones, so a live publish path cannot accidentally
# rediscover them as first-class apps.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REGISTRY="$REPO_ROOT/apps/registry.json"

while [ $# -gt 0 ]; do
  case "$1" in
    --registry)
      REGISTRY="${2:-}"
      if [ -z "$REGISTRY" ]; then
        echo "lint-app-registry-shelf: --registry requires a path" >&2
        exit 2
      fi
      shift 2
      ;;
    -h|--help)
      sed -n '2,15p' "$0"
      exit 0
      ;;
    *)
      echo "lint-app-registry-shelf: unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

ruby -rjson - "$REPO_ROOT" "$REGISTRY" <<'RUBY'
repo_root = ARGV.fetch(0)
registry_path = ARGV.fetch(1)

def fail_with(offenders)
  warn "lint-app-registry-shelf: blocked #{offenders.length} registry issue(s)."
  warn "lint-app-registry-shelf: apps/registry.json must contain durable, pickup-ready app rows."
  offenders.first(40).each { |offender| warn "  #{offender}" }
  warn "  ... and #{offenders.length - 40} more" if offenders.length > 40
  exit 1
end

begin
  registry = JSON.parse(File.read(registry_path))
rescue Errno::ENOENT
  fail_with(["missing registry: #{registry_path}"])
rescue JSON::ParserError => e
  fail_with(["invalid JSON in #{registry_path}: #{e.message}"])
end

offenders = []
apps = registry["apps"]
unless apps.is_a?(Array)
  fail_with(["apps must be an array"])
end

throwaway = /(?:^|[-_])(dogfood|e2e|demo|example|fixture|sandbox|scratch|test|tmp|throwaway)(?:$|[-_])/i
inactive_values = %w[archived deprecated disabled expired revoked yanked]
seen = {}

apps.each_with_index do |app, index|
  prefix = "apps[#{index}]"
  unless app.is_a?(Hash)
    offenders << "#{prefix}: entry must be an object"
    next
  end

  app_id = app["app_id"].to_s.strip
  if app_id.empty?
    offenders << "#{prefix}: app_id must be non-empty"
  elsif seen.key?(app_id)
    offenders << "#{prefix}: duplicate app_id #{app_id.inspect} (first seen at apps[#{seen[app_id]}])"
  else
    seen[app_id] = index
  end

  lifecycle = [app["status"], app["lifecycle"], app["tier"]]
    .compact
    .map { |value| value.to_s.strip.downcase }
  inactive = lifecycle.any? { |value| inactive_values.include?(value) }

  if !inactive && app_id.match?(throwaway)
    offenders << "#{prefix}: active throwaway app_id #{app_id.inspect}; remove it or mark status/lifecycle/tier as yanked/expired/archived"
  end

  %w[display_name path manifest command].each do |field|
    next if app[field].is_a?(String) && !app[field].strip.empty?
    offenders << "#{prefix}: #{field} must be a non-empty string"
  end

  %w[zero_ui persistent_ui].each do |field|
    next if app[field] == true || app[field] == false
    offenders << "#{prefix}: #{field} must be boolean"
  end

  path = app["path"].to_s
  if !path.empty? && path.start_with?("/")
    offenders << "#{prefix}: path must be repo-relative, got #{path.inspect}"
  elsif !path.empty? && !File.directory?(File.join(repo_root, path))
    offenders << "#{prefix}: path does not exist: #{path}"
  end

  manifest = app["manifest"].to_s
  if !manifest.empty? && manifest.start_with?("/")
    offenders << "#{prefix}: manifest must be repo-relative, got #{manifest.inspect}"
  elsif !manifest.empty? && !File.file?(File.join(repo_root, manifest))
    offenders << "#{prefix}: manifest does not exist: #{manifest}"
  end

  Array(app["owned_outputs"]).each do |schema|
    next if schema.is_a?(String) && schema.include?("/")
    offenders << "#{prefix}: owned_outputs entries must be namespaced schema strings, got #{schema.inspect}"
  end
end

fail_with(offenders) unless offenders.empty?
RUBY
