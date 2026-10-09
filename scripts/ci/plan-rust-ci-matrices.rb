#!/usr/bin/env ruby
# frozen_string_literal: true

# Emits the rust-test / rust-doctest / clippy matrix JSON for the crate
# families whose Cargo reverse-dependency closure is touched by the change
# under test. ci.yml's `fold_paths` job maps changed paths to families with
# directional filters (a family runs when one of ITS crates' directories or a
# directory it depends on changes; workspace-level files force every family),
# then this script assembles the matrices as the union of the enabled
# families' shards.
#
# Mini-only cutover (2026-07-12): the fold_db_node archive/integration
# topology was deleted with the crate (phase 2). The top-of-graph family is
# now `mini` (lastdb_node + lastdb_host + lastdb_uds + lastdb_identity). See
# card fold-github-ci-mini-redesign and branch archive/desktop-dmg-pre-removal.
#
# Shard topology invariants (mirrored by plan-rust-ci-matrices-test.sh):
# - The families partition the workspace with NO gaps and NO overlap; the
#   always-on `rest` shard is the dynamic complement, so a newly-added crate
#   is covered even before it is assigned to a family.
# - mini/fold_db/schema/support each contribute a small fixed set of shards
#   (no fold_db_node hash partitions or nextest-archive replay).
#
# Usage:
#   FAM_MINI=true FAM_FOLD_DB=false FAM_SCHEMA=false FAM_SUPPORT=false \
#     ruby scripts/ci/plan-rust-ci-matrices.rb [--github-output PATH]
#   ruby scripts/ci/plan-rust-ci-matrices.rb --all   # force every family

require "json"

# Self-hosted Linux migration (card ci-self-hosted-runners-migrate-fold-linux):
# planned Rust package-family jobs run on the org's self-hosted Linux box.
# ONE-LINE REVERT: set RUST_FAMILY_RUNS_ON back to GITHUB_RUNS_ON.
GITHUB_RUNS_ON = "ubuntu-latest"
SELF_HOSTED_LINUX_RUNS_ON = "self-hosted-linux-x64"
RUST_FAMILY_RUNS_ON = SELF_HOSTED_LINUX_RUNS_ON

FOLD_DB_ARGS = "-p fold_db -p observability"
MINI_ARGS =
  "-p lastdb_node -p lastdb_host -p lastdb_uds -p lastdb_identity"
SCHEMA_CORE_ARGS =
  "-p schema_service_core -p schema_service_client " \
  "-p schema_service_server_shared"
SCHEMA_RUNTIME_ARGS =
  "-p schema_service_server_http -p schema_service_server_lambda " \
  "-p schema_service_worker -p schema_service_s3"
SUPPORT_ARGS =
  "-p app_identity_crypto -p folddb_profile " \
  "-p exemem_common -p generate_schema_org_classifications " \
  "-p generate_validated_schema_org_seeds"

def matrices(fam)
  test = []
  test << { shard: "mini", args: MINI_ARGS } if fam[:mini]
  test << { shard: "fold_db", args: FOLD_DB_ARGS } if fam[:fold_db]
  if fam[:schema]
    test << { shard: "schema_service_core", args: SCHEMA_CORE_ARGS }
    test << { shard: "schema_service_runtime", args: SCHEMA_RUNTIME_ARGS }
  end
  test << { shard: "support_crates", args: SUPPORT_ARGS } if fam[:support]
  # Always-on complement: newly-added workspace crates fall through here until
  # they are assigned to a stable package-family shard.
  test << { shard: "rest", args: "dynamic-rest-complement" }

  doctest = []
  doctest << { shard: "mini", args: MINI_ARGS } if fam[:mini]
  doctest << { shard: "fold_db", args: FOLD_DB_ARGS } if fam[:fold_db]
  if fam[:schema]
    doctest << { shard: "schema_service_core", args: SCHEMA_CORE_ARGS }
    doctest << { shard: "schema_service_runtime", args: SCHEMA_RUNTIME_ARGS }
  end
  doctest << { shard: "support_crates", args: SUPPORT_ARGS } if fam[:support]
  doctest << { shard: "rest", args: "dynamic-rest-complement" }

  clippy = [{ shard: "fmt", args: "fmt" }]
  clippy << { shard: "mini", args: MINI_ARGS } if fam[:mini]
  clippy << { shard: "fold_db", args: FOLD_DB_ARGS } if fam[:fold_db]
  if fam[:schema]
    clippy << { shard: "schema_service_core", args: SCHEMA_CORE_ARGS }
    clippy << { shard: "schema_service_runtime", args: SCHEMA_RUNTIME_ARGS }
  end
  clippy << { shard: "support_crates", args: SUPPORT_ARGS } if fam[:support]
  clippy << { shard: "rest", args: "dynamic-rest-complement" }

  # Every planned Rust family shard carries an explicit runner so ci.yml can use
  # `runs-on: ${{ matrix.runs_on }}` with no fallback expression.
  [test, doctest, clippy].each do |matrix|
    matrix.each { |s| s[:runs_on] ||= RUST_FAMILY_RUNS_ON }
  end

  { test_matrix: test, doctest_matrix: doctest, clippy_matrix: clippy }
end

def env_true?(name)
  ENV.fetch(name, "false").strip == "true"
end

if __FILE__ == $PROGRAM_NAME
  all = ARGV.include?("--all")
  fam = {
    mini: all || env_true?("FAM_MINI"),
    fold_db: all || env_true?("FAM_FOLD_DB"),
    schema: all || env_true?("FAM_SCHEMA"),
    support: all || env_true?("FAM_SUPPORT"),
  }
  out = matrices(fam)

  github_output = nil
  if (i = ARGV.index("--github-output"))
    github_output = ARGV.fetch(i + 1)
  end

  out.each do |key, matrix|
    line = "#{key}=#{JSON.generate(matrix)}"
    if github_output
      File.open(github_output, "a") { |f| f.puts(line) }
    end
    puts line
  end
  warn "plan-rust-ci-matrices: families=#{fam.select { |_, v| v }.keys.join(',')} " \
       "test=#{out[:test_matrix].length} doctest=#{out[:doctest_matrix].length} " \
       "clippy=#{out[:clippy_matrix].length} shards"
end
