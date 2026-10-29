#!/usr/bin/env ruby
# frozen_string_literal: true

# Select the bounded Rust clippy scope for Fold's required GitHub gate.
#
# Classification fails closed. Documentation-only changes skip Rust clippy.
# A path inside a proven leaf family selects that family. Shared, workflow,
# mixed-family, and unknown paths retain the current Mini clippy command.

require "optparse"
require "shellwords"

CLIPPY_ARGS = {
  # Broad is the exact pre-change required command. Keep this fallback stable.
  "broad" => %w[-p lastdb_node --lib --bins],
  "mini" => %w[-p lastdb_node --lib --bins],
  "schema" => %w[
    -p schema_service_server_http
    -p schema_service_server_lambda
    -p schema_service_s3
    --lib
    --bins
  ],
}.freeze

# These paths sit at the top of their dependency family. A change cannot
# invalidate a different required family through an in-workspace consumer.
LEAF_PREFIXES = {
  "mini" => %w[
    lastdb_node/
    lastdb_host/
    lastdb_uds/
    lastdb_identity/
  ],
  "schema" => %w[
    schema_service/crates/server_http/
    schema_service/crates/server_lambda/
    schema_service/crates/schema_service_s3/
  ],
}.freeze

def docs_path?(path)
  path == "README.md" ||
    path == "SECURITY.md" ||
    path.start_with?("docs/") ||
    path.end_with?(".md")
end

def leaf_family(path)
  LEAF_PREFIXES.each do |family, prefixes|
    return family if prefixes.any? { |prefix| path.start_with?(prefix) }
  end
  nil
end

def plan(paths)
  paths = paths.map(&:strip).reject(&:empty?).uniq
  return { mode: "broad", family: "broad", reason: "no changed paths detected; failing closed" } if paths.empty?
  return { mode: "docs", family: "none", reason: "documentation-only change" } if paths.all? { |path| docs_path?(path) }

  code_paths = paths.reject { |path| docs_path?(path) }
  families = code_paths.map { |path| leaf_family(path) }
  if families.none?(&:nil?) && families.uniq.length == 1
    family = families.first
    return { mode: "family", family: family, reason: "leaf crate family selected by #{code_paths.first}" }
  end

  trigger = code_paths.find { |path| leaf_family(path).nil? } || code_paths.first
  { mode: "broad", family: "broad", reason: "shared, mixed-family, or unmapped path: #{trigger}" }
end

def command_for(family)
  args = CLIPPY_ARGS.fetch(family)
  ["cargo", "clippy", *args, "--", "-D", "warnings"]
end

options = { github_output: ENV["GITHUB_OUTPUT"] }
OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [--github-output PATH] [--run FAMILY] [PATH ...]"
  opts.on("--github-output PATH", "Append the plan to a GitHub Actions output file") do |path|
    options[:github_output] = path
  end
  opts.on("--run FAMILY", CLIPPY_ARGS.keys, "Run the selected clippy family") do |family|
    options[:run] = family
  end
  opts.on("--print-command FAMILY", CLIPPY_ARGS.keys, "Print the selected command") do |family|
    options[:print_command] = family
  end
end.parse!

if options[:run]
  exec(*command_for(options[:run]))
elsif options[:print_command]
  puts Shellwords.join(command_for(options[:print_command]))
else
  paths = ARGV.empty? ? STDIN.read.lines(chomp: true) : ARGV
  result = plan(paths)
  output = {
    "mode" => result.fetch(:mode),
    "family" => result.fetch(:family),
    "run_clippy" => result.fetch(:mode) != "docs",
    "reason" => result.fetch(:reason),
  }
  if options[:github_output] && !options[:github_output].empty?
    File.open(options[:github_output], "a") do |file|
      output.each { |key, value| file.puts "#{key}=#{value}" }
    end
  end
  output.each { |key, value| puts "#{key}=#{value}" }
end
