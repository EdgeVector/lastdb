#!/usr/bin/env ruby
# frozen_string_literal: true
#
# bump-homebrew-formula.rb — surgically bump version + per-platform url/sha256
# in a EdgeVector/homebrew-lastdb formula (Formula/lastdb.rb or the back-compat
# Formula/folddb.rb), IN PLACE, touching nothing else. release.yml's bump-tap
# job runs it once per formula so both stay in lockstep with every tag.
#
# WHY this exists instead of regenerating the formula from a template:
# release.yml's `bump-tap` job used to `cat > folddb.rb <<RUBY ... RUBY` the
# ENTIRE formula on every tag. That silently clobbered any hand-edit landed on
# the tap — the `service do` block (lost in the v0.5.1 auto-bump, PR #26→#27)
# and the migrate-tilde-data caveat (PR #28) both got wiped. Surgical editing
# preserves everything the release doesn't own (service block, caveats, install
# steps, comments) and kills that whole regression class.
#
# It rewrites EXACTLY three kinds of line: `version`, `url`, and `sha256`.
# The caller (release.yml) re-asserts that invariant with a git-diff guard, and
# scripts/release/bump-homebrew-formula-test.sh proves a service block survives.
#
# The download URLs point at EdgeVector/homebrew-lastdb releases (NOT
# EdgeVector/fold), because fold is private and GitHub 404s anonymous asset
# downloads there — which is what `brew install` uses. We only rewrite the
# `/download/vX.Y.Z/` version segment of each URL; the host/path are left
# untouched on purpose. Don't repoint these at EdgeVector/fold without first
# making that repo public.
#
# Usage:
#   VERSION=0.6.0 \
#   SHA_AARCH64_DARWIN=… SHA_X86_64_DARWIN=… SHA_X86_64_LINUX=… \
#   ruby scripts/release/bump-homebrew-formula.rb path/to/Formula/folddb.rb

def die(msg)
  warn "bump-homebrew-formula: #{msg}"
  exit 1
end

path = ARGV[0] or die("usage: bump-homebrew-formula.rb <formula.rb>")
die("formula not found: #{path}") unless File.exist?(path)

version = ENV["VERSION"].to_s
die("VERSION env is empty") if version.empty?
die("VERSION must not carry a leading 'v' (got #{version.inspect})") if version.start_with?("v")

# Each platform's sha256 is keyed off the platform TRIPLE in the tarball
# filename on the `url` line immediately preceding it — not the full filename.
# The lastdb rebrand (tap PR #54) repointed both Formula/lastdb.rb AND the
# back-compat Formula/folddb.rb at the canonical `lastdb-<triple>.tar.gz`
# tarballs. Older formula revisions may still reference legacy
# `folddb-<triple>.tar.gz` URLs, so this script accepts either prefix and keys
# sha256s on the target triple. Current formulas should continue to reference
# the canonical `lastdb-<triple>.tar.gz` assets.
sha_for_triple = {
  "aarch64-apple-darwin"     => ENV["SHA_AARCH64_DARWIN"].to_s,
  "x86_64-apple-darwin"      => ENV["SHA_X86_64_DARWIN"].to_s,
  "x86_64-unknown-linux-gnu" => ENV["SHA_X86_64_LINUX"].to_s,
}
# A triple's sha is required only if the FORMULA references that triple —
# the release is Apple-Silicon-only (2026-07-05), so the x86 env vars are
# unset; a formula still carrying an x86 url block fails loudly below when
# its sha256 line arms an empty value.

# Read as UTF-8 explicitly. Formulas carry non-ASCII bytes (em dashes, etc. in
# caveats), and matching a regex against a line decoded as US-ASCII raises
# "invalid byte sequence in US-ASCII". Ruby's default external encoding follows
# the locale — UTF-8 on the GitHub runner, but US-ASCII under a bare/sandboxed
# LANG — so pin it here instead of relying on the environment.
lines = File.readlines(path, encoding: "UTF-8")
expected_url_hits = lines.count do |line|
  line.match?(%r{/download/v[^/]+/(?:lastdb|folddb)-[A-Za-z0-9._-]+\.tar\.gz})
end
die("expected at least 1 release asset `url` line, found 0") if expected_url_hits.zero?

version_hits = 0
url_hits = 0
sha_hits = 0
pending_sha = nil # the value the NEXT `sha256` line should carry, armed by its `url` line

lines.map! do |line|
  # version "X.Y.Z" — exactly one in the file.
  if line =~ /^\s*version\s+"/
    version_hits += 1
    next line.sub(/(version\s+")[^"]*(")/) { "#{Regexp.last_match(1)}#{version}#{Regexp.last_match(2)}" }
  end

  # url "…/download/vX.Y.Z/<prefix>-<triple>.tar.gz" — rewrite only the version
  # segment, and arm the matching sha256 for the next `sha256` line. Accept
  # either the canonical `lastdb-` or an older legacy `folddb-` tarball prefix
  # (sha keyed on the triple).
  if (m = line.match(%r{/download/v[^/]+/((?:lastdb|folddb)-([A-Za-z0-9._-]+)\.tar\.gz)}))
    filename = m[1]
    triple = m[2]
    die("url references an unknown platform triple: #{triple}") unless sha_for_triple.key?(triple)
    # Fail loudly when the formula references a triple the release didn't
    # build (Apple-Silicon-only since 2026-07-05): an empty armed sha means
    # a stale formula block, not a bumpable platform.
    die("formula references #{triple} but no sha was provided (release no longer builds it?)") if sha_for_triple[triple].empty?
    url_hits += 1
    pending_sha = sha_for_triple[triple]
    next line.sub(%r{(/download/)v[^/]+(/#{Regexp.escape(filename)})}) { "#{Regexp.last_match(1)}v#{version}#{Regexp.last_match(2)}" }
  end

  # sha256 "…" — rewrite using the value armed by the preceding `url` line.
  if line =~ /^\s*sha256\s+"/
    die("sha256 line with no preceding url line: #{line.strip}") if pending_sha.nil?
    sha_hits += 1
    sha = pending_sha
    pending_sha = nil
    next line.sub(/(sha256\s+")[^"]*(")/) { "#{Regexp.last_match(1)}#{sha}#{Regexp.last_match(2)}" }
  end

  line
end

die("expected exactly 1 `version` line, found #{version_hits}") unless version_hits == 1
die("expected #{expected_url_hits} `url` lines, found #{url_hits}") unless url_hits == expected_url_hits
die("expected #{expected_url_hits} `sha256` lines, found #{sha_hits}") unless sha_hits == expected_url_hits

File.write(path, lines.join, encoding: "UTF-8")
