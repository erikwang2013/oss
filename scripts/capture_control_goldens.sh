#!/usr/bin/env bash
# Rebuild crates/oss-server/tests/goldens from the current SQLite-backed build.
#
# Deterministic by construction: every fixture id/clock/timestamp is a literal;
# only SQLite `strftime('%s','now')` columns are normalized to 0 (see README).
# Runs offline; rerunning on the same commit must produce byte-identical files
# and MANIFEST.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

goldens="crates/oss-server/tests/goldens"
test -f "$goldens/README.md" || {
  echo "missing $goldens/README.md" >&2
  exit 1
}

export CAPTURE_CONTROL_GOLDENS=1
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS=2
export LC_ALL=C

cargo test --offline --locked -p oss-server --test capture_goldens \
  -- --ignored --exact golden_capture::capture_control_goldens --nocapture

manifest="$(mktemp)"
trap 'rm -f "$manifest"' EXIT

# Sorted relative path -> SHA-256, one fixture per line. MANIFEST excludes itself.
while IFS= read -r file; do
  hash="$(sha256sum "$file" | cut -d' ' -f1)"
  printf '%s  %s\n' "$hash" "${file#"$goldens"/}"
done < <(find "$goldens" -type f ! -name MANIFEST -print | sort) >"$manifest"

mv "$manifest" "$goldens/MANIFEST"
trap - EXIT
echo "wrote $goldens/MANIFEST ($(wc -l <"$goldens/MANIFEST") files)"
