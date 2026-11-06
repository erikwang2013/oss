#!/usr/bin/env bash
# H1/H2 no-database guard: reject SQLite reintroduction in the final workspace.
#
# Grep-only on purpose (no cargo/network): the H1 gate also checks
# `cargo tree -i sqlx` is empty, but this guard must stay cheap in CI.
#
# Exit 0 = clean, 1 = forbidden matches. Until H1 removes SQLite the current
# tree fails by design; .github/workflows/ci.yml runs this job with
# continue-on-error and a TODO(H1) to make it blocking.
#
# Exclusions: docs/, the archived pre-removal migrator (ARCHIVE_DIR, default
# archive/pre-h1), node_modules, and the no-db gate's own files (their text
# necessarily names what they forbid).
#
# Usage: scripts/check_no_db.sh
set -uo pipefail
cd "$(dirname "$0")/.."
ARCHIVE_DIR=${ARCHIVE_DIR:-archive/pre-h1}
MAX_PER_CHECK=${MAX_MATCHES:-8}

mapfile -t FILES < <(
  find Cargo.toml Cargo.lock crates web scripts tests .github/workflows \
    -type f \
    ! -path '*/node_modules/*' \
    ! -path "*/${ARCHIVE_DIR}/*" \
    ! -path '*/scripts/check_no_db.sh' \
    ! -path '*/tests/e2e_no_db.sh' \
    ! -path '*/tests/lib/no_db_helpers.sh' \
    2>/dev/null | sort
)

TOTAL=0
report() { # name hits
  local name=$1 hits=$2 count=0 shown=0 line
  if [ -n "$hits" ]; then
    count=$(printf '%s\n' "$hits" | grep -c . || true)
  fi
  if [ "$count" -eq 0 ]; then
    echo "  OK   $name"
    return 0
  fi
  TOTAL=$((TOTAL + count))
  echo "  FAIL $name: $count match(es)"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    printf '         %s\n' "$line"
    shown=$((shown + 1))
    if [ "$shown" -ge "$MAX_PER_CHECK" ]; then
      [ "$count" -gt "$shown" ] && echo "         ... $((count - shown)) more"
      break
    fi
  done <<<"$hits"
}

scan() { # name extended-regex
  report "$1" "$(grep -nIE -- "$2" "${FILES[@]}" 2>/dev/null || true)"
}

scan_i() { # name case-insensitive extended-regex
  report "$1" "$(grep -niE -- "$2" "${FILES[@]}" 2>/dev/null || true)"
}

MANIFESTS=()
for f in "${FILES[@]}"; do
  case "$f" in Cargo.toml|Cargo.lock|*/Cargo.toml) MANIFESTS+=("$f") ;; esac
done

echo "== no-db guard (H1): $(date -u +%Y-%m-%dT%H:%M:%SZ) =="
echo "  files=${#FILES[@]} manifests=${#MANIFESTS[@]} archive=${ARCHIVE_DIR}"

# A bare `state.db` occurs as a Rust field access; require a quoted/path-like
# context so the guard reports database filenames, not `state.db.get_user`.
DB_NAMES='["'"'"'/(](oss|raft|state)\.db([^.A-Za-z0-9_]|$)'
report "dependency sqlx/libsqlite3-sys/rusqlite" \
  "$(grep -nIE '\b(sqlx|libsqlite3-sys|rusqlite)\b' "${MANIFESTS[@]}" 2>/dev/null || true)"
scan "sqlx references" '\bsqlx\b'
scan_i "sqlite references" 'sqlite'
scan "SQLite database filenames" "$DB_NAMES"
scan "SqliteControl* symbols" 'SqliteControl[A-Za-z0-9_]*'
scan "migrations/ references" 'migrations/'

if [ -d crates/oss-server/migrations ]; then
  TOTAL=$((TOTAL + 1))
  echo "  FAIL migrations directory: crates/oss-server/migrations/"
else
  echo "  OK   migrations directory: crates/oss-server/migrations/"
fi

echo
if [ "$TOTAL" -eq 0 ]; then
  echo "PASS: no SQLite references in the final workspace"
  exit 0
fi
echo "BLOCKED: $TOTAL SQLite match(es) (expected pre-H1; TODO(H1) make CI blocking)"
exit 1
