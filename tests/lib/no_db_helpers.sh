#!/usr/bin/env bash
# Shared helpers for tests/e2e_no_db.sh (H2 no-database gate).
#
# Sourced by the gate script after it sets BIN, W, PIDS and STRICT. The helper
# implementations are trimmed from tests/e2e_object_control.sh and
# tests/e2e_object_control_cluster.sh so the H2 gate behaves like the G3
# rehearsals it replaces.

PASSES=0 FAILS=0 PENDINGS=0 SKIPS=0
PENDING_LIST=""

pass() { PASSES=$((PASSES + 1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS + 1)); echo "  FAIL: $*"; }
skip() { SKIPS=$((SKIPS + 1)); echo "  SKIP: $*"; }

# A known pre-H1 gap. Default mode records it and stays green; strict mode
# (STRICT_NO_DB=1 / --require-no-sqlite) turns it into a real failure.
pending() {
  if [ "${STRICT:-0}" = 1 ]; then
    fail "STRICT-PENDING-H1: $*"
  else
    PENDINGS=$((PENDINGS + 1))
    PENDING_LIST="${PENDING_LIST}  - $*
"
    echo "  PENDING-H1: $*"
  fi
}

ck() { if "$@" >/dev/null 2>&1; then pass "$*"; else fail "$*"; fi; }
ck_eq() { if [ "$1" = "$2" ]; then pass "$3"; else fail "$3 (got '$1', want '$2')"; fi; }

finish() {
  echo
  echo "PASS=$PASSES FAIL=$FAILS PENDING=$PENDINGS SKIP=$SKIPS"
  if [ -n "$PENDING_LIST" ]; then
    echo "PENDING-H1 (not failures in default mode; enforced by STRICT_NO_DB=1):"
    printf '%s' "$PENDING_LIST"
  fi
  if [ "$FAILS" -ne 0 ]; then
    echo "FAIL: no-database gate has $FAILS failing assertion(s)"
    exit 1
  fi
  echo "PASS: no-database gate"
  exit 0
}

free_port() {
  python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'
}

# N distinct ports in one call: free_port called N times can hand out the same
# port twice (each call closes before the next binds), which made sibling nodes
# collide. All sockets stay bound until the value is printed.
alloc_ports() { # n
  python3 - "$1" <<'PY'
import socket, sys
socks = []
for _ in range(int(sys.argv[1])):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    socks.append(s)
print(" ".join(str(s.getsockname()[1]) for s in socks))
PY
}

wait_http() { # port pid [path] [tries]
  local port=$1 pid=$2 path=${3:-/health/live} tries=${4:-240} i
  for i in $(seq 1 "$tries"); do
    kill -0 "$pid" 2>/dev/null || return 1
    if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$port$path" 2>/dev/null || true)" = 200 ]; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

login() { # port user password
  curl -s --max-time 10 -X POST "http://127.0.0.1:$1/api/v1/login" -H 'content-type: application/json' \
    -d "{\"username\":\"$2\",\"password\":\"$3\"}" |
    python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))'
}

# Joined nodes may still be installing a snapshot when the port is up.
wait_login() { # port user password [tries]
  local i t
  for i in $(seq 1 "${4:-120}"); do
    t=$(login "$1" "$2" "$3")
    if [ -n "$t" ]; then printf '%s' "$t"; return 0; fi
    sleep 0.5
  done
  return 1
}

code() { curl -s -o /dev/null -w '%{http_code}' --max-time 10 ${2:+-H "authorization: Bearer $2"} "$1"; }
jval() { python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
for k in sys.argv[1].split("."):
    d=d[int(k)] if k.isdigit() else d[k]
print("" if d is None else d)' "$1"; }

# Any file descriptor pointing at SQLite state (including deleted WAL/SHM and
# etilqs temp files). No object-backed process may hold one.
no_db_fds() { # pid
  local f t
  for f in /proc/"$1"/fd/*; do
    t=$(readlink "$f" 2>/dev/null) || continue
    if printf '%s\n' "$t" | grep -Eq '(\.db(-wal|-shm|-journal)?( \(deleted\))?$|sqlite|etilqs_)'; then
      echo "  fd $f -> $t" >&2
      return 1
    fi
  done
  return 0
}

# No SQLite database files of any name under a node directory.
no_db_files() { # dir
  local hits
  hits=$(find "$1" \( -name '*.db' -o -name '*.db-wal' -o -name '*.db-shm' -o -name '*.db-journal' \) -print 2>/dev/null)
  if [ -n "$hits" ]; then
    printf '%s\n' "$hits" >&2
    return 1
  fi
  return 0
}

ck_node_no_db() { # label pid dir
  if no_db_fds "$2" && no_db_files "$3"; then
    pass "$1: no SQLite fds/files"
  else
    fail "$1: SQLite fd or file present"
  fi
}

# Config for any node. Object mode (default) points database_url at a directory
# that must never exist: if a pre-H1 path opens SQLite it fails closed instead
# of silently creating oss.db. SQLite mode is only used to seed pre-H1 fixtures.
mkcfg() { # name port [standalone|site|raft] [node_id] [bootstrap] [objects|sqlite] [cfg_file]
  local name=$1 port=$2 kind=${3:-standalone} node=${4:-1} boot=${5:-false}
  local backend=${6:-objects} cfg=${7:-oss.toml}
  local d="$W/$name" dburl
  if [ "$backend" = sqlite ]; then
    dburl="sqlite://$d/oss.db?mode=rwc"
  else
    dburl="sqlite://$d/no-sqlite/oss.db?mode=rwc"
  fi
  mkdir -p "$d/data" "$d/staging"
  cat >"$d/$cfg" <<EOF
listen = "127.0.0.1:$port"
data_dir = "$d/data"
staging_dir = "$d/staging"
database_url = "$dburl"
jwt_secret = "$JWT"
max_put_bytes = 268435456
groups = []
[server]
control_backend = "$backend"
EOF
  if [ "$kind" != "standalone" ]; then
    cat >>"$d/$cfg" <<EOF
[site]
name = "$name"
region = "us-east-1"
priority = 10
token = "$SITE_TOKEN"
EOF
  fi
  if [ "$kind" = "raft" ]; then
    cat >>"$d/$cfg" <<EOF
[raft]
node_id = $node
role = "voter"
bootstrap = $boot
advertise_endpoint = "http://127.0.0.1:$port"
cluster_token = "$RAFT_TOKEN"
db_path = "$d/raft.db"
# Purge each snapshot immediately so a later empty node must join by snapshot.
purge_batch_size = 1
election_timeout_ms = 1000
heartbeat_interval_ms = 200
rpc_timeout_ms = 1000
write_timeout_ms = 3000
feature_level = 3
EOF
  fi
}

start_node() { # name port log [cfg_file] -> pid in STARTED
  (cd "$W/$1" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" \
    exec "$BIN" --config "$W/$1/${4:-oss.toml}" serve >"$W/$3" 2>&1) &
  STARTED=$!
  PIDS="$PIDS $STARTED"
  wait_http "$2" "$STARTED" || return 1
  # A stale listener on the same port can answer for a process that then dies
  # with "address already in use"; never report that node as started.
  sleep 0.3
  kill -0 "$STARTED" 2>/dev/null
}

start_witness() { # dir port log -> pid in STARTED
  (cd "$W/$1" && exec "$BIN" --config "$W/$1/witness.toml" witness >"$W/$3" 2>&1) &
  STARTED=$!
  PIDS="$PIDS $STARTED"
  wait_http "$2" "$STARTED" /health
}

stop_pid() { kill -9 "$1" 2>/dev/null; wait "$1" 2>/dev/null; }

raft_json() { curl -s --max-time 3 -H "x-oss-raft-token: $RAFT_TOKEN" "http://127.0.0.1:$1/api/v1/site/raft/status"; }
leader() { raft_json "$1" | jval leader; }
voters() { raft_json "$1" | python3 -c 'import json,sys
try: print(",".join(map(str,json.load(sys.stdin).get("voters",[]))))
except Exception: print("")'; }

wait_voters() { # port csv [tries]
  local i
  for i in $(seq 1 "${3:-120}"); do
    [ "$(voters "$1")" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  voters=$(voters "$1") want=$2" >&2
  return 1
}

site_field() { # port token field
  curl -s --max-time 10 "http://127.0.0.1:$1/api/v1/site" -H "authorization: Bearer $2" | jval "$3"
}

mint() { # port token
  curl -s --max-time 10 -X POST "http://127.0.0.1:$1/api/v1/access-keys" \
    -H "authorization: Bearer $2" -H 'content-type: application/json' -d '{}'
}

wait_until() { # tries interval command...
  local tries=$1 interval=$2 i
  shift 2
  for i in $(seq 1 "$tries"); do
    "$@" >/dev/null 2>&1 && return 0
    sleep "$interval"
  done
  return 1
}

# --- pre-H1 fixture seeding -------------------------------------------------
# H1 deletes SQLite, so these three helpers and every fixture section in
# tests/e2e_no_db.sh disappear at H1. The replacement is an archived seed DB
# created by the pre-removal binary (see the gate record rollback note).

migrate_fixture() { # name
  local d="$W/$1" rc
  "$BIN" control-migrate --from "$d/oss.db" --state-dir "$d/data" \
    --raft-db "$d/raft.db" --apply --json >"$d/migrate.json" 2>&1
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "  control-migrate failed for $1:" >&2
    tail -20 "$d/migrate.json" >&2
  fi
  return "$rc"
}

# Move fixture SQLite files out of an object node dir so the no-db assertions
# see only object state. Directories such as raft.db.d stay in place.
stash_sqlite() { # node_name
  local d="$W/$1" dest="$W/fixtures/$1"
  mkdir -p "$dest"
  find "$d" -maxdepth 1 -type f \
    \( -name '*.db' -o -name '*.db-wal' -o -name '*.db-shm' -o -name '*.db-journal' \) \
    -exec mv -t "$dest" {} + 2>/dev/null || true
}

set_fixture_feature_level() { # oss.db level
  python3 - "$1" "${2:-3}" <<'PY'
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.execute("INSERT INTO schema_meta(key,value) VALUES('raft_feature_level',?) "
             "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (sys.argv[2],))
conn.commit()
PY
}
