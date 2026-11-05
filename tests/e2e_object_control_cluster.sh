#!/usr/bin/env bash
# G3 cluster rehearsal: one SQLite+file-Raft node is backed up and migrated to
# the object control store, two empty object voters join by snapshot, the leader
# is killed and a write is accepted through the new leader, the old leader
# restarts and catches up, and a witness joins.  No object node may open or
# create an oss.db/raft.db file.
#
# Usage: OSS_BIN=/path/to/oss-server tests/e2e_object_control_cluster.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BIN=${OSS_BIN:-target/debug/oss-server}
BIN=$(realpath "$BIN") || { echo "oss-server not found (set OSS_BIN)" >&2; exit 1; }
W=$(mktemp -d)
PIDS=""
cleanup() {
  for p in $PIDS; do kill -9 "$p" 2>/dev/null; done
  wait 2>/dev/null
  if [ "${KEEP:-0}" = 1 ]; then echo "WORK=$W"; else rm -rf "$W"; fi
}
trap cleanup EXIT

PASSES=0 FAILS=0 SKIPS=0
pass() { echo "  PASS: $*"; PASSES=$((PASSES + 1)); }
fail() { echo "  FAIL: $*"; FAILS=$((FAILS + 1)); }
skip() { echo "  SKIP: $*"; SKIPS=$((SKIPS + 1)); }
ck() { if "$@" >/dev/null 2>&1; then pass "$*"; else fail "$*"; fi; }
ck_eq() { if [ "$1" = "$2" ]; then pass "$3"; else fail "$3 (got '$1', want '$2')"; fi; }
finish() {
  echo
  echo "PASS=$PASSES FAIL=$FAILS SKIP=$SKIPS"
  [ "$FAILS" -ne 0 ] && exit 1
  exit 0
}

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
jget() { # file dotted.path (JSON may follow tracing lines on stdout)
  python3 -c 'import json,sys
s=open(sys.argv[1],encoding="utf-8").read()
i=s.find("{")
d=json.loads(s[i:]) if i >= 0 else {}
for k in sys.argv[2].split("."):
    d=d[int(k)] if k.isdigit() else d[k]
print(d)' "$1" "$2"
}
jval() { # dotted.path from stdin
  python3 -c 'import json,sys
d=json.load(sys.stdin)
for k in sys.argv[1].split("."):
    d=d[int(k)] if k.isdigit() else d[k]
print(d)' "$1"
}

ADMIN=ocadmin; APW=ocadmin-pw-123; ALICE=alice; ALPW=alice-pw-123
JWT=dev-only-change-me-0123456789abcdef
RAFT_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
SITE_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(16))')

PA=$(free_port); PB=$(free_port); PC=$(free_port); PW=$(free_port)
[ "$PA" != "$PB" ] && [ "$PB" != "$PC" ] || { echo "self-check: duplicate ports" >&2; exit 1; }

wait_http() { # port pid [path]
  local i
  for i in $(seq 1 240); do
    kill -0 "$2" 2>/dev/null || return 1
    if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$1${3:-/health/live}" 2>/dev/null || true)" = 200 ]; then
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
code() { curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$1" -H "authorization: Bearer $2"; }
raft_json() { curl -s --max-time 3 -H "x-oss-raft-token: $RAFT_TOKEN" "http://127.0.0.1:$1/api/v1/site/raft/status"; }
leader() { raft_json "$1" | jval leader; }
voters() { raft_json "$1" | python3 -c 'import json,sys
try: print(",".join(map(str,json.load(sys.stdin).get("voters",[]))))
except Exception: print("")'; }

no_sqlite_fds() {
  ! ls -l "/proc/$1/fd" 2>/dev/null | grep -Eq '/(oss|raft)\.db( |$)'
}

mkcfg() { # name port node bootstrap backend
  local d="$W/$1" port=$2 node=$3 boot=$4 backend=$5
  mkdir -p "$d/data" "$d/staging"
  cat >"$d/oss.toml" <<EOF
listen = "127.0.0.1:$port"
data_dir = "$d/data"
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "$JWT"
max_put_bytes = 268435456
groups = []
[server]
control_backend = "$backend"
[site]
name = "oc-cluster"
region = "us-east-1"
priority = 10
token = "$SITE_TOKEN"
[raft]
node_id = $node
role = "voter"
bootstrap = $boot
advertise_endpoint = "http://127.0.0.1:$port"
cluster_token = "$RAFT_TOKEN"
db_path = "$d/raft.db"
# Purge each snapshot immediately so a later empty learner must join by snapshot.
purge_batch_size = 1
election_timeout_ms = 1000
heartbeat_interval_ms = 200
rpc_timeout_ms = 1000
write_timeout_ms = 3000
feature_level = 3
EOF
}

start_node() { # name port log
  (cd "$W/$1" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" \
    exec "$BIN" --config "$W/$1/oss.toml" serve >"$W/$3" 2>&1) &
  local p=$!
  PIDS="$PIDS $p"
  if wait_http "$2" "$p"; then STARTED=$p; return 0; fi
  STARTED=$p
  return 1
}
stop_pid() { kill -9 "$1" 2>/dev/null; wait "$1" 2>/dev/null; }

wait_voters() { # port csv
  local i
  for i in $(seq 1 120); do
    [ "$(voters "$1")" = "$2" ] && return 0
    sleep 0.5
  done
  echo "  voters=$(voters "$1") want=$2" >&2
  return 1
}

echo "== 1. SQLite + file-Raft fixture =="
mkcfg a "$PA" 1 true sqlite
if start_node a "$PA" a-sqlite.log; then pass "SQLite fixture started"; else fail "SQLite fixture start"; tail -40 "$W/a-sqlite.log"; finish; fi
T=$(login "$PA" "$ADMIN" "$APW"); ck [ -n "$T" ]
BC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$PA/api/v1/buckets" \
  -H "authorization: Bearer $T" -H 'content-type: application/json' -d '{"name":"oc-bucket"}')
ck_eq "$BC" "204" "SQLite console create bucket"
UC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$PA/api/v1/users" \
  -H "authorization: Bearer $T" -H 'content-type: application/json' -d "{\"username\":\"$ALICE\",\"password\":\"$ALPW\"}")
ck_eq "$UC" "200" "SQLite console create user"
M=$(curl -s --max-time 10 -X POST "http://127.0.0.1:$PA/api/v1/access-keys" \
  -H "authorization: Bearer $T" -H 'content-type: application/json' -d '{}')
AK=$(printf '%s' "$M" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("access_key",""))
except Exception: print("")')
SK=$(printf '%s' "$M" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("secret_key",""))
except Exception: print("")')
ck [ -n "$AK" ]; ck [ -n "$SK" ]
printf 'cluster-object\n' >"$W/one.bin"
if AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url "http://127.0.0.1:$PA" s3 cp "$W/one.bin" s3://oc-bucket/one.bin >/dev/null 2>&1; then
  pass "SQLite S3 put"
else
  fail "SQLite S3 put"
fi
ck [ -f "$W/a/raft.db" ]
ck [ -f "$W/a/oss.db" ]
TA0=$(login "$PA" "$ALICE" "$ALPW"); ck [ -n "$TA0" ]
ABC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$PA/api/v1/buckets" \
  -H "authorization: Bearer $TA0" -H 'content-type: application/json' -d '{"name":"alice-bucket"}')
ck_eq "$ABC" "204" "SQLite user-owned bucket for follower checks"
stop_pid "$STARTED"
# G1 currently rejects an empty runstate whose encoder always emits a level-3
# extension; mark the fixture as level 3 so the migration path can be rehearsed.
sqlite3 "$W/a/oss.db" "INSERT INTO schema_meta(key,value) VALUES('raft_feature_level','3') ON CONFLICT(key) DO UPDATE SET value='3'"
SHA_BEFORE=$(sha256sum "$W/a/oss.db" | cut -d' ' -f1)
sleep 0.2

echo "== 2. offline migrate to object control store (Raft mode) =="
$BIN control-migrate --from "$W/a/oss.db" --state-dir "$W/a/data" --raft-db "$W/a/raft.db" --apply --json >"$W/migrate.json" 2>&1
ck_eq "$?" "0" "control-migrate --apply"
ck_eq "$(jget "$W/migrate.json" status)" "applied" "migration status"
ck_eq "$(jget "$W/migrate.json" mode)" "raft" "migration mode"
ck_eq "$(sha256sum "$W/a/oss.db" | cut -d' ' -f1)" "$SHA_BEFORE" "migration left source byte-identical"
ck [ -f "$W/a/data/.oss.sys/control__XLDIR__state__XLDIR__root/xl.meta" ]
ck [ "$(jget "$W/migrate.json" raft.entries)" -gt 0 ]
# FileLogStore refuses to open while the legacy SQLite raft.db file exists.
for ext in "" -wal -shm; do
  if [ -e "$W/a/raft.db$ext" ]; then mv "$W/a/raft.db$ext" "$W/a/raft.db.legacy$ext"; fi
done
ck [ ! -e "$W/a/raft.db" ]
sed -i 's/control_backend = "sqlite"/control_backend = "objects"/' "$W/a/oss.toml"

echo "== 3. migrated node boots object backend; reads and S3 work =="
if start_node a "$PA" a-object.log; then pass "object node 1 started"; else fail "object node 1 start"; tail -60 "$W/a-object.log"; finish; fi
A_PID=$STARTED
ck no_sqlite_fds "$A_PID"
ck [ ! -e "$W/a/data/oss.db" ]
TA=$(login "$PA" "$ADMIN" "$APW"); ck [ -n "$TA" ]
ck_eq "$(code "http://127.0.0.1:$PA/api/v1/users" "$TA")" "200" "console users read on migrated node"
ck_eq "$(code "http://127.0.0.1:$PA/api/v1/site" "$TA")" "200" "site read on migrated node"
if AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url "http://127.0.0.1:$PA" s3 cp s3://oc-bucket/one.bin "$W/one.back" >/dev/null 2>&1; then
  pass "S3 read on migrated node"
else
  fail "S3 read on migrated node"
fi
ck cmp -s "$W/one.bin" "$W/one.back"
for i in $(seq 1 60); do [ "$(leader "$PA")" = "1" ] && break; sleep 0.5; done
ck_eq "$(leader "$PA")" "1" "node 1 is leader"

echo "== 4. empty object voters join and receive a snapshot =="
mkcfg b "$PB" 2 false objects
mkcfg c "$PC" 3 false objects
if start_node b "$PB" b.log; then pass "empty node 2 started"; else fail "node 2 start"; tail -40 "$W/b.log"; finish; fi
B_PID=$STARTED
if start_node c "$PC" c.log; then pass "empty node 3 started"; else fail "node 3 start"; tail -40 "$W/c.log"; finish; fi
C_PID=$STARTED
ck [ ! -e "$W/b/oss.db" ]; ck [ ! -e "$W/c/oss.db" ]
sleep 1
timeout 90 "$BIN" raft-add-witness --config "$W/a/oss.toml" --node-id 2 --endpoint "http://127.0.0.1:$PB" >"$W/add2.log" 2>&1
ck_eq "$?" "0" "add node 2"
ck wait_voters "$PA" "1,2"
# The first join builds the leader snapshot; wait for its log compaction so the
# next empty node cannot be caught up by replaying logs.
PURGED=""
for i in $(seq 1 60); do
  PURGED=$(raft_json "$PA" | jval purged)
  [ "$PURGED" != "None" ] && break
  sleep 0.5
done
ck [ "$PURGED" != "None" ]
timeout 90 "$BIN" raft-add-witness --config "$W/a/oss.toml" --node-id 3 --endpoint "http://127.0.0.1:$PC" >"$W/add3.log" 2>&1
ck_eq "$?" "0" "add node 3"
ck wait_voters "$PA" "1,2,3"
SNAP=""
for i in $(seq 1 60); do
  SNAP=$(find "$W/c/raft.db.d/snapshot" -type f 2>/dev/null | head -1)
  [ -n "$SNAP" ] && break
  sleep 0.5
done
ck [ -n "$SNAP" ]
TB=$(login "$PB" "$ALICE" "$ALPW"); ck [ -n "$TB" ]
ck_eq "$(code "http://127.0.0.1:$PB/api/v1/buckets" "$TB")" "200" "bucket read on joined node 2"
BCB=$(curl -s --max-time 10 "http://127.0.0.1:$PB/api/v1/buckets" -H "authorization: Bearer $TB")
printf '%s' "$BCB" | grep -q 'alice-bucket' && pass "joined node serves migrated bucket row" || fail "joined node serves migrated bucket row"
ck no_sqlite_fds "$B_PID"
ck [ ! -e "$W/b/raft.db" ]

echo "== 5. leader kill, election, write through new leader =="
OLD=$(leader "$PA")
ck_eq "$OLD" "1" "leader before kill"
stop_pid "$A_PID"
NEW=""
NEWP=""
for i in $(seq 1 120); do
  for p in "$PB" "$PC"; do
    l=$(leader "$p")
    case "$l" in
      2) NEW=2; NEWP=$PB; break ;;
      3) NEW=3; NEWP=$PC; break ;;
    esac
  done
  [ -n "$NEW" ] && break
  sleep 0.5
done
ck [ -n "$NEW" ]
TN=$(login "$NEWP" "$ALICE" "$ALPW"); ck [ -n "$TN" ]
FC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$NEWP/api/v1/buckets" \
  -H "authorization: Bearer $TN" -H 'content-type: application/json' -d '{"name":"failover-bucket"}')
ck_eq "$FC" "204" "bucket write committed through new leader"
ck_eq "$(code "http://127.0.0.1:$NEWP/api/v1/buckets" "$TN")" "200" "bucket read on new leader"
OTHERP=$([ "$NEW" = 2 ] && echo "$PC" || echo "$PB")
TB2=$(login "$OTHERP" "$ALICE" "$ALPW")
BODY=$(curl -s --max-time 10 "http://127.0.0.1:$OTHERP/api/v1/buckets" -H "authorization: Bearer $TB2")
printf '%s' "$BODY" | grep -q 'failover-bucket' && pass "write replicated to other survivor" || fail "write replicated to other survivor"

echo "== 6. killed leader restarts and catches up =="
if start_node a "$PA" a-restart.log; then pass "node 1 restarted"; else fail "node 1 restart"; tail -60 "$W/a-restart.log"; finish; fi
A_PID=$STARTED
TA2=$(login "$PA" "$ALICE" "$ALPW"); ck [ -n "$TA2" ]
CAUGHT=""
for i in $(seq 1 120); do
  BODY=$(curl -s --max-time 3 "http://127.0.0.1:$PA/api/v1/buckets" -H "authorization: Bearer $TA2")
  printf '%s' "$BODY" | grep -q 'failover-bucket' && { CAUGHT=1; break; }
  sleep 0.5
done
ck [ -n "$CAUGHT" ]
ck no_sqlite_fds "$A_PID"

echo "== 7. witness joins =="
mkdir -p "$W/w"
cat >"$W/w/witness.toml" <<EOF
[raft]
node_id = 4
role = "witness"
bootstrap = false
advertise_endpoint = "http://127.0.0.1:$PW"
cluster_token = "$RAFT_TOKEN"
db_path = "$W/w/raft.db"
election_timeout_ms = 1000
heartbeat_interval_ms = 200
rpc_timeout_ms = 1000
write_timeout_ms = 3000
feature_level = 3
EOF
(cd "$W/w" && exec "$BIN" --config "$W/w/witness.toml" witness >"$W/w.log" 2>&1) &
W_PID=$!
PIDS="$PIDS $W_PID"
if wait_http "$PW" "$W_PID" /health; then pass "witness started"; else fail "witness start"; tail -40 "$W/w.log"; fi
timeout 90 "$BIN" raft-add-witness --config "$W/a/oss.toml" --node-id 4 --endpoint "http://127.0.0.1:$PW" >"$W/add4.log" 2>&1
ck_eq "$?" "0" "add witness node 4"
ck wait_voters "$PA" "1,2,3,4"
ck [ -d "$W/w/raft.db.d" ]
ck [ ! -e "$W/w/raft.db" ]
ck no_sqlite_fds "$W_PID"

echo "== 8. no SQLite files opened or created by object nodes =="
ck [ -f "$W/a/oss.db" ]  # source kept for rollback/backup
ck [ ! -e "$W/a/raft.db" ]
for d in b c w; do
  ck [ ! -e "$W/$d/oss.db" ]
  ck [ ! -e "$W/$d/raft.db" ]
done
for p in "$A_PID" "$B_PID" "$C_PID" "$W_PID"; do ck no_sqlite_fds "$p"; done

finish
