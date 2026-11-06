#!/usr/bin/env bash
# H2 final no-database gate (pre-H1 build): every object-backed node must start
# and serve without creating or opening SQLite state.
#
# Default mode runs the runtime gate against the current object backend. The
# pre-H1 tree still needs a SQLite fixture to seed an admin/root (and still
# links sqlx), so fixture seeding and the workspace guards are recorded as
# PENDING-H1 instead of false green. Strict mode
# (STRICT_NO_DB=1 or --require-no-sqlite) promotes every pending item to a
# failure and runs the H1 guards; expect it to fail until H1.
#
# Prereq: cargo build -p oss-server; aws-cli; curl; python3.
# Usage: OSS_BIN=target/debug/oss-server tests/e2e_no_db.sh [--require-no-sqlite]
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE/.."
BIN="${OSS_BIN:-$PWD/target/debug/oss-server}"
STRICT="${STRICT_NO_DB:-0}"
for arg in "$@"; do
  case "$arg" in
    --require-no-sqlite) STRICT=1 ;;
    --help|-h) sed -n '2,15p' "$0"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done
[ -x "$BIN" ] || { echo "missing oss-server binary (cargo build -p oss-server, or set OSS_BIN)"; exit 2; }
BIN=$(realpath "$BIN")
for c in aws curl python3; do command -v "$c" >/dev/null || { echo "need $c"; exit 2; }; done

. "$HERE/lib/no_db_helpers.sh"

ADMIN=ndadmin APW=ndadmin-pw-123
SITE_USER=sitealice SITE_PW=sitealice-pw-123
RAFT_USER=raftalice RAFT_PW=raftalice-pw-123
JWT=dev-only-change-me-0123456789abcdef
RAFT_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
SITE_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
W=$(mktemp -d)
PIDS=""
cleanup() {
  local p
  for p in $PIDS; do kill -9 "$p" 2>/dev/null; done
  wait 2>/dev/null
  if [ "${KEEP:-0}" = 1 ]; then echo "WORK=$W"; else rm -rf "$W"; fi
}
trap cleanup EXIT

aws_at() { # port ak sk aws-args...
  local port=$1 ak=$2 sk=$3
  shift 3
  AWS_ACCESS_KEY_ID="$ak" AWS_SECRET_ACCESS_KEY="$sk" AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "http://127.0.0.1:$port" "$@"
}
body_has() { printf '%s' "$1" | grep -qF -- "$2"; }
bucket_visible() { # port jwt bucket
  curl -s --max-time 10 "http://127.0.0.1:$1/api/v1/buckets" -H "authorization: Bearer $2" |
    grep -q "\"name\":\"$3\""
}
wait_leader() { # port node [tries]
  local i
  for i in $(seq 1 "${3:-120}"); do
    [ "$(leader "$1")" = "$2" ] && return 0
    sleep 0.5
  done
  return 1
}
pair() { # port jwt peer_id endpoint region ak sk peer_token
  curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$1/api/v1/sites" \
    -H "authorization: Bearer $2" -H 'content-type: application/json' \
    -d "{\"peer_id\":\"$3\",\"endpoint\":\"$4\",\"region\":\"$5\",\"priority\":10,\"s3_access_key\":\"$6\",\"s3_secret_key\":\"$7\",\"peer_site_token\":\"$8\"}"
}
console_post() { # port jwt path body
  curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$1$3" \
    -H "authorization: Bearer $2" -H 'content-type: application/json' -d "$4"
}

echo "== environment =="
echo "  binary=$BIN mtime=$(stat -c %y "$BIN" | cut -d. -f1)"
echo "  mode=$([ "$STRICT" = 1 ] && echo strict-no-sqlite || echo default) host=$(uname -sr)"

echo "== 1. fresh empty-root startups: standalone, site, Raft =="
P0=$(free_port)
mkcfg fresh "$P0" standalone
if start_node fresh "$P0" fresh.log; then pass "fresh standalone started"; else fail "fresh standalone start"; tail -30 "$W/fresh.log"; finish; fi
S=$STARTED
ck_node_no_db fresh-standalone "$S" "$W/fresh"
ck [ ! -e "$W/fresh/no-sqlite" ]
stop_pid "$S"
P0=$(free_port)
mkcfg freshsite "$P0" site
if start_node freshsite "$P0" freshsite.log; then pass "fresh site-enabled node started"; else fail "fresh site node start"; tail -30 "$W/freshsite.log"; finish; fi
S=$STARTED
ck_node_no_db fresh-site "$S" "$W/freshsite"
ck [ ! -e "$W/freshsite/no-sqlite" ]
stop_pid "$S"
P0=$(free_port)
mkcfg freshraft "$P0" raft 1 true
if start_node freshraft "$P0" freshraft.log; then pass "fresh Raft node started"; else fail "fresh Raft start"; tail -30 "$W/freshraft.log"; finish; fi
S=$STARTED
ck_node_no_db fresh-raft "$S" "$W/freshraft"
ck [ ! -e "$W/freshraft/no-sqlite" ]
ck wait_leader "$P0" 1
stop_pid "$S"

# Fixture seeding is pre-H1 only. H1 replaces it with the archived seed DB.
echo "== 2. standalone: SQLite seed -> object backend, S3/console smoke, kill -9 =="
P1=$(free_port)
mkcfg standalone "$P1" standalone "" "" sqlite fixture.toml
mkcfg standalone "$P1" standalone
if start_node standalone "$P1" fixture.log fixture.toml; then pass "SQLite seed fixture started"; else fail "SQLite seed fixture start"; tail -30 "$W/fixture.log"; finish; fi
S=$STARTED
T=$(login "$P1" "$ADMIN" "$APW")
ck [ -n "$T" ]
ck_eq "$(console_post "$P1" "$T" /api/v1/buckets '{"name":"nd-bucket"}')" "204" "seed console bucket"
ck_eq "$(console_post "$P1" "$T" /api/v1/users "{\"username\":\"bob\",\"password\":\"bob-pw-1234\"}")" "200" "seed console user"
KEYS=$(mint "$P1" "$T")
AK=$(printf '%s' "$KEYS" | jval access_key)
SK=$(printf '%s' "$KEYS" | jval secret_key)
ck [ -n "$AK" ]
ck [ -n "$SK" ]
printf 'no-db-object\n' >"$W/one.bin"
ck aws_at "$P1" "$AK" "$SK" s3 cp "$W/one.bin" s3://nd-bucket/one.bin
stop_pid "$S"
ck migrate_fixture standalone
stash_sqlite standalone
ck [ ! -e "$W/standalone/oss.db" ]
if start_node standalone "$P1" standalone.log; then pass "object standalone started from migrated root"; else fail "object standalone start"; tail -40 "$W/standalone.log"; finish; fi
S=$STARTED
ck_node_no_db object-standalone "$S" "$W/standalone"
ck [ ! -e "$W/standalone/no-sqlite" ]
T=$(login "$P1" "$ADMIN" "$APW")
ck [ -n "$T" ]
ck_eq "$(code "http://127.0.0.1:$P1/api/v1/users")" "401" "console rejects unauthenticated users list"
BODY=$(curl -s --max-time 10 "http://127.0.0.1:$P1/api/v1/users" -H "authorization: Bearer $T")
body_has "$BODY" '"username":"bob"' && pass "console users list shows seeded bob" || fail "console users list shows seeded bob"
BODY=$(curl -s --max-time 10 "http://127.0.0.1:$P1/api/v1/buckets" -H "authorization: Bearer $T")
body_has "$BODY" '"name":"nd-bucket"' && pass "console buckets list shows seeded bucket" || fail "console buckets list shows seeded bucket"
ck_eq "$(console_post "$P1" "$T" /api/v1/buckets '{"name":"nd-bucket-2"}')" "204" "object console create bucket"
ck_eq "$(console_post "$P1" "$T" /api/v1/users '{"username":"carol","password":"carol-pw-123"}')" "200" "object console create user"
if printf '%s' "$(mint "$P1" "$T")" | grep -q '"access_key"'; then
  pass "object console create access key"
else
  pending "object console create access key (pre-H1 db.rs raw SQLite write; no access_keys table in object root)"
fi
if aws_at "$P1" bad-key bad-secret s3 ls >/dev/null 2>&1; then fail "S3 rejects bad credentials"; else pass "S3 rejects bad credentials"; fi
ck aws_at "$P1" "$AK" "$SK" s3 cp s3://nd-bucket/one.bin "$W/one.back"
ck cmp -s "$W/one.bin" "$W/one.back"
ck aws_at "$P1" "$AK" "$SK" s3api head-object --bucket nd-bucket --key one.bin
ck aws_at "$P1" "$AK" "$SK" s3 ls s3://nd-bucket
ck aws_at "$P1" "$AK" "$SK" s3 cp "$W/one.bin" s3://nd-bucket-2/two.bin
ck_node_no_db object-standalone-after-smoke "$S" "$W/standalone"
stop_pid "$S"
if start_node standalone "$P1" standalone-restart.log; then pass "object standalone restarted after kill -9"; else fail "object standalone restart"; tail -40 "$W/standalone-restart.log"; finish; fi
S=$STARTED
ck_node_no_db object-standalone-restart "$S" "$W/standalone"
T=$(login "$P1" "$ADMIN" "$APW")
ck [ -n "$T" ]
ck wait_until 60 0.25 bucket_visible "$P1" "$T" nd-bucket
ck aws_at "$P1" "$AK" "$SK" s3 cp s3://nd-bucket/one.bin "$W/one.back2"
ck cmp -s "$W/one.bin" "$W/one.back2"
stop_pid "$S"

echo "== 3. site-enabled pair: pair/apply/ship =="
read -r PA PB <<<"$(alloc_ports 2)"
for side in sitea siteb; do
  port=$PA; [ "$side" = siteb ] && port=$PB
  mkcfg "$side" "$port" site "" "" sqlite fixture.toml
  mkcfg "$side" "$port" site
  if start_node "$side" "$port" "$side-fixture.log" fixture.toml; then
    pass "$side SQLite seed started"
  else
    fail "$side SQLite seed start"; tail -30 "$W/$side-fixture.log"; finish
  fi
  SS=$STARTED
  ST=$(login "$port" "$ADMIN" "$APW")
  ck [ -n "$ST" ]
  # Pre-H1: access-key creation is still a raw SQLite write, so seed the pair
  # keys in the fixture and carry them through migration (S3 reads work).
  SEED_KEYS=$(mint "$port" "$ST")
  if [ "$side" = sitea ]; then
    A_AK=$(printf '%s' "$SEED_KEYS" | jval access_key); A_SK=$(printf '%s' "$SEED_KEYS" | jval secret_key)
  else
    B_AK=$(printf '%s' "$SEED_KEYS" | jval access_key); B_SK=$(printf '%s' "$SEED_KEYS" | jval secret_key)
  fi
  stop_pid "$SS"
  ck migrate_fixture "$side"
  stash_sqlite "$side"
  ck [ ! -e "$W/$side/oss.db" ]
  if start_node "$side" "$port" "$side.log"; then pass "$side object site node started"; else fail "$side object start"; tail -40 "$W/$side.log"; finish; fi
  if [ "$side" = sitea ]; then SA=$STARTED; else SB=$STARTED; fi
done
ck_node_no_db site-A "$SA" "$W/sitea"
ck_node_no_db site-B "$SB" "$W/siteb"
AT=$(login "$PA" "$ADMIN" "$APW")
BT=$(login "$PB" "$ADMIN" "$APW")
ck [ -n "$AT" ]
ck [ -n "$BT" ]
A_ID=$(site_field "$PA" "$AT" id)
B_ID=$(site_field "$PB" "$BT" id)
A_TOK=$(site_field "$PA" "$AT" site_token)
B_TOK=$(site_field "$PB" "$BT" site_token)
ck [ -n "$A_ID" ]; ck [ -n "$B_ID" ]; ck [ -n "$A_TOK" ]; ck [ -n "$B_TOK" ]
ck [ -n "$A_AK" ]; ck [ -n "$B_AK" ]
ck_eq "$(pair "$PA" "$AT" "$B_ID" "http://127.0.0.1:$PB" us-east-1 "$B_AK" "$B_SK" "$B_TOK")" "200" "site A pairs B"
ck_eq "$(pair "$PB" "$BT" "$A_ID" "http://127.0.0.1:$PA" us-east-1 "$A_AK" "$A_SK" "$A_TOK")" "200" "site B pairs A"
ck_eq "$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://127.0.0.1:$PA/api/v1/sites/$B_ID/ping" \
  -H "authorization: Bearer $AT")" "200" "site ping"
ck_eq "$(console_post "$PA" "$AT" /api/v1/buckets '{"name":"site-bucket"}')" "204" "site console create bucket"
ck_eq "$(console_post "$PA" "$AT" /api/v1/users "{\"username\":\"$SITE_USER\",\"password\":\"$SITE_PW\"}")" "200" "site console create user"
# Pre-H1 gap: object domain writes do not call the site hooks yet, so a write
# on A is not enqueued for B. Bounded probe; default mode records it.
if wait_until 20 0.5 bucket_visible "$PB" "$BT" site-bucket; then
  pass "site control replication (bucket visible on peer)"
else
  pending "object site control replication (pre-H1: site hooks not wired into object bucket/user writes)"
fi
# Seed the bucket on B too, so data-plane ship is exercised independently of
# control replication. Versioning is required for site data enqueue.
ck_eq "$(console_post "$PB" "$BT" /api/v1/buckets '{"name":"site-bucket"}')" "204" "site peer seeds ship bucket"
# Data-plane enqueue for a non-version-delete PUT requires bucket versioning.
if aws_at "$PA" "$A_AK" "$A_SK" s3api put-bucket-versioning --bucket site-bucket --versioning-configuration Status=Enabled; then
  pass "site bucket versioning enabled"
  ck aws_at "$PB" "$B_AK" "$B_SK" s3api put-bucket-versioning --bucket site-bucket --versioning-configuration Status=Enabled
  printf 'site-ship\n' >"$W/site.bin"
  ck aws_at "$PA" "$A_AK" "$A_SK" s3 cp "$W/site.bin" s3://site-bucket/k.bin
  if wait_until 60 0.5 aws_at "$PB" "$B_AK" "$B_SK" s3 cp s3://site-bucket/k.bin "$W/site.back"; then
    ck cmp -s "$W/site.bin" "$W/site.back"
    pass "object site data ship"
  else
    pending "object site data ship (pre-H1: no row enqueued/replicated on object backend)"
  fi
else
  pending "object S3 bucket versioning write (pre-H1: xl-s3 raw SQL; no buckets table)"
  pending "object site data ship (requires object S3 versioning for data enqueue)"
fi
ck_node_no_db site-A-after-ship "$SA" "$W/sitea"
ck_node_no_db site-B-after-ship "$SB" "$W/siteb"
stop_pid "$SA"
stop_pid "$SB"

echo "== 4. 3-node object Raft + witness, snapshot transfer, kill -9 =="
read -r R1 R2 R3 RW <<<"$(alloc_ports 4)"
mkcfg ra "$R1" raft 1 true sqlite fixture.toml
mkcfg ra "$R1" raft 1 true
if start_node ra "$R1" fixture.log fixture.toml; then pass "Raft SQLite seed started"; else fail "Raft SQLite seed start"; tail -40 "$W/fixture.log"; finish; fi
RA=$STARTED
TA=$(login "$R1" "$ADMIN" "$APW")
ck [ -n "$TA" ]
ck_eq "$(console_post "$R1" "$TA" /api/v1/buckets '{"name":"raft-bucket"}')" "204" "Raft seed console bucket"
ck_eq "$(console_post "$R1" "$TA" /api/v1/users "{\"username\":\"$RAFT_USER\",\"password\":\"$RAFT_PW\"}")" "200" "Raft seed console user"
TL=$(login "$R1" "$RAFT_USER" "$RAFT_PW")
ck [ -n "$TL" ]
ck_eq "$(console_post "$R1" "$TL" /api/v1/buckets '{"name":"raft-user-bucket"}')" "204" "Raft seed user bucket"
stop_pid "$RA"
ck set_fixture_feature_level "$W/ra/oss.db" 3
ck migrate_fixture ra
stash_sqlite ra
ck [ ! -e "$W/ra/oss.db" ]
if start_node ra "$R1" ra.log; then pass "object Raft node 1 started"; else fail "object Raft node 1 start"; tail -40 "$W/ra.log"; finish; fi
RA=$STARTED
ck_node_no_db raft-node1 "$RA" "$W/ra"
ck wait_leader "$R1" 1
TA=$(login "$R1" "$ADMIN" "$APW")
ck [ -n "$TA" ]
ck wait_until 60 0.25 bucket_visible "$R1" "$TA" raft-bucket
# A live write after migration advances the applied LogId past the migrated
# T0-N0 entry; without it the first snapshot build trips openraft's validity
# check (see the gate record's known pre-H1 issue).
ck_eq "$(console_post "$R1" "$TA" /api/v1/buckets '{"name":"raft-bucket-2"}')" "204" "Raft console create second bucket"
ck wait_until 60 0.25 bucket_visible "$R1" "$TA" raft-bucket-2
TL=$(login "$R1" "$RAFT_USER" "$RAFT_PW")
ck [ -n "$TL" ]
ck wait_until 60 0.25 bucket_visible "$R1" "$TL" raft-user-bucket
mkcfg rb "$R2" raft 2 false
mkcfg rc "$R3" raft 3 false
if start_node rb "$R2" rb.log; then pass "object Raft node 2 started"; else fail "object Raft node 2 start"; tail -40 "$W/rb.log"; finish; fi
RB=$STARTED
if start_node rc "$R3" rc.log; then pass "object Raft node 3 started"; else fail "object Raft node 3 start"; tail -40 "$W/rc.log"; finish; fi
RC=$STARTED
ck_node_no_db raft-node2 "$RB" "$W/rb"
ck_node_no_db raft-node3 "$RC" "$W/rc"
timeout 90 "$BIN" raft-add-witness --config "$W/ra/oss.toml" --node-id 2 --endpoint "http://127.0.0.1:$R2" >"$W/add2.log" 2>&1
ck_eq "$?" "0" "add Raft node 2"
ck wait_voters "$R1" "1,2"
PURGED=""
for i in $(seq 1 120); do
  PURGED=$(raft_json "$R1" | jval purged)
  [ -n "$PURGED" ] && [ "$PURGED" != "None" ] && break
  sleep 0.5
done
ck [ -n "$PURGED" ] && [ "$PURGED" != "None" ]
timeout 90 "$BIN" raft-add-witness --config "$W/ra/oss.toml" --node-id 3 --endpoint "http://127.0.0.1:$R3" >"$W/add3.log" 2>&1
ck_eq "$?" "0" "add Raft node 3"
ck wait_voters "$R1" "1,2,3"
SNAP=""
for i in $(seq 1 120); do
  SNAP=$(find "$W/rc/raft.db.d/snapshot" -type f 2>/dev/null | head -1)
  [ -n "$SNAP" ] && break
  sleep 0.5
done
ck [ -n "$SNAP" ]
if [ -n "$(login "$R3" "$ADMIN" "$APW")" ]; then
  pass "admin login on snapshot-joined node"
else
  pending "admin login on snapshot-joined node (pre-H1: object backend skips admin bootstrap)"
fi
TC=$(wait_login "$R3" "$RAFT_USER" "$RAFT_PW")
ck [ -n "$TC" ]
ck wait_until 120 0.5 bucket_visible "$R3" "$TC" raft-user-bucket
ck_node_no_db raft-node3-after-snapshot "$RC" "$W/rc"
ck [ ! -e "$W/ra/raft.db" ]
ck [ ! -e "$W/rb/raft.db" ]
ck [ ! -e "$W/rc/raft.db" ]
stop_pid "$RB"
if start_node rb "$R2" rb-restart.log; then pass "Raft node 2 restarted after kill -9"; else fail "Raft node 2 restart"; tail -40 "$W/rb-restart.log"; finish; fi
RB=$STARTED
ck_node_no_db raft-node2-restart "$RB" "$W/rb"
TB=$(wait_login "$R2" "$RAFT_USER" "$RAFT_PW")
ck [ -n "$TB" ]
ck wait_until 120 0.5 bucket_visible "$R2" "$TB" raft-user-bucket
mkdir -p "$W/w"
cat >"$W/w/witness.toml" <<EOF
[raft]
node_id = 4
role = "witness"
bootstrap = false
advertise_endpoint = "http://127.0.0.1:$RW"
cluster_token = "$RAFT_TOKEN"
db_path = "$W/w/raft.db"
election_timeout_ms = 1000
heartbeat_interval_ms = 200
rpc_timeout_ms = 1000
write_timeout_ms = 3000
feature_level = 3
EOF
if start_witness w "$RW" w.log; then pass "witness started"; else fail "witness start"; tail -40 "$W/w.log"; finish; fi
SW=$STARTED
ck_node_no_db witness "$SW" "$W/w"
timeout 90 "$BIN" raft-add-witness --config "$W/ra/oss.toml" --node-id 4 --endpoint "http://127.0.0.1:$RW" >"$W/add4.log" 2>&1
ck_eq "$?" "0" "add witness node 4"
ck wait_voters "$R1" "1,2,3,4"
ck [ -d "$W/w/raft.db.d" ]
ck [ ! -e "$W/w/raft.db" ]
ck_node_no_db final-raft-node1 "$RA" "$W/ra"
ck_node_no_db final-raft-node2 "$RB" "$W/rb"
ck_node_no_db final-raft-node3 "$RC" "$W/rc"
ck_node_no_db final-witness "$SW" "$W/w"

echo "== 5. H1 workspace guards =="
if [ "$STRICT" = 1 ]; then
  ck scripts/check_no_db.sh
  if cargo tree -i sqlx --locked >/dev/null 2>&1; then
    fail "cargo tree -i sqlx is empty"
  else
    pass "cargo tree -i sqlx is empty"
  fi
else
  pending "scripts/check_no_db.sh clean (pre-H1 tree still has sqlx/sqlite matches)"
  pending "cargo tree -i sqlx empty (pre-H1 tree still links sqlx)"
  pending "object-native admin bootstrap (pre-H1 seeds a SQLite fixture)"
fi

finish
