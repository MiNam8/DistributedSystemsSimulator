#!/usr/bin/env bash
#
# Starts a fresh 3-node cluster with the given kvserver flags, runs kvbench
# against it, and appends a CSV row.
#
#   bench/run.sh <label> <csv> [extra kvserver flags...]
#
set -uo pipefail
cd "$(dirname "$0")/.."

LABEL="$1"; shift
CSV="$1"; shift
CLIENTS="${CLIENTS:-64}"
DURATION="${DURATION:-15s}"
OP="${OP:-put}"

DIR=$(mktemp -d /tmp/kvbench-XXXXXX)
PEERS="1=127.0.0.1:17001,2=127.0.0.1:17002,3=127.0.0.1:17003"
ADDRS=127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003

cleanup() { pkill -x kvserver 2>/dev/null; rm -rf "$DIR"; }
trap cleanup EXIT

pkill -x kvserver 2>/dev/null; sleep 0.5
for i in 1 2 3; do
  ./bin/kvserver -id=$i -peers="$PEERS" -data="$DIR/n$i" "$@" > "$DIR/n$i.log" 2>&1 &
done

# Wait for a leader rather than sleeping a fixed amount.
for _ in $(seq 40); do
  if ./bin/kvctl -peers=$ADDRS status 2>/dev/null | grep -q ' leader '; then break; fi
  sleep 0.25
done

./bin/kvbench -peers=$ADDRS -clients="$CLIENTS" -duration="$DURATION" -op="$OP" \
  -label="$LABEL" -csv="$CSV"

# The mean group-commit size is the direct measure of how many fsyncs were saved.
./bin/kvctl -peers=$ADDRS status 2>/dev/null | grep ' leader ' | sed 's/^/    /'
