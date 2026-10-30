#!/usr/bin/env bash
#
# Starts a three-node cluster, writes through gRPC and HTTP, kills the leader
# with SIGKILL, shows the client carrying on, then restarts the dead node from
# its write-ahead log alone.
#
#   make build && ./demo.sh
#
set -uo pipefail
cd "$(dirname "$0")"

# -x matches the executable name exactly, so this can never match the shell
# running it (which is what happened with -f).
pkill -x kvserver 2>/dev/null; sleep 1
rm -rf /tmp/kvdemo && mkdir -p /tmp/kvdemo

PEERS="1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
SHIMS="1=127.0.0.1:8011,2=127.0.0.1:8012,3=127.0.0.1:8013"
ADDRS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003

launch() {
  ./bin/kvserver -id=$1 -peers="$PEERS" -http=127.0.0.1:80$(($1+10)) -http-peers="$SHIMS" \
    -data=/tmp/kvdemo/n$1 > /tmp/kvdemo/n$1.log 2>&1 &
  echo $! > /tmp/kvdemo/n$1.pid
}

for i in 1 2 3; do launch $i; done
sleep 3

echo "=== curl -L against a node that is probably not the leader ==="
curl -sSL -X PUT http://127.0.0.1:8011/kv/lang -d 'Go'; echo
curl -sSL http://127.0.0.1:8012/kv/lang; echo

for k in a b c d e; do ./bin/kvctl -peers=$ADDRS put $k "val-$k" >/dev/null; done
echo
echo "=== cluster status ==="
./bin/kvctl -peers=$ADDRS status
LEADER=$(./bin/kvctl -peers=$ADDRS status | awk '$4=="leader"{print $3}')

echo
echo "=== kill -9 the leader (node $LEADER) ==="
kill -9 "$(cat /tmp/kvdemo/n$LEADER.pid)" 2>/dev/null
sleep 2
echo "write after failover : $(./bin/kvctl -peers=$ADDRS put after-failover yes)"
echo "get c                : $(./bin/kvctl -peers=$ADDRS get c)"
echo "get lang             : $(./bin/kvctl -peers=$ADDRS get lang)"
./bin/kvctl -peers=$ADDRS status

echo
echo "=== restart node $LEADER from its write-ahead log ==="
launch "$LEADER"
sleep 3
./bin/kvctl -peers=$ADDRS status
echo "read after restart   : $(./bin/kvctl -peers=$ADDRS get after-failover)"
echo
echo "=== on-disk WAL of node 1 ==="
ls -la /tmp/kvdemo/n1/
pkill -x kvserver 2>/dev/null
exit 0
