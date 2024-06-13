#!/bin/bash

PATH_TO_MONICA_BIN_DIR=/home/berg/GitHub/monica/_cmake_debug
PATH_TO_PYTHON=/home/berg/.cache/pypoetry/virtualenvs/klimertrag-2-zmTujJrv-py3.10/bin/python

MONICA_PARAMETERS=$(pwd)/data/params
export MONICA_PARAMETERS
echo "$MONICA_PARAMETERS"

$PATH_TO_MONICA_BIN_DIR/monica-zmq-proxy -pps -f 6666 -b 6677 &
in_proxy_pid=$!
echo "in_proxy_pid -> $in_proxy_pid"
$PATH_TO_MONICA_BIN_DIR/monica-zmq-proxy -pps -f 7788 -b 7777 &
out_proxy_pid=$!
echo "out_proxy_pid -> $out_proxy_pid"

monica_pids=()
for _ in {1..20}
do
  $PATH_TO_MONICA_BIN_DIR/monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788 &
  monica_pids+=($!)
done
echo "monica_pids -> ${monica_pids[*]}"

echo "run producer"
$PATH_TO_PYTHON run_producer.py &
$PATH_TO_PYTHON run_consumer.py
echo "setup  finished -> kill all servers and proxies"

kill "$in_proxy_pid"
echo "killed in_proxy_pid -> $in_proxy_pid"
kill "$out_proxy_pid"
echo "killed out_proxy_pid -> $out_proxy_pid"
for pid in ${monica_pids[*]}
do
  kill "$pid"
  echo "killed monica_pid -> $pid"
done

