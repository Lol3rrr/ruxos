#!/bin/bash

trap stop SIGINT

function stop {
	echo "Pressed Ctrl-C"
	exit 0
}

lein run test -w lin-kv --bin /ruxos/lin-kv --concurrency 2n --consistency-models linearizable --time-limit 60 --rate 500

lein run serve
