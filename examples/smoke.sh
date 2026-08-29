#!/bin/sh
set -eu

compose="docker compose -f examples/compose.yml"
pids=""
smoke_key="smoke-$$"

cleanup() {
	for pid in $pids; do
		kill "$pid" 2>/dev/null || true
	done
}
trap cleanup EXIT INT TERM

produce() {
	topic=$1
	record=$2
	printf '%s\n' "$record" | $compose exec -T kafka kafka-console-producer \
		--bootstrap-server localhost:9092 --topic "$topic" \
		--property parse.key=true --property key.separator=:
}

expect_key_value() {
	topic=$1
	key=$2
	expected=$3
	attempt=0
	while [ "$attempt" -lt 30 ]; do
		output=$($compose exec -T kafka kafka-console-consumer \
			--bootstrap-server localhost:9092 --topic "$topic" --from-beginning \
			--property print.key=true --timeout-ms 1000 2>/dev/null || true)
		if printf '%s\n' "$output" | grep -F "\"$key\"" | grep -F "$expected" >/dev/null; then
			return 0
		fi
		attempt=$((attempt + 1))
		sleep 1
	done
	printf 'missing topic=%s key=%s value=%s\n' "$topic" "$key" "$expected" >&2
	return 1
}

expect_record() {
	topic=$1
	attempt=0
	while [ "$attempt" -lt 30 ]; do
		if $compose exec -T kafka kafka-console-consumer \
			--bootstrap-server localhost:9092 --topic "$topic" --from-beginning \
			--max-messages 1 --timeout-ms 1000 >/dev/null 2>&1; then
			return 0
		fi
		attempt=$((attempt + 1))
		sleep 1
	done
	printf 'missing record on topic=%s\n' "$topic" >&2
	return 1
}

# Global table must contain profile before joins app bootstraps it.
produce join-profiles "\"$smoke_key\":\"gold\""

EXAMPLE_STOP_AFTER=24s go run ./examples/filter-map & pids="$pids $!"
EXAMPLE_STOP_AFTER=24s go run ./examples/stateful & pids="$pids $!"
EXAMPLE_STOP_AFTER=24s go run ./examples/joins & pids="$pids $!"
EXAMPLE_RESTART_AFTER=8s EXAMPLE_STOP_AFTER=24s go run ./examples/eos-recovery & pids="$pids $!"

sleep 4
produce filter-map-input "\"$smoke_key\":\"hello\""
produce stateful-input "\"$smoke_key\":\"value\""
produce eos-input "\"$smoke_key\":\"value\""
produce join-table-input "\"$smoke_key\":\"table\""
sleep 2
produce join-stream-input "\"$smoke_key\":\"stream\""
produce join-left-input "\"$smoke_key\":\"left\""
produce join-right-input "\"$smoke_key\":\"right\""
produce join-orders "\"order-$smoke_key\":{\"id\":\"order-$smoke_key\",\"user_id\":\"$smoke_key\"}"

expect_key_value filter-map-output "$smoke_key" '"HELLO"'
expect_key_value stateful-count-output "$smoke_key" '1'
expect_record stateful-example-counts-changelog
expect_key_value eos-output "$smoke_key" '1'
expect_key_value join-table-output "$smoke_key" '"stream:1"'
expect_key_value join-stream-output "$smoke_key" '"left:right"'
expect_key_value join-global-output "order-$smoke_key" "\"order-$smoke_key:gold\""

# Phase 2 uses empty local state. Count 2 proves changelog value 1 restored first.
sleep 4
produce eos-input "\"$smoke_key\":\"value\""
expect_key_value eos-output "$smoke_key" '2'
expect_record eos-recovery-example-counts-changelog

for pid in $pids; do
	wait "$pid"
done
pids=""
