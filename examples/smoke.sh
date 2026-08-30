#!/bin/sh
set -eu

pids=""
smoke_key="smoke-$$"
log_dir=$(mktemp -d)

cleanup() {
	status=$?
	for pid in $pids; do
		kill "$pid" 2>/dev/null || true
	done
	if [ "$status" -ne 0 ]; then
		for log in "$log_dir"/*.log; do
			printf '\n==> %s <==\n' "$log" >&2
			tail -n 80 "$log" >&2
		done
	fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

wait_for_tasks() {
	log=$1
	attempt=0
	while [ "$attempt" -lt 60 ]; do
		count=$(grep -c 'task opened' "$log" 2>/dev/null || true)
		if [ "$count" -ge 3 ]; then
			return 0
		fi
		attempt=$((attempt + 1))
		sleep 1
	done
	printf 'tasks not ready; inspect %s\n' "$log" >&2
	return 1
}

# Global table must contain profile before joins app bootstraps it.
go run ./examples/internal/smokeio profile "$smoke_key"

EXAMPLE_STATE_DIR="$log_dir/filter-map-state" EXAMPLE_STOP_AFTER=50s go run ./examples/filter-map >"$log_dir/filter-map.log" 2>&1 & pids="$pids $!"
EXAMPLE_STATE_DIR="$log_dir/stateful-state" EXAMPLE_STOP_AFTER=50s go run ./examples/stateful >"$log_dir/stateful.log" 2>&1 & pids="$pids $!"
EXAMPLE_STATE_DIR="$log_dir/joins-state" EXAMPLE_STOP_AFTER=50s go run ./examples/joins >"$log_dir/joins.log" 2>&1 & pids="$pids $!"
EXAMPLE_RESTART_AFTER=20s EXAMPLE_STOP_AFTER=50s go run ./examples/eos-recovery >"$log_dir/eos.log" 2>&1 & pids="$pids $!"

wait_for_tasks "$log_dir/filter-map.log"
wait_for_tasks "$log_dir/stateful.log"
wait_for_tasks "$log_dir/joins.log"
wait_for_tasks "$log_dir/eos.log"
go run ./examples/internal/smokeio initial "$smoke_key"
go run ./examples/internal/smokeio verify-initial "$smoke_key"

# Phase 2 uses empty local state. Count 2 proves changelog value 1 restored first.
sleep 12
go run ./examples/internal/smokeio second "$smoke_key"
go run ./examples/internal/smokeio verify-final "$smoke_key"

for pid in $pids; do
	wait "$pid"
done
pids=""
rm -r "$log_dir"
