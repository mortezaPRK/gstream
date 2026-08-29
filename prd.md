# gstream Functional v1 PRD

Status: functional v1 behavior final; acceptance evidence tracked below

Toolchain: Go 1.27.0

Performance status: benchmarks deferred; performance numbers unverified

## Product

gstream is Go library for typed Kafka stream processing. Users build topology with `KStream`, `KTable`, and `GlobalKTable`, then run it through `gstream/app`. franz-go, Pebble, Kafka admin requests, task scheduling, and restore mechanics remain internal.

v1 supports one live application instance processing multiple partitions. One task owns each partition. Standby replicas, live cross-instance state migration, and advanced store/cache tuning are post-v1.

## Public runtime

```go
runtime, err := app.New(config, builtTopology,
    app.WithLogger(logger),
    app.WithPrometheusRegisterer(registry),
)
err = runtime.Run(ctx)
err = runtime.Close()
```

`app.New` applies configuration defaults, validates configuration and topology, and registers metrics. Duplicate Prometheus registration fails without leaving partially registered collectors. `Run` may execute once and blocks until context cancellation, `Close`, or fatal runtime error. `Close` is idempotent and waits for active lifecycle cleanup.

Startup order:

1. Validate configuration and built topology.
2. Inspect caller-managed source, sink, and global-table topics without auto-creation.
3. Resolve source partition count and reject mismatches.
4. Plan, create, or validate deterministic changelog and repartition topics.
5. Bootstrap global stores from all partitions.
6. Start global-table tail consumers.
7. Create Kafka processing client and join consumer group.
8. Restore assigned partition tasks before records flow.

Missing caller-managed topics fail with exact topic names. Internal-topic creation errors report required names and configurations.

## Processing model

Fetched records are grouped by task partition. Each partition is processed sequentially. Distinct partitions run concurrently up to `NumTaskThreads`. Executors, stores, mutation collectors, and stream-time remain partition-local.

### At-least-once

Outputs and changelog mutations are acknowledged before source offsets enter pending commit set. `CommitInterval` batches latest successful offset per topic-partition. Revocation and clean shutdown flush pending offsets. Crash before commit can replay records and duplicate output.

### Exactly-once

One transaction spans polls until `CommitInterval`. Sink output, changelog mutations, and consumed offsets commit atomically. Processing consumers and restore probes use read-committed semantics.

Any failure or broker/session abort after processing may have changed local Pebble state is fatal. Runtime aborts transaction, stops, and requires task reopen plus restore from committed changelog. Continuing with Pebble ahead of committed Kafka state is forbidden.

Context cancellation aborts open transaction and exits cleanly. Unknown transaction commit outcome is fatal.

## State and recovery

v1 state types:

- Key-value aggregation stores
- Time-window stores with grace and late-record handling
- Session-window stores with merge and retention
- Stream-table materializations
- Two-sided stream-stream join stores
- Fully replicated global tables

Every partition-local store has deterministic `<application-id>-<store>-changelog`. Key-value changelogs use compaction. Window/session changelogs use compaction plus deletion with retention of window or gap plus grace.

Restore starts after local checkpoint and terminates at committed Kafka state. Raw read-committed fetch probe reads Last Stable Offset, allowing all-aborted changelog range to finish without caller deadline.

## Operators and partitioning

Functional v1 operators:

- Filter, Map, MapValues, SelectKey
- GroupByKey, Count, Aggregate
- Tumbling and hopping time windows
- Session windows
- Stream-table inner join
- Windowed stream-stream inner join
- Global-table lookup join
- Explicit and automatic repartition

`Map` and `SelectKey` mark key distribution dirty. Downstream grouping and joins insert deterministic repartition edges automatically. Generated repartition bindings infer source partition count during startup; explicit `Repartition` count overrides inference. Current v1 requires all source topics in one topology to use matching partition counts.

## Internal topics

gstream auto-creates only:

- `<application-id>-<store>-changelog`
- `<application-id>-<binding>-repartition`

Source, sink, and global-table topics are user-managed and never auto-created by public runtime. Existing internal topics must match planned partition count. Repartition topics use delete cleanup policy. Key-value changelogs use compact; window and session changelogs use compact-and-delete with retention metadata.

## Observability

Structured logs use caller logger or `slog.Default`. Caller may provide Prometheus registerer; global registry is never used implicitly.

Runtime-fed metrics cover:

- Records in/out and processing latency
- Consumer lag
- Commit count and duration
- Restore progress and duration
- Late and dropped records
- Transaction aborts
- Store size
- Pebble cache hits and misses

## Examples

Four public-facade applications live under `examples/`:

1. Stateless ALO filter/map
2. Stateful count plus time/session windows
3. Stream-table, stream-stream, and global-table joins
4. EOS count with controlled stop, empty-local-state restart, and changelog recovery

Shared Compose broker and Make targets start Kafka, create caller-managed topics, run each app, seed smoke inputs, assert sink/changelog records, and stop Kafka.

## Implementation evidence

| Area | Repository evidence | Current proof |
| --- | --- | --- |
| Stable toolchain and split CI | `.github/workflows/ci.yml`, `.golangci.yml`, PR #25 | Go 1.27.0; lint and unit jobs pass; e2e regression under repair |
| Clean EOS shutdown and bounded restore | `internal/kafka/client_test.go`, `internal/state/restore_test.go`, PR #26 | unit and integration-tag compile pass; local Docker unavailable |
| Fatal EOS abort recovery | `internal/kafka/client.go`, EOS restart integration test, PR #27 | unit and integration-tag compile pass; local Docker unavailable |
| Commit cadence and task concurrency | `internal/kafka/client.go`, `internal/kafka/batch_test.go`, PR #28 | unit and race suites pass locally |
| Internal topics and repartition | `internal/kafka/admin_plan_test.go`, `repartition_test.go`, PR #29 | planner unit tests pass; broker test compiles and skips without Docker |
| Public runtime and metrics | `app/app_test.go`, `app/metrics_test.go`, PR #30 | facade, lifecycle, conflict, metric, CI, and race tests pass locally |
| Examples and docs | `examples/`, `README.md`, this PRD | all examples compile; broker smoke requires Docker |

Evidence labels remain strict: compiled, skipped, passed, failed, and pending are not interchangeable. Functional implementation is complete only when final branch gates below are green.

## Final acceptance gates

- `make tidy`
- `make ci`
- `make lint`
- `make test-race`
- `make integration-test` with Docker-backed tests executed, not skipped
- `make example-smoke` against shared Compose broker
- Clean Git status after committed work
- Successful GitHub lint, unit, and e2e checks

Performance benchmarks are not acceptance gate.

## Deferred post-v1

- Performance benchmarks and latency or throughput claims
- Standby replicas and warm failover
- Multi-instance live state migration
- Advanced Pebble/cache tuning
- Schema registry adapters
- Cross-cluster processing
- Release tags, changelog, packaging, and release automation
