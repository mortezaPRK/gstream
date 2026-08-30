# gstream

Typed Kafka stream processing for Go 1.27. gstream combines generics-based DSL, franz-go, and partition-local Pebble state behind public application runtime.

## Quick start

```go
builder := gstream.NewStreamBuilder()
input := gstream.Stream(builder, "orders", "orders-source",
    gstream.JSONSerde[string]{}, gstream.JSONSerde[Order]{})
input.Filter(func(_ string, order Order) bool { return order.Active }).
    To("active-orders", "active-orders-sink",
        gstream.JSONSerde[string]{}, gstream.JSONSerde[Order]{})

cfg, _ := gstream.Configure(
    gstream.WithName("orders-app"),
    gstream.WithBrokers("localhost:9092"),
)
runtime, _ := app.New(cfg, builder.Build(),
    app.WithPrometheusRegisterer(prometheus.NewRegistry()))
err := runtime.Run(ctx)
```

Caller creates source, sink, and global-table topics. `Run` validates them, creates deterministic changelog and repartition topics, restores state, then joins Kafka consumer group.

Useful commands:

```sh
make ci
make lint
make test-race
make examples-up
make examples-topics
make example-filter-map
make example-smoke
make examples-down
```

## Architecture and guarantees

Each assigned Kafka partition owns one task, topology executor, Pebble shard, and changelog partition. Records stay ordered within partition; independent partitions run concurrently up to `NumTaskThreads`.

- At-least-once produces outputs before committing source offsets. `CommitInterval` batches commits. Crashes can duplicate output.
- Exactly-once keeps one Kafka transaction open until `CommitInterval`, atomically committing sink records, changelog mutations, and offsets.
- Any EOS abort after local state may have changed stops application. Restart restores Pebble from read-committed changelog; runtime never continues with local state ahead of Kafka.
- Revocation and shutdown flush ALO pending offsets. Context cancellation aborts open EOS transaction.

## State and internal topics

Key-value, time-window, session-window, stream-table, stream-stream, and global-table state use Pebble. Internal names are deterministic:

- `<application-id>-<store>-changelog`
- `<application-id>-<name>-repartition`

Key-changing `Map` and `SelectKey` trigger automatic repartition before downstream grouping or joins. Explicit `Repartition` overrides generated planning. v1 requires source topics in one topology to use matching partition counts. Window and session changelogs use compact-and-delete retention.

## Metrics

Pass caller-owned Prometheus registerer with `app.WithPrometheusRegisterer`. Registration conflicts fail `app.New`. Metrics include records in/out, processing duration, lag, commits and duration, restore progress and duration, dropped/late records, transaction aborts, store size, and Pebble cache counters.

## Examples

- `examples/filter-map`: stateless ALO filter/map
- `examples/stateful`: count plus time/session windows
- `examples/joins`: stream-table, stream-stream, global-table
- `examples/eos-recovery`: EOS processing, controlled stop, fresh-state restart, changelog recovery

Shared Kafka lives in `examples/compose.yml`. `make example-smoke` seeds all four apps, verifies sink/changelog records, and checks controlled shutdown and EOS restart recovery.

## Limitations

v1 assumes one live application instance, though it processes many partitions concurrently. No standby replicas, live state migration, schema registry integration, cross-cluster support, or advanced store/cache tuning. Performance benchmarks and performance claims remain deferred and unverified.
