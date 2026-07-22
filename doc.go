// Package gstream is a production-grade, stateful stream-processing library for Go.
// It exposes a type-safe, generics-based DSL (KStream, KTable, GlobalKTable) built on
// franz-go (Kafka client) and Pebble (local state store), supporting selectable
// at-least-once and exactly-once processing guarantees. Developers express stateful
// topologies — map, filter, join, windowing, aggregations — with Kafka Streams-style
// ergonomics and idiomatic Go generics, without the JVM.
package gstream
