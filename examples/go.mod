module github.com/mortezaPRK/gstream/examples

go 1.27

require (
	github.com/mortezaPRK/gstream v0.0.0
	github.com/mortezaPRK/gstream/serdes/json v0.0.0
)

require (
	github.com/klauspost/compress v1.18.7 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go v1.21.6 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)

replace github.com/mortezaPRK/gstream => ..

replace github.com/mortezaPRK/gstream/serdes/json => ../serdes/json
