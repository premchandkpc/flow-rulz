module github.com/flowrulz/flow-rulz-sdk/go

go 1.26.0

require (
	github.com/flowrulz/flow-rulz-sdk/go/flowrulz v0.0.0
	github.com/google/uuid v1.6.0
)

require (
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/nats-io/nats.go v1.54.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/flowrulz/flow-rulz-sdk/go/flowrulz => ./flowrulz
