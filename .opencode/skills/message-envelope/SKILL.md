# Message Envelope

Replace raw JSON body with a structured `Envelope` containing Headers, Body, Metadata, Topic, Partition, Offset, Timestamp, Key, and Attributes. The FFI boundary should pass an envelope (serialized) instead of a raw byte slice.

**Files affected:**
- `rust/src/ffi.rs`
- `rust/src/executor/mod.rs`
- `go/internal/bridge/bridge.go`
- `go/internal/transport/consumer.go`

**Verification:** All existing tests pass. Envelope serialization/deserialization tests added.
