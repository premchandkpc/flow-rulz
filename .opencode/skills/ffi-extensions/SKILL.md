# FFI Extensions

Add to the FFI surface: ABI version handshake, capability negotiation, panic isolation (catch_unwind in extern "C" fns), timeout/cancellation parameters, streaming API for large results, zero-copy slice passing, shared memory regions, and `flowrulz_execute_batch` for bulk processing.

**Files affected:**
- `rust/src/ffi.rs`
- `go/internal/bridge/bridge.go`
- `go/internal/bridge/bridge_test.go`

**Verification:** ABI version check rejects mismatched libraries. Batch execute processes N messages in a single FFI call. Streaming returns partial results without full buffering.
