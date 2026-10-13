# Execution Context

`ExecutionContext` — a struct threaded through the VM that carries MessageID, CorrelationID, TraceID, Partition, Offset, Headers, Deadline, Tenant, RetryCount, and local variables. The VM should operate on the context, not directly on the body.

**Implemented:**
- `rust/src/executor/context.rs` — `ExecutionContext` struct with message_id, correlation_id, trace_id, partition, offset, headers, retry_count, tenant, deadline_ms
- `rust/src/executor/mod.rs` — `ctx` field on `VM`, initialized in `new()`
- `rust/src/ffi.rs` — `flowrulz_execute` now accepts optional context parameters (msg_id, corr_id, trace_id ptr+len, partition u32, offset i64)
- `go/internal/bridge/bridge.go` — `ExecContext` Go type, `Execute` accepts ctx parameter, passes all fields through FFI

**Still needed:**
- Thread context through individual op handlers (Next, Parallel, Gate, Map, Emit, Dag)
- Pass context to the `caller_cb` for service-level tracing
- Add headers, retry_count, tenant, deadline_ms to FFI

**Verification:** Existing tests pass unchanged. New `TestExecuteWithContext` and `TestExecuteWithPartialContext` verify context fields pass through FFI.
