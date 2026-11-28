---
title: Archived Rust VM agent skills
type: proposal
status: historical
owner: premchandkpc
last_verified: 2026-11-28 @ 285a8c0
---

# Archived Rust VM agent skills

Fourteen agent skills were kept in `.opencode/skills/` describing a bytecode VM, scheduler, DSL compiler and Go/Rust bridge. **Planned, never built in this repository.** At commit `285a8c0` none of the files they target exist (`rust/src/executor/`, `rust/src/bytecode/`, `rust/src/dsl/`, `go/internal/engine/`, `go/internal/bridge/`). The shipped Rust worker is `rust-worker/` and has no VM. See [Rust worker](../rust-worker.md) and [Rust/Go parity](../architecture/rust-go-worker-parity.md) for what exists.

They were removed from the agent skill path because an agent loading one would scaffold code at paths that do not match the repo. The text is preserved here as design history, and the original files remain in git history before this change. Treat every item as Planned.

## Execution Context

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

Summary: Add ExecutionContext struct to VM — MessageID, CorrelationID, TraceID, Partition, Offset, Headers, Deadline, Tenant, RetryCount, local vars

## Message Envelope

Replace raw JSON body with a structured `Envelope` containing Headers, Body, Metadata, Topic, Partition, Offset, Timestamp, Key, and Attributes. The FFI boundary should pass an envelope (serialized) instead of a raw byte slice.

Files it targeted (none exist):
- `rust/src/ffi.rs`
- `rust/src/executor/mod.rs`
- `go/internal/bridge/bridge.go`
- `go/internal/transport/consumer.go`

**Verification:** All existing tests pass. Envelope serialization/deserialization tests added.

Summary: Replace raw JSON body with structured Envelope containing Headers, Body, Metadata, Topic, Partition, Offset, Timestamp, Key

## VM Scheduler

Add a scheduler layer between the executor and the VM: `Executor → Scheduler → WorkerQueue → ThreadPool → VM`. Async and Parallel operations submit work to the scheduler instead of blocking. Use `rayon` or a custom work-stealing thread pool.

Files it targeted (none exist):
- `rust/src/executor/mod.rs`
- `rust/src/executor/scheduler.rs` (new)
- `rust/src/executor/worker.rs` (new)

**Verification:** Parallel opcodes (`p:`) execute concurrently. Chunk operations distribute across workers. Existing serial tests pass.

Summary: Add Executor → Scheduler → WorkerQueue → ThreadPool → VM layer for async/parallel dispatch

## Work-Stealing Deque

Replace the simple thread pool with a work-stealing deque for parallel execution. Workers that complete their tasks steal pending work from overloaded workers. Use `crossbeam-deque` or `rayon-core` directly.

Files it targeted (none exist):
- `rust/src/executor/scheduler.rs`
- `Cargo.toml` (add `crossbeam-deque` if not present)

**Verification:** Parallel benchmarks show balanced load across workers.

Summary: Work-stealing deques for parallel execution via crossbeam-deque

## VM Continuations

Add suspend/resume to the VM. When a service call blocks (e.g., long RPC), the VM suspends the current execution, stores a continuation (instruction pointer + context snapshot), and resumes when the response arrives. Async opcode uses this natively.

Files it targeted (none exist):
- `rust/src/executor/mod.rs`
- `rust/src/executor/continuation.rs` (new)
- `rust/src/bytecode/plan.rs`

**Verification:** Async tests pass without blocking the scheduler thread. Throughput under concurrent async calls increases.

Summary: Suspend/resume VM when service calls block; continuations for async opcode

## Bytecode Versioning

Add a header to the serialized ExecutionPlan: Magic bytes, Version, Checksum, Flags, Instruction Count. The deserializer checks the version and rejects incompatible plans. Prevents "Plan V2 → VM V1" crashes.

Files it targeted (none exist):
- `rust/src/bytecode/plan.rs`
- `rust/src/executor/mod.rs` (deserialize check)

**Verification:** Plan serialized with V2 is rejected by V1 deserializer. Tests for version mismatch.

Summary: Header with Magic, Version, Checksum on serialized plans; reject incompatible versions

## Rule Cache

Add an LRU cache in the Go engine layer for compiled plans. `Engine.Deploy` compiles and caches. `Engine.GetCached` returns compiled plan without recompiling. The cache has a configurable max size and eviction policy.

Files it targeted (none exist):
- `go/internal/engine/engine.go`
- `go/internal/engine/cache.go` (new)

**Verification:** Repeated execution of the same rule ID uses cached plan. Cache eviction works under load.

Summary: LRU cache in Go engine for compiled plans; avoid recompilation on hot path

## Plan Snapshotting

Support saving compiled ExecutionPlans to disk as binary snapshots. The Go engine can mmap a snapshot and pass the pointer directly to the Rust VM. Enables cold-start without recompilation.

Files it targeted (none exist):
- `rust/src/bytecode/plan.rs`
- `go/internal/engine/engine.go`
- `go/internal/engine/snapshot.go` (new)

**Verification:** Plan compiled once, saved to disk, loaded from disk, executes identically.

Summary: Save/load compiled plans as binary snapshots; mmap for cold-start

## Plugin System

Add a plugin interface for service calls: Builtin (Rust native), WASM (extensible), Remote (HTTP/gRPC). The service table maps service names to plugin type + config. WASM plugins run in a sandboxed runtime.

Files it targeted (none exist):
- `rust/src/executor/plugin.rs` (new)
- `rust/src/executor/next.rs`
- `Cargo.toml` (add `wasmtime` or similar)

**Verification:** Builtin plugin works as before. WASM plugin executes user-provided bytecode. Remote plugin calls an HTTP endpoint.

Summary: Plugin interface for service calls: Builtin, WASM, Remote (HTTP/gRPC)

## Kafka Transport — Full Semantics

Replace the in-memory transport stubs with real Kafka integration using `confluent-kafka-go`. Implement consumer groups, partition assignment, offset commit, batch poll, backpressure, poison messages, rebalance handling, and dead letter queues.

Files it targeted (none exist):
- `go/internal/transport/consumer.go`
- `go/internal/transport/producer.go`
- `go/internal/transport/consumer_group.go` (new)
- `go/internal/transport/dlx.go` (new)
- `go.mod`

**Verification:** Integration tests with a real Kafka broker (or test container) verify consumer group rebalancing, offset commits, and poison message routing to DLQ.

Summary: Full Kafka semantics: consumer groups, offset commit, batch poll, backpressure, poison messages, DLQ, rebalance

## DSL Extensions

Add DSL tokens for: `loop`, `window`, `aggregate`, `filter`, `merge`, `join`, `cache`, `state`, `timer`, `delay`, `await`, `race`, `foreach`, `reduce`, `switch`, `try`/`catch`/`finally`. Each new token requires lexer, parser, AST, compiler, and VM handler changes.

Files it targeted (none exist):
- `rust/src/dsl/lexer.rs`
- `rust/src/dsl/parser.rs`
- `rust/src/dsl/compiler.rs`
- `rust/src/bytecode/opcode.rs`
- `rust/src/executor/mod.rs`

**Verification:** Each new DSL construct has unit tests covering lex → parse → compile → execute.

Summary: Add loop, window, aggregate, filter, merge, join, cache, state, timer, delay, await, race, try/catch DSL tokens

## VM Optimizations

Add optimizer passes: constant folding, dead code elimination, common subexpression elimination, jump optimization, tail merging, instruction fusion, branch prediction hints.

Files it targeted (none exist):
- `rust/src/dsl/optimizer.rs`
- `rust/src/dsl/optimizer_pass.rs` (new)

**Verification:** Each optimization pass has unit tests. Compiled plans produce fewer instructions for equivalent DSL.

Summary: Optimizer passes: constant folding, DCE, CSE, jump optimization, tail merging, instruction fusion

## FFI Extensions

Add to the FFI surface: ABI version handshake, capability negotiation, panic isolation (catch_unwind in extern "C" fns), timeout/cancellation parameters, streaming API for large results, zero-copy slice passing, shared memory regions, and `flowrulz_execute_batch` for bulk processing.

Files it targeted (none exist):
- `rust/src/ffi.rs`
- `go/internal/bridge/bridge.go`
- `go/internal/bridge/bridge_test.go`

**Verification:** ABI version check rejects mismatched libraries. Batch execute processes N messages in a single FFI call. Streaming returns partial results without full buffering.

Summary: ABI version handshake, panic isolation, timeout/cancellation, streaming API, batch execute

## Memory Pools

Add typed object pools: Expression Pool, Execution Context Pool, Plan Pool, JSON Arena, Small Vector (inline storage for small collections), Scratch Buffers, Thread-Local Arena. Allocate from thread-local slabs where possible, falling back to the global slab pool.

Files it targeted (none exist):
- `rust/src/memory/pool.rs` (new)
- `rust/src/memory/arena.rs`
- `rust/src/memory/slab.rs`

**Verification:** Benchmarks show reduced allocation pressure. Thread-local pools don't synchronize on the hot path.

Summary: Typed object pools: Expression Pool, Context Pool, Plan Pool, JSON Arena, thread-local arenas
