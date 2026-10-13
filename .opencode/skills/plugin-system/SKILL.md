# Plugin System

Add a plugin interface for service calls: Builtin (Rust native), WASM (extensible), Remote (HTTP/gRPC). The service table maps service names to plugin type + config. WASM plugins run in a sandboxed runtime.

**Files affected:**
- `rust/src/executor/plugin.rs` (new)
- `rust/src/executor/next.rs`
- `Cargo.toml` (add `wasmtime` or similar)

**Verification:** Builtin plugin works as before. WASM plugin executes user-provided bytecode. Remote plugin calls an HTTP endpoint.
