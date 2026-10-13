# VM Continuations

Add suspend/resume to the VM. When a service call blocks (e.g., long RPC), the VM suspends the current execution, stores a continuation (instruction pointer + context snapshot), and resumes when the response arrives. Async opcode uses this natively.

**Files affected:**
- `rust/src/executor/mod.rs`
- `rust/src/executor/continuation.rs` (new)
- `rust/src/bytecode/plan.rs`

**Verification:** Async tests pass without blocking the scheduler thread. Throughput under concurrent async calls increases.
