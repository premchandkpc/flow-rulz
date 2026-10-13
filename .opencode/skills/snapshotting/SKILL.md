# Plan Snapshotting

Support saving compiled ExecutionPlans to disk as binary snapshots. The Go engine can mmap a snapshot and pass the pointer directly to the Rust VM. Enables cold-start without recompilation.

**Files affected:**
- `rust/src/bytecode/plan.rs`
- `go/internal/engine/engine.go`
- `go/internal/engine/snapshot.go` (new)

**Verification:** Plan compiled once, saved to disk, loaded from disk, executes identically.
