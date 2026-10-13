# Work-Stealing Deque

Replace the simple thread pool with a work-stealing deque for parallel execution. Workers that complete their tasks steal pending work from overloaded workers. Use `crossbeam-deque` or `rayon-core` directly.

**Files affected:**
- `rust/src/executor/scheduler.rs`
- `Cargo.toml` (add `crossbeam-deque` if not present)

**Verification:** Parallel benchmarks show balanced load across workers.
