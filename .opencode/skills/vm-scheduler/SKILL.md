# VM Scheduler

Add a scheduler layer between the executor and the VM: `Executor → Scheduler → WorkerQueue → ThreadPool → VM`. Async and Parallel operations submit work to the scheduler instead of blocking. Use `rayon` or a custom work-stealing thread pool.

**Files affected:**
- `rust/src/executor/mod.rs`
- `rust/src/executor/scheduler.rs` (new)
- `rust/src/executor/worker.rs` (new)

**Verification:** Parallel opcodes (`p:`) execute concurrently. Chunk operations distribute across workers. Existing serial tests pass.
