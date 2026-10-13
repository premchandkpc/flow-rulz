# Memory Pools

Add typed object pools: Expression Pool, Execution Context Pool, Plan Pool, JSON Arena, Small Vector (inline storage for small collections), Scratch Buffers, Thread-Local Arena. Allocate from thread-local slabs where possible, falling back to the global slab pool.

**Files affected:**
- `rust/src/memory/pool.rs` (new)
- `rust/src/memory/arena.rs`
- `rust/src/memory/slab.rs`

**Verification:** Benchmarks show reduced allocation pressure. Thread-local pools don't synchronize on the hot path.
