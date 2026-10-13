# VM Optimizations

Add optimizer passes: constant folding, dead code elimination, common subexpression elimination, jump optimization, tail merging, instruction fusion, branch prediction hints.

**Files affected:**
- `rust/src/dsl/optimizer.rs`
- `rust/src/dsl/optimizer_pass.rs` (new)

**Verification:** Each optimization pass has unit tests. Compiled plans produce fewer instructions for equivalent DSL.
