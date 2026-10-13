# Bytecode Versioning

Add a header to the serialized ExecutionPlan: Magic bytes, Version, Checksum, Flags, Instruction Count. The deserializer checks the version and rejects incompatible plans. Prevents "Plan V2 → VM V1" crashes.

**Files affected:**
- `rust/src/bytecode/plan.rs`
- `rust/src/executor/mod.rs` (deserialize check)

**Verification:** Plan serialized with V2 is rejected by V1 deserializer. Tests for version mismatch.
