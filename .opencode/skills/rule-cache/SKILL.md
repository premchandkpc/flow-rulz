# Rule Cache

Add an LRU cache in the Go engine layer for compiled plans. `Engine.Deploy` compiles and caches. `Engine.GetCached` returns compiled plan without recompiling. The cache has a configurable max size and eviction policy.

**Files affected:**
- `go/internal/engine/engine.go`
- `go/internal/engine/cache.go` (new)

**Verification:** Repeated execution of the same rule ID uses cached plan. Cache eviction works under load.
