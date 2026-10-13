# Domains

Each directory is one platform domain. Clients (tenants) subscribe to a domain; the example services under `examples/` consume it through FlowRulz.

```
domains/<domain>/
├── contracts/   # event contracts (YAML) – source for code generation
├── rules/       # rule sets (YAML)
├── workflows/   # workflow definitions (reference only; the tenant API does not expose workflow mutation)
├── deploy.sh    # registers contracts and rules with a running API
└── builds/
    └── go/      # generated Go code (its own module, depends on sdk/go/flowrulz)
```

Generated output is organised per target language under `builds/<lang>`. Only `go` is written into the repository today (`make generate-domain DOMAIN=<domain>`, which reads the contracts stored in the database). Java, Protobuf and JSON Schema targets exist in `cmd/codegen` but this script does not write them here.

`sdk/` stays at the repository root: it is the domain-independent client runtime that every generated build imports.
