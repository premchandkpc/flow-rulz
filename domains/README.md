# Domain configuration examples

Each directory is an example Domain configuration package. A Domain contains execution Services, and each Service pins one or more immutable ruleset revisions. Runtime events provide their own `tenant_id` and `partition_key`; neither belongs in these configuration files.

```text
domains/<domain>/
├── domain.json # Domain -> Services -> pinned ruleset revisions
├── contracts/  # versioned event contracts (YAML)
├── rules/      # immutable ruleset source (YAML)
├── workflows/  # reference definitions; not currently managed by the HTTP API
├── deploy.sh   # registers contracts/rules and applies domain.json
└── builds/
    └── go/     # generated Go code, in its own module
```

Run a deploy script from any directory; it resolves files relative to its own location. Set `FLOWRULZ_API` to the API base URL. For production control-plane authentication, set `FLOWRULZ_TRUSTED_PROXY_TOKEN` to the value configured by the API and the script will send it as `X-Proxy-Auth`. Development/test mode can use the API's configured development authentication behavior.

The deployment script publishes contracts and rulesets first, then creates or replaces the Domain using `domain.json`. The Domain payload contains only `name` and `services`; it does not contain event tenant identity, partition keys, or event-type lists.

Generated output is organized under `builds/<lang>`. Only Go is currently checked in (`make generate-domain DOMAIN=<domain>`). Java, Protobuf, and JSON Schema targets exist in `cmd/codegen`, but this script does not write them here.

The root `sdk/` directory contains the domain-independent client runtime used by generated builds.
