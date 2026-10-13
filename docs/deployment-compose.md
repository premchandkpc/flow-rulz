# Docker Compose deployment

This is the supported local full-stack topology. It builds the Go API, worker, and React console; starts PostgreSQL, NATS JetStream, Redis, Loki, and Promtail; and connects them on a private Docker network.

## Start

From the repository root:

```sh
cp .env.example .env
# Edit .env; defaults are intentionally development-only.
docker compose config
docker compose up --build -d
docker compose ps
# Confirm exactly three worker containers
docker compose ps worker
```

Open **http://localhost:3000**. The UI's default settings use same-origin paths:
- `/api` proxies to `api:8080`
- `/worker-metrics/metrics` proxies to `worker:9090/metrics`
- `/loki` is the base path for Loki's query API, backed by `loki:3100`

Docker's embedded DNS resolves service names such as `postgres`, `nats`, `redis`, `api`, `worker`, and `loki` on the Compose network. Do not use `localhost` in container-to-container URLs: inside a container, localhost means that same container.

## Configuration

`.env.example` is the single checked-in template for Compose configuration. Copy it to `.env`; Compose loads it for variable substitution and the app services load it through `env_file`. The Compose manifest only sets values that must differ inside a container, such as the listen address and mounted migration path.

Keep these pairs consistent when changing credentials:
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` and `DATABASE_URL`
- `REDIS_PASSWORD` and the app's `REDIS_PASSWORD`
- `NUM_SHARDS` across API, worker, SDK producers and any other event publishers
- `WORKER_REPLICAS` with the deployed worker count; the default Compose topology is three worker replicas

The defaults are for local development only. Use a secret manager or orchestrator secrets for production credentials. Do not commit `.env`. The Compose-published ports are bound to `127.0.0.1`, not all host interfaces.

## Services

| Service | Purpose | Host access |
|---|---|---|
| `console` | React UI and same-origin reverse proxy | `http://localhost:3000` |
| `api` | HTTP management API | `http://localhost:8080` |
| `worker` | Three worker replicas by default; JetStream consumer, shard leases, scheduler, outbox and worker metrics | Metrics are private; proxied through the console |
| `postgres` | Transactional storage | `localhost:5432` |
| `nats` | JetStream event broker | `localhost:4222`; monitoring `localhost:8222` |
| `redis` | Optional cache/coordination dependency | `localhost:6379` |
| `loki` | Local log query/store | Private; queried through console |
| `promtail` | Discovers Compose containers and forwards Docker logs to Loki | No published port |
| `codegen` | On-demand generator CLI | Run with the `tools` profile |

PostgreSQL migrations are applied by the application storage initialization. The Compose file deliberately does not also mount migrations into PostgreSQL's one-time init directory, which could execute them outside the migration tracking mechanism.

## Logs and metrics

The worker exposes Prometheus metrics on port 9090 only inside the Compose network. The console's reverse proxy lets the browser fetch them without publishing the metrics listener or enabling wildcard CORS.

Promtail uses Docker service discovery to tail container stdout/stderr and forwards it to Loki. The console Logs page uses the Loki query endpoint. This is a **local development convenience**, not a hardened production logging topology: Promtail mounts the Docker socket, which is a privileged host interface despite a read-only mount. Do not deploy this socket-based Promtail configuration to shared or production hosts without replacing it with an approved log collector and restricted runtime permissions.

For direct logs, use:

```sh
docker compose logs -f api worker
docker compose logs -f
```

## Useful operations

```sh
# Validate interpolated configuration (requires .env)
docker compose config

# Rebuild and start
docker compose up --build -d

# Follow service output
docker compose logs -f api worker console

# Stop without deleting persisted data
docker compose down

# Destructively remove local database, broker, Redis and Loki data
docker compose down -v

# Run the code generator CLI
docker compose --profile tools run --rm codegen --help
```

## Known boundaries

- The stack starts **three worker replicas by default** (`WORKER_REPLICAS=3`) and is for local development and demos. The API's authentication/tenant configuration must be secured before exposure to untrusted clients.
- Compose starts three workers with `WORKER_REPLICAS=3`. The current lease manager uses a fixed-replica allocation contract, not dynamic membership. If a worker permanently fails, surviving workers do not automatically increase their shard target; restore the replica or deliberately change the configured replica count and recreate the workers. Validate lease fencing and per-key ordering under failure before production use.
- The UI metrics endpoint is process-local and exposes no authentication. It is kept private and only reverse-proxied from the local console.
- MongoDB is an alternative storage backend, not part of the default Compose stack. It requires a replica-set or sharded deployment for transactions.
- The codegen container is a CLI utility, not a long-running HTTP service.
- Docker Compose DNS is service discovery for this single-host deployment. Kubernetes requires separate manifests, secret handling, probes, resource limits, and an explicit worker scaling strategy; this Compose stack does not claim to be a Kubernetes deployment.
