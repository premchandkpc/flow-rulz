# Banking Example

Banking is a **domain**; this folder holds the **services** that work inside it. The domain specification (contracts, rule sets, workflow, generated Go types) lives in [`domains/banking/`](../../domains/banking). The services here import `domains/banking/builds/go` and `sdk/go/flowrulz`.

## Flow

```
transaction.initiated (rule set, all_matches)
  ├─ fraud_screen ............ → fraud.screening_requested   (fraud-screening service)
  ├─ high_value_compliance ... → compliance.check            (compliance service)
  ├─ international_compliance  → compliance.check
  ├─ structuring_watch ....... → aml.alert_raised            (aml service)
  └─ low_risk_authorize ...... → transaction.authorized

fraud.screening_result (first_match)
  deny → transaction.blocked · review → transaction.review_required · approve → transaction.authorized

compliance.result (first_match)
  failed → transaction.blocked (+ aml.alert_raised) · risky → transaction.review_required · passed → transaction.authorized

transaction.authorized (all_matches)
  settle → transaction.settled (ledger) · notify_large → notification.requested (customer-care)

transaction.blocked → notify_blocked → notification.requested
```

Services (one goroutine each in `backend/consumer`): fraud-screening, compliance, ledger, operations, customer-care, aml.

## Run

```sh
docker compose -f docker-compose.yml up -d postgres nats redis api worker
(cd domains/banking && FLOWRULZ_API=http://localhost:18080 ./deploy.sh)   # contracts, rule sets, domain
cd examples/banking
FLOWRULZ_TRANSPORT=nats FLOWRULZ_TENANT_ID=development-tenant go run ./backend/consumer
FLOWRULZ_TRANSPORT=nats FLOWRULZ_TENANT_ID=development-tenant go run ./backend/producer
```

The producer sends seven scenarios (low-risk, mid-value, structuring-sized, high-value, large wire to GB, international to DE, sanctioned IR).

## Regenerate builds

```sh
make generate-domain DOMAIN=banking
```

Delivery is at-least-once; each hop takes a few seconds because follow-up events go through the transactional outbox.
