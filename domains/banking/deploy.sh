#!/bin/bash
# Deploy banking domain contracts and rules

set -e

API_URL="${FLOWRULZ_API:-http://localhost:8080}"
TENANT_ID="${FLOWRULZ_TENANT:-development-tenant}"

echo "Deploying banking example to $API_URL"

# Generated code lives in ./builds/go (make generate-domain); deploy only registers contracts and rules.

echo "Registering contracts..."
for f in contracts/*.yaml; do
  echo "  Registering $f"
  curl --fail-with-body -sS -X POST "$API_URL/v1/contracts" \
    -H "X-Tenant-ID: $TENANT_ID" -H "Content-Type: application/yaml" \
    --data-binary @"$f" | jq .
done

echo "Deploying rules..."
for f in rules/*.yaml; do
  ruleSet=$(basename "$f" .yaml)
  echo "  Deploying $ruleSet"
  curl --fail-with-body -sS -X POST "$API_URL/v1/rules/$ruleSet/revisions" \
    -H "X-Tenant-ID: $TENANT_ID" -H "Content-Type: application/yaml" \
    --data-binary @"$f" | jq .
done

echo "Registering domain..."
event_types=$(for f in contracts/*.yaml; do basename "$f" .yaml; done | jq -R . | jq -sc .)
domain_id=$(curl -sS "$API_URL/v1/domains" -H "X-Tenant-ID: $TENANT_ID" | jq -r '.data.domains[] | select(.name=="banking") | .id')
if [ -n "$domain_id" ]; then
  curl --fail-with-body -sS -X PUT "$API_URL/v1/domains/$domain_id" -H "X-Tenant-ID: $TENANT_ID" -H "Content-Type: application/json" \
    -d "{\"name\":\"banking\",\"event_types\":$event_types}" | jq -c .data.event_types
else
  curl --fail-with-body -sS -X POST "$API_URL/v1/domains" -H "X-Tenant-ID: $TENANT_ID" -H "Content-Type: application/json" \
    -d "{\"name\":\"banking\",\"event_types\":$event_types}" | jq -c .data.event_types
fi

# Workflow definitions (workflows/*.yaml) are reference files: the tenant API does not expose workflow mutation.

echo "Banking deployment complete!"