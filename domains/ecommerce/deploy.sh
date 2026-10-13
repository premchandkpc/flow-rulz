#!/bin/bash
# Deploy e-commerce domain contracts and rules

set -e

API_URL="${FLOWRULZ_API:-http://localhost:8080}"
TENANT_ID="${FLOWRULZ_TENANT:-development-tenant}"

echo "Deploying e-commerce example to $API_URL"

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

# Workflow definitions (workflows/*.yaml) are reference files: the tenant API does not expose workflow mutation.

echo "E-commerce deployment complete!"