#!/usr/bin/env bash
# Deploy e-commerce contracts, immutable ruleset revisions, and Domain assignments.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

API_URL="${FLOWRULZ_API:-http://localhost:8080}"
DOMAIN_FILE="$SCRIPT_DIR/domain.json"
AUTH_ARGS=()
if [[ -n "${FLOWRULZ_TRUSTED_PROXY_TOKEN:-}" ]]; then
  AUTH_ARGS=(-H "X-Proxy-Auth: ${FLOWRULZ_TRUSTED_PROXY_TOKEN}")
fi

command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
test -f "$DOMAIN_FILE" || { echo "Missing Domain configuration: $DOMAIN_FILE" >&2; exit 1; }

echo "Deploying e-commerce configuration to $API_URL"

echo "Registering contracts..."
for file in "$SCRIPT_DIR"/contracts/*.yaml; do
  [[ -e "$file" ]] || continue
  echo "  Registering $(basename "$file")"
  curl --fail-with-body -sS -X POST "$API_URL/v1/contracts" \
    "${AUTH_ARGS[@]}" \
    -H "Content-Type: application/yaml" \
    --data-binary @"$file" | jq .
done

echo "Publishing ruleset revisions..."
for file in "$SCRIPT_DIR"/rules/*.yaml; do
  [[ -e "$file" ]] || continue
  rule_set="$(basename "$file" .yaml)"
  echo "  Publishing $rule_set"
  curl --fail-with-body -sS -X POST "$API_URL/v1/rules/$rule_set/revisions" \
    "${AUTH_ARGS[@]}" \
    -H "Content-Type: application/yaml" \
    --data-binary @"$file" | jq .
done

domain_name="$(jq -r '.name' "$DOMAIN_FILE")"
domain_id="$(
  curl --fail-with-body -sS "$API_URL/v1/domains" "${AUTH_ARGS[@]}" |
    jq -r --arg name "$domain_name" '.data.domains[]? | select(.name == $name) | .id' |
    head -n 1
)"

echo "Applying Domain -> Services -> ruleset assignments..."
if [[ -n "$domain_id" && "$domain_id" != "null" ]]; then
  curl --fail-with-body -sS -X PUT "$API_URL/v1/domains/$domain_id" \
    "${AUTH_ARGS[@]}" \
    -H "Content-Type: application/json" \
    --data-binary @"$DOMAIN_FILE" | jq .
else
  curl --fail-with-body -sS -X POST "$API_URL/v1/domains" \
    "${AUTH_ARGS[@]}" \
    -H "Content-Type: application/json" \
    --data-binary @"$DOMAIN_FILE" | jq .
fi

echo "e-commerce deployment configuration complete."
