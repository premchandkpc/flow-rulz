#!/bin/bash
# Generate typed Go code for a domain from its contracts stored in the database into domains/<domain>/builds/go.
# Usage: scripts/generate-domain.sh <banking|ecommerce|food-delivery>
set -euo pipefail

domain="${1:?usage: $0 <banking|ecommerce|food-delivery>}"
root="$(cd "$(dirname "$0")/.." && pwd)"
dir="$root/domains/$domain"
[ -d "$dir/contracts" ] || { echo "unknown domain: $domain" >&2; exit 2; }

export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/flow-rulz?sslmode=disable}"
out="$root/domains/$domain/builds/go"
mkdir -p "$out"
cd "$root"
go build -o /tmp/flow-rulz-codegen ./cmd/codegen

for f in "$dir"/contracts/*.yaml; do
  name=$(awk -F': *' '/^name:/{print $2; exit}' "$f" | tr -d '"')
  version=$(awk -F': *' '/^version:/{print $2; exit}' "$f" | tr -d '"')
  echo "Generating $name@$version"
  pkg="${name//./_}"
  tmp="$(mktemp -d)"
  /tmp/flow-rulz-codegen -contract "$name" -version "$version" -target go -output "$tmp" >/dev/null
  rm -rf "$out/$pkg"
  mkdir -p "$out/$pkg"
  cp "$tmp"/go/*.go "$out/$pkg/"
  rm -rf "$tmp"
done
echo "Generated code for $domain in domains/$domain/builds/go"
