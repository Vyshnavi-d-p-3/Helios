#!/usr/bin/env bash
# Smoke-check a local docker compose cluster (default ports per compose file).
# Run from repo root after: cd deployments && docker compose up -d --build
set -euo pipefail

BASE="${BASE_URLS:-http://127.0.0.1:8080 http://127.0.0.1:8081 http://127.0.0.1:8082}"

for url in ${BASE}; do
  printf 'GET %s/livez -> ' "${url}"
  code="$(curl -sS -o /dev/null -w '%{http_code}' "${url}/livez")"
  echo "${code}"
  [[ "${code}" == "200" ]] || exit 1

  printf 'GET %s/cluster/leader -> ' "${url}"
  curl -sS "${url}/cluster/leader" | head -c 200
  echo
done

echo "smoke_cluster: ok"
