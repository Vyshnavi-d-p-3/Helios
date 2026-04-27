#!/usr/bin/env bash
set -euo pipefail

# TSBS helper for Helios.
# Generates Influx line protocol data, converts it to remote_write payloads,
# and posts batches to Helios.

TARGET="${TARGET:-http://127.0.0.1:8080/api/v1/write}"
SCALE="${SCALE:-100}"
SEED="${SEED:-123}"
TSBS_DIR="${TSBS_DIR:-bench/tsbs/tsbs}"
OUT_FILE="${OUT_FILE:-bench/tsbs/data.influx}"
BATCH="${BATCH:-1000}"
CONCURRENCY="${CONCURRENCY:-8}"

if [[ ! -x "${TSBS_DIR}/bin/tsbs_generate_data" ]]; then
  echo "error: tsbs_generate_data not found under ${TSBS_DIR}/bin"
  echo "hint: cd bench/tsbs && git clone --depth 1 https://github.com/timescale/tsbs.git && make -C tsbs tsbs_generate_data"
  exit 1
fi

mkdir -p "$(dirname "${OUT_FILE}")"

echo "generating TSBS data (scale=${SCALE}, seed=${SEED})..."
"${TSBS_DIR}/bin/tsbs_generate_data" \
  --use-case=devops \
  --seed="${SEED}" \
  --scale="${SCALE}" \
  --timestamp-start="2024-01-01T00:00:00Z" \
  --timestamp-end="2024-01-01T00:15:00Z" \
  --log-interval="10s" \
  --format="influx" > "${OUT_FILE}"

echo "loading into Helios at ${TARGET} ..."
go run ./bench/tsbs/cmd/influx_to_remote_write \
  --in "${OUT_FILE}" \
  --target "${TARGET}" \
  --batch "${BATCH}" \
  --concurrency "${CONCURRENCY}"
