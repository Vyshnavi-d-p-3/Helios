# Helios — 5 minute demo

## 0) Build and start

```bash
go build ./cmd/helios
./helios -data-dir /tmp/helios-demo -http-addr :8080 -anomaly-enabled
```

## 1) Write sample data

```bash
curl -sS -X POST localhost:8080/api/v1/write \
  -H 'Content-Type: application/json' \
  -d '{"samples":[
    {"metric":"http_requests_total","labels":{"status":"200","job":"api"},"ts":1000,"value":100},
    {"metric":"http_requests_total","labels":{"status":"200","job":"api"},"ts":61000,"value":160},
    {"metric":"http_requests_total","labels":{"status":"500","job":"api"},"ts":61000,"value":3}
  ]}'
```

## 2) Native query

```bash
curl -sS 'localhost:8080/api/v1/query?metric=http_requests_total&partial=1&t=61000' | jq
```

## 3) PromQL rate

```bash
curl -sS 'localhost:8080/api/v1/query?query=rate(http_requests_total[1m])&t=61000' | jq
```

## 4) PromQL aggregation

```bash
curl -sS 'localhost:8080/api/v1/query?query=sum%20by%20(status)%20(http_requests_total)&t=61000' | jq
```

## 5) Label discovery

```bash
curl -sS localhost:8080/api/v1/labels | jq
curl -sS localhost:8080/api/v1/label/status/values | jq
```

## 6) Anomaly stream

Open a terminal:

```bash
curl -N localhost:8080/api/v1/anomalies
```

Then inject a spike in another:

```bash
curl -sS -X POST localhost:8080/api/v1/write \
  -H 'Content-Type: application/json' \
  -d '{"samples":[{"metric":"cpu","labels":{"host":"a"},"ts":1000,"value":1}]}'
for i in $(seq 1 100); do
  curl -sS -X POST localhost:8080/api/v1/write \
    -H 'Content-Type: application/json' \
    -d "{\"samples\":[{\"metric\":\"cpu\",\"labels\":{\"host\":\"a\"},\"ts\":$((i*1000+1000)),\"value\":1}]}" >/dev/null
done
curl -sS -X POST localhost:8080/api/v1/write \
  -H 'Content-Type: application/json' \
  -d '{"samples":[{"metric":"cpu","labels":{"host":"a"},"ts":200000,"value":500}]}' >/dev/null
```

## 7) Metrics endpoint

```bash
curl -sS localhost:8080/metrics | head -40
```

## 8) Benchmark path

```bash
SCALE=10 TARGET=http://127.0.0.1:8080/api/v1/write ./bench/tsbs/load_helios.sh
go run ./bench/tsbs/cmd/query_benchmark -target http://127.0.0.1:8080 -queries 1000
```

## 9) Multi-node (optional)

Use Docker Compose from `deployments/` (see root **README** for env vars). After nodes are up, check leadership on any instance:

```bash
curl -sS http://127.0.0.1:8080/cluster/leader | jq
```

Writes to a follower are proxied to the leader’s HTTP API automatically.
