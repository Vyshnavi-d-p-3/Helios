# PromQL Compliance (Helios Subset)

Helios intentionally supports a focused PromQL subset for v1. This document captures what is covered by smoke tests and what is explicitly out of scope.

## Supported

- Vector selectors with matchers:
  - `metric`
  - `metric{label="value"}`
  - operators: `=`, `!=`, `=~`, `!~`
- Range selectors:
  - `metric[5m]`, `metric[1h]`
- Range functions:
  - `rate()`, `irate()`, `increase()`
  - `avg_over_time()`, `sum_over_time()`, `count_over_time()`
  - `min_over_time()`, `max_over_time()`
- Aggregations:
  - `sum`, `count`, `avg`, `min`, `max`
  - grouping form: `sum by (job) (...)`

## Smoke-Tested Queries

The compliance smoke tests at `internal/promql/compliance/compliance_test.go` exercise representative supported queries:

- `req_total{job="api"}`
- `rate(req_total[1m])`
- `irate(req_total[1m])`
- `increase(req_total[1m])`
- `avg_over_time(latency[2m])`
- `sum by (job) (req_total{job="api"})`
- `count by (job) (req_total{job="api"})`

## Unsupported (Current)

These are expected to fail parse/eval in current Helios:

- Binary vector/scalar operators:
  - `req_total + req_total`
- `without` aggregations:
  - `sum without (job) (req_total)`
- Subqueries:
  - `req_total[5m:30s]`
- Histogram and advanced functions:
  - `histogram_quantile(...)`
- `offset`, `@` modifiers, and full Prometheus operator/function parity

## Notes

- This is a **smoke** compliance pass for the subset Helios claims, not full Prometheus compliance.
- The subset is intentionally small to keep parser/evaluator behavior predictable and testable while the storage engine matures.
