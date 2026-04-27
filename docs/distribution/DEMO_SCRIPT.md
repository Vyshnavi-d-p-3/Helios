# Demo recording script (~5 minutes)

Use **asciinema** (`asciinema rec helios-demo.cast`), **OBS**, or **Loom**. Prefer **≤ 5 minutes**. Large font (**16pt+**), two terminals visible where noted.

**Before recording**

- Build once: `go build -o helios ./cmd/helios` from repo root  
- Optional: browser tab with the GitHub repo open for a 5-second establishing shot  

---

### [0:00–0:20] Intro

Title card or voice: *“Helios is a replicated time-series database I built in Go — five-minute walkthrough.”*

---

### [0:20–0:50] Single-node

```bash
./helios -data-dir /tmp/helios-demo -http-addr :8080 &
curl -s localhost:8080/healthz
```

*(Optional: pipe through `jq` if installed.)*

---

### [0:50–1:30] Native write + query

```bash
curl -sS -X POST localhost:8080/api/v1/write \
  -H 'Content-Type: application/json' \
  -d '{"samples":[
    {"metric":"http_requests_total","labels":{"status":"200","job":"api"},"ts":1000,"value":100},
    {"metric":"http_requests_total","labels":{"status":"200","job":"api"},"ts":61000,"value":160},
    {"metric":"http_requests_total","labels":{"status":"500","job":"api"},"ts":61000,"value":3}
  ]}'

curl -sS 'localhost:8080/api/v1/query?metric=http_requests_total&partial=1&t=61000'
```

---

### [1:30–2:30] PromQL

```bash
curl -sS 'localhost:8080/api/v1/query?query=rate(http_requests_total[1m])&t=61000'

curl -sS 'localhost:8080/api/v1/query?query=sum%20by%20(status)%20(http_requests_total)&t=61000'
```

Short voice-over: instant vector `rate` over `[1m]` and aggregation by `status`.

---

### [2:30–3:00] Labels

```bash
curl -sS localhost:8080/api/v1/labels
curl -sS localhost:8080/api/v1/label/status/values
```

---

### [3:00–3:45] Anomalies (SSE)

Terminal A:

```bash
curl -N localhost:8080/api/v1/anomalies
```

Terminal B: write a burst of steady points, then one obvious spike — watch Terminal A.

---

### [3:45–4:35] Cluster (Compose)

From **`deployments/`** (adjust if your compose maps different host ports):

```bash
cd deployments
docker compose up -d --build

for p in 8080 8081 8082; do
  curl -sS "http://127.0.0.1:${p}/cluster/leader"; echo
done
```

Kill the leader container (`docker kill helios-1` **or** whichever holds leadership), wait ~2s, re-query `/cluster/leader` on survivors.

---

### [4:35–5:00] Numbers + outro

Flash the README performance section on screen.

Outro: *“Code on GitHub — link in description.”*

**Practice once, record once.** At most **two** takes keeps it human.
