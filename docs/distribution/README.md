# Helios — distribution & career artifacts

These files turn the shipped codebase into publishable/portable outcomes (blog, demo script, posts, CV text, interview prep). They are adapted from `helios_distribution_artifacts.md.pdf`; **personalize voice and numbers** before publishing.

| File | Purpose |
|------|---------|
| [BLOG_POST.md](./BLOG_POST.md) | Long-form article draft (~3k words) |
| [DEMO_SCRIPT.md](./DEMO_SCRIPT.md) | ~5-minute recording beat sheet (asciinema / OBS) |
| [LINKEDIN.md](./LINKEDIN.md) | Short announcement + lesson bullets |
| [SHOW_HN.md](./SHOW_HN.md) | Title, first comment, etiquette |
| [CV_BULLETS.md](./CV_BULLETS.md) | Resume / portfolio variants |
| [INTERVIEW_PREP.md](./INTERVIEW_PREP.md) | Q&A aligned with this repo |

Repo: https://github.com/Vyshnavi-d-p-3/Helios  

---

**Accuracy notes (code vs narrative):**

- Writes over per-metric cardinality limits return **503** with an error body (see `internal/api/api.go`), not HTTP 429.
- Cardinality-related observability includes `helios_ingest_rejected_total{reason="cardinality"}` rather than a per-series gauge name from the PDF draft.
- Anomalies: **GET `/api/v1/anomalies`** (SSE), not `.../stream`.
- Docker Compose in this repo maps HTTP to **8080 / 8081 / 8082** on the host (`deployments/docker-compose.yml`).
