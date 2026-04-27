# Show HN — submission template

**Title**

`Show HN: Helios – a replicated time-series database in Go`

**URL**

Use your published blog post (preferred). If you do not have one yet, the GitHub repo URL is acceptable but weaker for discussion depth.

**First comment (post immediately after submitting)**

Hi HN — I built **Helios** over the last quarter as a portfolio project: a 3-node replicated TSDB with Gorilla compression, an inverted label index, a PromQL subset, and inline EWMA anomaly detection.

The write-up covers what surprised me — including a Gorilla encoder edge case that took serious time to chase, the label-index decision I almost got wrong, and snapshots that keep Raft practical (SSTable hardlinks + streamed manifest rather than blindly tarring the whole data dir).

This is learning software, not a production SKU. The bugs and tradeoffs were real; throughput and failover numbers in the README come from TSBS-style runs on my machine.

**Code:** https://github.com/Vyshnavi-d-p-3/Helios

Happy to dive into design decisions.

---

**Posting tips**

- Prefer **Tuesday–Thursday morning**, US Pacific, if you care about visibility.
- Do **not** ask for upvotes.
- Reply substantively to top-level comments in the **first ~2 hours**.
- Expect blunt feedback; concise, technical replies earn trust.

**Tags (optional):** `#Go` `#DistributedSystems` `#Observability` `#TSDB`
