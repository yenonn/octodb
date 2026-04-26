# OctoDB — Roadmap

A rough phased roadmap. Dates are intentionally loose — this is a side project built around a full-time engineering role.

---

## Phase 0 — Foundation (Now)
**Goal:** Get the project scaffolded and the design documented clearly enough to return to after interruptions.

- [x] Define problem statement
- [x] Define core architecture
- [x] Define multi-tenancy model
- [x] Define ACL model
- [x] Choose name (OctoDB)
- [x] Create project documentation (README, ADR, IDEAS)
- [ ] Create GitHub repository (public or private)
- [ ] Register domain (octodb.io / octodb.dev)
- [ ] Write the public thesis post — "Why OctoDB needs to exist"

---

## Phase 1 — Go Prototype
**Goal:** Prove the data model and ACL design work against real OTel data. A working system, not a production system.

### Implementation Sequence

Each step validates one architectural bet before moving to the next. Order matters — later steps depend on confidence gained from earlier ones.

**Step 1: Trace ingestion + Postgres storage**
Start with traces, not metrics or logs. Traces are the hardest signal to store correctly (tree structure, parent/child relationships, exemplar links). If the OTel-native schema thesis works for traces, it works for everything. A working trace store that accepts OTLP is already demo-able.
- [ ] Scaffold Go project (module, directory structure, Makefile)
- [ ] Define `Store` interface — clean boundary between application logic and storage implementation
- [ ] Postgres implementation of `Store` (docker-compose for local dev)
- [ ] OTLP gRPC traces receiver (`ExportTraceServiceRequest`)
- [ ] Span storage with `parent_span_id` relationship preserved
- [ ] Resource deduplication table (JSONB for resource attributes)
- [ ] Query endpoint: `GET /v1/traces/:trace_id` returning full span tree (CTEs for tree reconstruction)

**Step 2: Tenant routing**
Validates the core differentiator — does automatic tenant placement work with real OTel data, or are there edge cases?
- [ ] Tenant resolution from resource attributes
- [ ] Configurable tenant resolution policy (YAML)
- [ ] Data partitioned by tenant (Postgres schemas per tenant, or row-level security)

**Step 3: Metrics + logs ingestion**
Extends the receiver to all three signal types. Validates the "unified signal graph" thesis — are resources actually shareable across signal types?
- [ ] OTLP gRPC receiver extended for metrics and logs
- [ ] Metric datapoint storage with resource foreign key
- [ ] Log record storage with trace context linking
- [ ] Query: get metrics by resource attribute filter + time range
- [ ] Query: get logs by resource attribute filter + time range

**Step 4: Exemplar linking**
The demo moment — the thing that takes two systems today (Prometheus + Tempo) working in a single query.
- [ ] Exemplar storage with metric ↔ trace link
- [ ] Query: get the trace behind a metric datapoint
- [ ] Basic token auth (mTLS deferred to Phase 3)

**Step 5: ACL enforcement**
Validates "ACL in OTel vocabulary" — can attribute-based predicates express real-world access patterns?
- [ ] Principal definition (YAML config)
- [ ] Signal-type permission (read/write per signal)
- [ ] Resource attribute predicate evaluation at query time
- [ ] Cross-tenant read permission

**Step 6: Real workload validation**
The purpose of the entire prototype — learn what the real storage engine needs to do.
- [ ] Deploy OTel demo app (or custom test app), instrument, point at OctoDB
- [ ] Run for several days with realistic signal volume
- [ ] Document observed query patterns, slow queries, attribute cardinality
- [ ] Identify bottlenecks — ingestion throughput? Query latency? Storage volume?
- [ ] Output: storage engine requirements document → feeds Phase 2

### Why Postgres for the Prototype

Postgres is chosen as the prototype backing store — more capable than SQLite, still disposable behind a clean interface:

- **JSONB for OTel attributes** — resource attributes are open-ended key-value pairs with unpredictable cardinality. JSONB handles this natively with GIN indexes. No EAV tables or serialized blobs needed.
- **Real multi-tenancy primitives** — row-level security, schemas-per-tenant, connection pooling. Tenant isolation doesn't need to be invented from scratch.
- **Concurrent ingestion** — multiple services sending OTel data simultaneously. Postgres handles concurrent writes naturally (SQLite is single-writer and would hit lock contention).
- **Rich query capabilities** — CTEs for span tree reconstruction, window functions for time-series aggregation, full-text search for logs. Less application-level query logic needed.
- **Extensions ecosystem** — `pg_trgm` for log search, `timescaledb` if metrics need time-series optimization, `pg_cron` for retention policies.
- **Setup cost is minimal** — `docker compose up` for local dev.

**Trade-off to be aware of:** Postgres is capable enough that the prototype may become "good enough" and reduce urgency for the Rust rewrite. This isn't necessarily bad — many successful systems run on Postgres permanently — but it's worth tracking whether the Rust core (Phase 3) is still needed based on real performance data from Phase 1.

**The important thing stays the same:** the `Store` interface. Postgres is behind the interface, replaceable when Rust comes. Everything above it — OTLP receiver, tenant router, ACL engine, query API — doesn't know or care what's behind the interface.

### Architecture Layer Separation

```
┌─────────────────────────────┐
│  OTLP gRPC receiver         │  ← keeps
│  Tenant router               │  ← keeps
│  ACL engine                  │  ← keeps
│  Query API                   │  ← keeps
│  Exemplar linking logic      │  ← keeps
├─────────────────────────────┤
│  Storage interface (Go)      │  ← keeps (becomes contract for Rust)
├─────────────────────────────┤
│  Postgres implementation     │  ← replace when/if needed
└─────────────────────────────┘
```

---

## Phase 2 — Storage Design
**Goal:** Design the production storage engine based on real query patterns from Phase 1.

- [ ] Benchmark query patterns from Phase 1 prototype
- [ ] Evaluate data structures: Bε-tree vs ART vs hybrid
- [ ] Design WAL format
- [ ] Design columnar layout for metrics
- [ ] Design tree structure for traces
- [ ] Design resource deduplication strategy at scale
- [ ] Design compaction strategy
- [ ] Write storage engine specification document

---

## Phase 3 — Rust Core
**Goal:** Rewrite storage engine in Rust. Go prototype remains as correctness reference and query layer until full migration.

- [ ] OTLP gRPC ingestion in Rust (tonic)
- [ ] WAL implementation in Rust
- [ ] Storage engine implementation (data structures from Phase 2)
- [ ] mTLS ingestion auth (certificate → tenant identity)
- [ ] ACL engine in Rust
- [ ] Query layer in Rust
- [ ] Correctness validation against Go prototype
- [ ] Benchmarking vs ClickHouse, Prometheus TSDB, Grafana Tempo

---

## Phase 4 — Open Source + Consulting
**Goal:** Release publicly, build reputation, generate consulting opportunities.

- [ ] Open source core under permissive license (Apache 2.0 or MIT)
- [ ] Documentation site
- [ ] Helm chart for Kubernetes deployment
- [ ] Integration guides: replace Prometheus, replace Jaeger, replace Loki
- [ ] Blog series: "Building OctoDB" — architecture decisions, lessons learned
- [ ] Conference talk proposals: KubeCon, OTel community day, FOSDEM
- [ ] Consulting offering: OctoDB deployment, migration from existing stack, custom signal pipeline design

---

## Non-Goals (Explicitly Out of Scope for Now)

- Distributed/clustered mode — single node first
- Custom query language — HTTP API first, language layer later
- UI/dashboard — integrate with Grafana via data source plugin, don't build a frontend
- ML/AI features — focus on storage and query correctness first
- Multi-region federation — Octoroute handles this at the network layer

---

## Related Documentation

| Document | Location |
|----------|----------|
| Architecture Decisions | [`../architecture/ADR.md`](../architecture/ADR.md) |
| Ideas & Open Questions | [`IDEAS.md`](IDEAS.md) |
| Write-Path Review | [`../reviews/WRITE_PATH_REVIEW.md`](../reviews/WRITE_PATH_REVIEW.md) |
| Project README | [`../../README.md`](../../README.md) |
