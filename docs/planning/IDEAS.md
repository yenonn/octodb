# OctoDB — Ideas & Scratchpad

A running dump of raw thoughts, open questions, and future directions. Not everything here will ship. That's fine.

---

## Open Questions

### Data Model
- How do we handle OTLP schema evolution? The opentelemetry-proto spec changes — our storage schema needs a versioning strategy.
- What's the right granularity for resource deduplication? Per ingestion batch? Per time window? Global dedup table?
- How do we store sparse attributes efficiently? OTel resource attributes are open-ended — cardinality can explode.
- Should we store raw OTLP bytes alongside the structured model? Useful for replay and debugging but doubles storage.

### Multi-Tenancy
- What happens when a resource has no resolvable tenant attributes? Silent drop? Default tenant? Alert?
- How do we handle tenant migration — moving historical data from one tenant partition to another?
- Should tenants have storage quotas? Ingestion rate limits? How are these enforced?
- Cross-tenant exemplar linking — if a trace crosses tenant boundaries (service mesh call between teams), how do we handle exemplar links that reference spans in a different tenant?

### ACL
- How do we audit ACL evaluations? Every query against sensitive signal data should be logged.
- How do we handle ACL hot reload — changing permissions without restarting the database?
- Should ACL rules be stored inside OctoDB itself or in an external config file? External config is simpler, internal storage is more dynamic.
- OPA/Cedar integration as a future ACL backend — keep the interface clean enough to plug in.

### Query
- What does "trace reconstruction" look like as a query? SELECT * FROM traces WHERE trace_id = X is too simple — you want the full span tree.
- How do we expose service graph topology? The relationship between services is implicit in parent_span_id chains — can we materialize this as a queryable graph?
- Time-series downsampling for long retention — how do we age out high-resolution metrics into lower-resolution aggregates without losing exemplar links?

### Storage Engine
- Bε-tree vs LSM vs ART — need to benchmark against real OTel query patterns before deciding.
- WAL format — should it be OTLP binary directly, or a custom internal format? OTLP binary WAL enables native replay but may be less compact.
- Columnar storage for metrics — metrics are naturally columnar (many datapoints, same schema). Traces are naturally row/tree. Should we have hybrid storage?
- Memory-mapped files for hot data — relevant for low-latency trace lookup.

### Operational
- How does OctoDB cluster? Single-node first, but clustering design should not be an afterthought.
- What's the compaction strategy? LSM-style compaction, or something else?
- How do we expose OctoDB's own health as OTel signals? Dogfooding — OctoDB should be its own first user.

---

## Future Feature Ideas

### OctoDB as a Collector Replacement
If OctoDB natively speaks OTLP and handles routing internally, it could replace the OTel Collector entirely for many pipelines. An embedded processor pipeline (filter, transform, sample) inside OctoDB would make this viable. Lower operational surface for the end user.

### Adaptive Sampling at Ingestion
Tail-based sampling is hard because you need to see the full trace before deciding whether to keep it. OctoDB sees the full trace — it could make tail-based sampling decisions at write time, after all spans arrive, before committing to long-term storage. This would be a significant differentiator over collector-level sampling.

### Service Graph Materialization
Materialize the service dependency graph from trace parent/child relationships in real time. Query: "show me all services that call payment-service and their error rates." This is possible today with Tempo + Prometheus but requires two systems. OctoDB could serve this from a single query.

### Exemplar-Native Alerting
If OctoDB knows which metric datapoints have exemplar links to traces, it could surface the relevant trace automatically when an alert fires. "Your p99 latency just exceeded threshold — here is the trace that caused it." No cross-system correlation needed.

### OTLP-over-WebSocket for Browser Agents
Browser-based OTel SDKs can't use gRPC easily. An OctoDB WebSocket ingestion endpoint that speaks OTLP-JSON would enable frontend observability data to land directly in OctoDB without a collector proxy.

### Time-Travel Queries
Immutable storage + WAL replay = time-travel queries are theoretically possible. "Show me the service graph as it looked at 14:32 yesterday." Useful for incident reconstruction.

### Federated OctoDB
Multiple OctoDB instances (per region, per cluster) with a federation layer that makes cross-region queries feel like a single system. Octoroute handles the network routing between them naturally.

### OctoDB + Octoroute Integration
Octoroute does mTLS cert-based routing at the network layer. OctoDB does cert-based tenant routing at the data layer. Deep integration: Octoroute could inject tenant context headers at the network layer that OctoDB trusts natively, eliminating duplicate auth. Zero-trust end-to-end.

### OTel Arrow Ingestion (OAPT Protocol)
The OTel community has an Arrow-based transport protocol (OAPT) — the `otelarrowexporter` converts OTLP to Arrow columnar format on the wire, achieving ~50% bandwidth reduction over standard OTLP/gRPC with Zstd compression. The existing `otelarrowreceiver` converts Arrow back to row-oriented OTLP pdata, losing the columnar benefit at storage time.

OctoDB could implement its own Arrow receiver that **keeps data in Arrow format** instead of converting back:
```
Phase 1a: Standard OTLP gRPC → Postgres (get it working, any collector can send)
Phase 1b: Add OTel Arrow gRPC → keep as Arrow RecordBatch in memory → batch INSERT Postgres
                                                                     → (later) flush to Parquet for cold storage
```
The protocol already exists and has a sender — OctoDB just needs to implement the receiving end differently.

### Arrow as Internal Representation
Arrow could serve as OctoDB's in-memory lingua franca between ingestion and storage:
```
OTLP protobuf → Arrow RecordBatch (in-memory) → INSERT into Postgres (hot)
                                                → flush to Parquet (cold/archive)
                                                → serve to Grafana (zero-copy)
```
Grafana's data frame system is Arrow under the hood (since Grafana 7.0). If OctoDB uses Arrow internally, the query path from storage to Grafana dashboard has zero serialization overhead. Arrow Flight SQL is also available as a Grafana datasource protocol.

Not all signals benefit equally from columnar storage:
| Signal | Arrow/columnar fit | Why |
|--------|-------------------|-----|
| Metrics | Excellent | Fixed schema, ordered timestamps, many datapoints — ideal case for columnar compression |
| Logs | Good | Timestamp-ordered, repetitive severity/service fields. Variable body is the exception. |
| Traces | Moderate | Tree-structured, relationship-heavy. Point lookups by trace_id need indexes, not column scans. |

This argues for hybrid storage — Arrow/Parquet for metrics+logs, row-oriented (Postgres or custom) for traces. Phase 2 storage design decision.

---

## Competitive Landscape

### Why Existing Backends Need Bridging

The current observability ecosystem fragments across specialized backends, then requires glue to bridge them:

```
ClickHouse  → good at metrics/aggregations, weak at full-text search
OpenSearch   → good at full-text log search, weak at time-series
Tempo/Jaeger → good at traces, separate system entirely
Quesma       → proxy that translates Elasticsearch queries to ClickHouse SQL
Airbyte      → ETL to sync data between systems
```

Three backends plus bridging infrastructure for one observability pipeline. This fragmentation exists because none of these systems were designed around the OTel data model natively. Each is a general-purpose database that OTel data gets translated into.

### ClickHouse as Primary Competitor

ClickHouse (and ClickStack — ClickHouse + OTel ingestion + HyperDX UI) is the most direct competitor. What it does well:
- Columnar storage — excellent compression and scan performance for metrics/logs
- OTel Collector exporter — graduated to beta, production-ready
- Petabyte-scale workloads
- ClickStack bundles logs, metrics, traces in one backend

Where ClickHouse falls short of OctoDB's thesis:
- **OTLP is a translation target, not the schema** — resource attributes get denormalized, structural relationships flatten
- **No native multi-tenancy** — bolted on with WHERE clauses or separate databases
- **No ABAC in OTel vocabulary** — access control is database/table level, not attribute-predicate level
- **No mTLS tenant identity** — not built into the ingestion layer
- **Still needs OTel Collector** — doesn't speak OTLP gRPC natively, the collector exporter translates
- **Exemplar linking is manual** — JOIN across tables, not a first-class concept

ClickHouse is an incredible engine with the wrong abstraction for OTel. OctoDB is the right abstraction with no engine yet. The risk is that ClickHouse/ClickStack is "good enough" for most users.

### Grafana Integration Path

| Phase | Grafana integration |
|-------|-------------------|
| Phase 1 (Postgres) | Built-in Grafana Postgres datasource — works immediately, zero effort |
| Phase 2+ (Arrow) | Custom datasource plugin or Arrow Flight endpoint — zero-copy from storage |

### Validation Opportunity
Send the same OTel data to both OctoDB (Postgres) and ClickHouse. Compare query capability, what's lost in translation, and where OctoDB's native model provides capabilities ClickHouse can't match (exemplar linking, cross-signal queries, tenant isolation).

---

## Technology Notes

### Rust Crates to Evaluate
- `opentelemetry-proto` — Rust bindings for OTLP protobuf
- `tonic` — gRPC server for OTLP ingestion endpoint
- `tokio` — async runtime
- `arrow-rs` — columnar data for metrics storage
- `object_store` — S3/GCS/Azure Blob backend for cold storage
- `fjall` — Rust LSM storage engine (study internals)
- `sled` — embedded Rust database (study internals, mostly abandoned but educational)

### Go Libraries for Prototype
- `go.opentelemetry.io/proto/otlp` — OTLP protobuf definitions
- `google.golang.org/grpc` — gRPC server
- `github.com/jackc/pgx/v5` — Postgres driver (pure Go, performant, JSONB support)
- `github.com/golang-migrate/migrate` — database migration management
- `github.com/apache/arrow-go` — Arrow in-memory format (for Phase 1b Arrow ingestion)
- `github.com/open-telemetry/otel-arrow` — OAPT protocol implementation (Arrow-encoded OTLP)

### Papers to Read
- "The Bw-Tree: A B-tree for New Hardware" — Microsoft Research
- "Designing Access Methods: The RUM Conjecture" — Harvard
- "The Case for Learned Index Structures" — Google Brain
- "COLA: Cache-Oblivious Lookahead Arrays" — MIT
- "Anna: A KVS For Any Scale" — Berkeley

---

## Naming & Branding Notes

**Name:** OctoDB  
**Platform family:** Octo (Octoroute + OctoDB)  
**Tagline:** *OTLP is the schema, not the input format*

**Logo direction:**
- Avoid literal cartoon octopus
- Eight arms as abstract data flows converging into one storage core
- Geometric, clean, infrastructure-aesthetic
- Color: deep teal or slate — close to OTel's blue but distinct

**Domain ideas:**
- octodb.io
- octodb.dev
- getoctodb.io

---

## Inspiration Projects to Study

| Project | Language | What to Learn |
|---------|----------|---------------|
| TigerBeetle | Zig | Storage engine design, financial-grade correctness |
| RisingWave | Rust | Streaming SQL, Rust async patterns at scale |
| Databend | Rust | Cloud-native warehouse, columnar storage in Rust |
| Neon | Rust | Postgres storage layer separation |
| ClickHouse | C++ | Columnar storage, vectorized execution, OTel table schemas, MergeTree compaction |
| ClickStack | C++/TS | ClickHouse + OTel-native ingestion + HyperDX UI — closest competitor to OctoDB's vision |
| Prometheus TSDB | Go | Time-series storage, chunk encoding |
| Grafana Tempo | Go | Trace storage, Parquet backend |
| Quesma | Go | Query translation proxy (Elasticsearch DSL → ClickHouse SQL) — study how bridging works |
| OTel Arrow (OAPT) | Go | Arrow-based OTLP transport, columnar wire format for OTel signals |

---

## Scalability Position

### Current Stance: Single-Node First

Scalability is explicitly **not** a Phase 1–3 concern. Rationale:

- **Don't know what to scale yet.** Bottlenecks — ingestion throughput, query latency, storage volume, attribute cardinality — are unknown until real OTel data hits the prototype. Designing for scale before that is guessing.
- **Premature scaling kills side projects.** Sharding, consensus, replication, partition rebalancing — each is a distributed systems project on its own. Solving those instead of validating the core thesis is a distraction.
- **Proven path.** TigerBeetle, Neon, RisingWave all started single-node. Correctness first, scale later.

### Decisions That Don't Prevent Scaling Later

While not designing for scale, these design choices keep the door open:

- **Tenant isolation by design** — tenant-partitioned data is naturally shardable (one tenant = one shard candidate)
- **Stateless ingestion/query layer** — gRPC receiver and query API are stateless, horizontally scalable in front of storage
- **WAL as ingestion boundary** — decouples ingestion rate from storage write rate, the first scaling lever needed
- **Clean storage interface** — storage engine is behind a Go interface, replaceable without touching upper layers

### When to Revisit

Scaling design becomes relevant when:
1. Phase 1 prototype shows real query patterns and bottleneck data
2. Phase 2 storage engine design needs to account for future sharding primitives
3. A real user deployment exceeds single-node capacity

---

## Session Log

### 2026-04-18 — Founding Session
- Problem statement defined: OTel backends treat OTLP as input format, not schema
- Core thesis: eliminate routing layer, make OTLP the schema
- Multi-tenancy model: dynamic tenant resolution from resource attributes
- ACL model: ABAC expressed in OTel vocabulary
- mTLS as primary auth — consistent with Octoroute
- Kafka replaced by internal WAL; NATS considered as alternative
- Implementation path: Go prototype → storage design → Rust core
- Named: **OctoDB**
- Platform family: **Octo** (Octoroute + OctoDB)

### 2026-04-18 — Implementation Strategy Discussion
- Decided: start with traces first (hardest signal, proves thesis if it works)
- Decided: clean `Store` interface, prototype storage behind it is replaceable
- Decided: skip tenant routing and ACL initially, layer them after storage schema is validated
- Decided: single-node correctness first, scaling decisions deferred until real data informs them
- Defined 6-step implementation sequence for Phase 1 (see [`ROADMAP.md`](ROADMAP.md))
- Key insight: each step validates one architectural bet before moving on
- Key insight: everything above the `Store` interface survives storage replacement
- Changed prototype storage from SQLite to Postgres — JSONB for sparse attributes, concurrent writes, CTEs for span tree queries, row-level security for tenancy, richer query capabilities
- Trade-off noted: Postgres may be "good enough" that Rust rewrite loses urgency — not necessarily bad, but worth tracking

### 2026-04-19 — Arrow, ClickHouse, and Competitive Landscape
- Explored Apache Arrow as storage format — excellent for metrics/logs (time-series, columnar), moderate for traces (point lookups need indexes)
- Identified OTel Arrow protocol (OAPT) — existing exporter/receiver pair, ~50% bandwidth reduction
- Key insight: OctoDB could accept OAPT and keep data in Arrow format instead of converting back to rows
- Arrow as internal representation enables zero-copy path to Grafana (Grafana data frames are Arrow since v7.0)
- Explored ClickHouse/ClickStack as primary competitor — incredible engine, wrong abstraction for OTel
- Explored ClickHouse ↔ OpenSearch/Elasticsearch bridging (Quesma, Airbyte) — evidence that general-purpose backends require glue for observability
- Key insight: the bridging problem is evidence for OctoDB's thesis — purpose-built OTel storage eliminates the need for glue
- Hybrid storage argument strengthened: Arrow/Parquet for metrics+logs, row-oriented for traces
- Two-phase ingestion approach: Phase 1a standard OTLP, Phase 1b add Arrow ingestion
