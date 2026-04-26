# OctoDB

> A multi-tenant OTel-native database. OTLP is the ingestion protocol, the storage schema, and the query model.

---

## What is OctoDB?

OctoDB is a database where OpenTelemetry is a first-class citizen — not an afterthought bolted onto a general-purpose store.

Every existing observability backend takes the same approach: design a database for something else (metrics, logs, analytics), then add an OTLP receiver at the front. The OTel data model gets translated, flattened, or lossy-compressed to fit a schema that was never designed for it.

OctoDB flips this. The OTel data model **is** the schema. Traces, metrics, and logs are not three separate storage problems — they are one unified signal graph, stored and queried as such.

---

## The Problem OctoDB Solves

Modern observability pipelines look like this:

```
App → OTel Collector → Kafka → OTel Collector (consumer) → Backend (Prometheus / Jaeger / Loki)
```

This introduces:
- **Operational weight** — Kafka, multiple collectors, multiple backends, no unified access control
- **Data model loss** — OTLP semantics get stripped at every translation boundary
- **No multi-tenancy** — bolted on at the infrastructure layer, not the data layer
- **No unified ACL** — each backend has its own permission model, none speak OTel natively
- **Latency** — Kafka's batching model works against time-sensitive trace correlation

OctoDB eliminates the pipeline entirely.

```
App → OctoDB (OTLP native ingestion, routing, storage, query)
```

---

## Core Design Principles

### 1. OTLP as the Schema
OctoDB does not translate OTLP into an internal format. The OTel data model — Resources, Scopes, Spans, Metrics, LogRecords, Exemplars — is the storage schema. No translation layer, no semantic loss.

### 2. Eliminate the Routing Layer
OctoDB replaces Kafka/NATS as the routing layer. It exposes native OTLP gRPC ingestion and handles signal fan-out, tenant routing, and buffering internally via its own WAL. No external message broker needed.

### 3. Tenant Identity from Resource Attributes
Tenant identity is derived from OTel Resource attributes at ingestion time — no extra instrumentation required. The signal self-identifies.

```
Resource {
  "service.name": "payment-service",
  "k8s.namespace.name": "team-payments",
  "cloud.account.id": "aws-account-123"
}
→ Tenant: team-payments
```

### 4. ACL in OTel's Vocabulary
Access control is expressed using OTel's own attribute model — attribute-based access control (ABAC) scoped to signal types and resource attributes. No foreign permission model to learn.

### 5. Zero-Trust by Default
mTLS at the ingestion boundary. Certificate identity maps to tenant identity. Consistent with the Octoroute routing philosophy.

### 6. Modern Data Structures
OctoDB is built around data structures designed for its specific access patterns — not B-trees and LSM trees from the 1970s–90s by default. Candidates under evaluation: Bε-trees, ART (Adaptive Radix Tree), learned indexes for read-heavy workloads.

---

## Signal Model

OctoDB stores three OTel signal types as a unified graph:

| Signal | Storage Model | Primary Access Pattern |
|--------|--------------|----------------------|
| Traces | Span tree (structural, not flat columns) | Trace reconstruction by trace_id, service graph |
| Metrics | Time-series with resource deduplication | Range queries, aggregation by resource attributes |
| Logs | Indexed log records with trace context linking | Full-text + attribute filter, exemplar linking |

**Exemplars** link metrics to traces natively — "show me the trace behind this latency spike" is a single index lookup, not a cross-system query.

---

## Multi-Tenancy Model

### Tenancy Granularity
- **Coarse** — tenant per organization
- **Medium** — tenant per team or Kubernetes namespace *(recommended default)*
- **Fine** — tenant per service or deployment environment

### Dynamic Tenancy
Tenancy is resolved dynamically from resource attributes at ingestion time — no static configuration required per service. New services are automatically placed in the correct tenant partition.

### ACL Structure

```yaml
tenant: team-payments
  ingestion:
    signals: [traces, metrics, logs]
    auth: mTLS | token
  query:
    principals:
      - id: grafana-payments
        can_read:
          traces:
            where: service.name in ["payment-service", "checkout-service"]
          metrics:
            where: k8s.namespace.name == "production"

tenant: team-sre
  query:
    principals:
      - id: sre-platform
        can_read: all  # cross-tenant read
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                     OctoDB                          │
│                                                     │
│  ┌─────────────┐    ┌──────────────────────────┐   │
│  │ OTLP gRPC   │    │   Tenant Router          │   │
│  │ Ingestion   │───▶│   (resource attr based)  │   │
│  │ (mTLS)      │    └──────────┬───────────────┘   │
│  └─────────────┘               │                   │
│                                ▼                   │
│  ┌─────────────────────────────────────────────┐   │
│  │              WAL (Write-Ahead Log)          │   │
│  └──────────────────┬──────────────────────────┘   │
│                     │                              │
│          ┌──────────┼──────────┐                   │
│          ▼          ▼          ▼                   │
│      ┌───────┐  ┌───────┐  ┌──────┐               │
│      │Traces │  │Metrics│  │ Logs │               │
│      │ Store │  │ Store │  │Store │               │
│      └───────┘  └───────┘  └──────┘               │
│          │          │          │                   │
│          └──────────┴──────────┘                   │
│                     │                              │
│          ┌──────────▼──────────┐                   │
│          │   Exemplar Index    │                   │
│          │  (metrics ↔ traces) │                   │
│          └─────────────────────┘                   │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │         Query Layer + ACL Enforcement       │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

---

## The Octo Platform

OctoDB is part of a broader platform:

| Project | Layer | Description |
|---------|-------|-------------|
| **Octoroute** | Network plane | mTLS cert-based routing, Kubernetes operator, nginx/njs gateway |
| **OctoDB** | Data plane | OTel-native multi-tenant database, OTLP ingestion, ACL, unified signal storage |

Together: zero-trust, OTel-native, Kubernetes-native infrastructure primitives.

---

## Implementation Plan

| Phase | Status | Description |
|-------|--------|-----------|
| **Phase 1 — Go Prototype** | ✅ **Complete** | Core storage engine (WAL, segmented block store, memtable, bloom filters, index, manifest) implemented in Go. Integration tests validate WAL crash recovery and memtable flush pipelines. Postgres backend dropped in favor of native append-only storage. |
| **Phase 2 — Storage Design** | 🔄 **In Progress** | Native append-only format with two-level index proven. Query layer (gRPC / HTTP) scaffolded with Trace, Log, and Metric read paths. OTLP receiver and Block 2 query tests under active development. |
| **Phase 3 — Rust Core** | ⏳ **Pending** | Production storage engine rewrite for memory safety and performance. Go codebase serves as correctness reference. |
| **Phase 4 — Open Source + Consulting** | ⏳ **Pending** | Public release and migration-consulting practice. |

### Completed Components
- ✅ **WAL** (`internal/wal`) — append-only log with fsync boundary
- ✅ **Segment Store** (`internal/segment`) — structured block persistence
- ✅ **Memtable** (`internal/memtable`) — in-memory write buffer with flush pipeline
- ✅ **Bloom Filters** (`internal/bloom`) — fast segment membership tests
- ✅ **Index** (`internal/index`) — trace ID → segment locator
- ✅ **Manifest** (`internal/manifest`) — SST metadata manager
- ✅ **Storage Orchestrator** (`internal/store`) — unified WAL + memtable + segment lifecycle
- ✅ **Config Loader** (`internal/config`) — YAML-based topology configuration
- ✅ **Replay Checker** (`cmd/replay_check`) — WAL verification tool
- ✅ **Integration Tests** (`tests/integration`) — Block 1 (WAL crash recovery) & Block 2 (memtable flush + query)

### In Progress
- 🚧 **OTLP Query Layer** (`internal/server`) — gRPC + HTTP handlers for Trace, Log, and Metric reads
- 🚧 **ACL Enforcement** — ABAC rules engine tied to resource attributes

### Pending
- ⏳ OTLP Ingestion Receiver (native gRPC collector)
- ⏳ Block 2 query predicate push-down
- ⏳ Rust core rewrite
- ⏳ Production packaging & Helm charts

---

## Status

> ⚙️ **Active Prototype — Core Storage Engine Implemented**
> The WAL, segmented block store, memtable, bloom filters, index, and manifest layers are functional. Integration tests cover WAL crash recovery and memtable flush pipelines. The OTLP query layer is under active development.

---

## Quick Start

```bash
# Build the server and test client
make build

# Run integration tests
make test-integration

# Run specific test blocks
make test-block1   # WAL crash recovery
make test-block2   # Memtable flush + query
```

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Follow the existing `internal/` package conventions
4. Keep ADRs updated for architecture changes
5. Submit a PR — reference the `docs/architecture/ADR.md` and `docs/reviews/WRITE_PATH_REVIEW.md`

---

## Project Structure

```
octodb/
├── cmd/
│   ├── octodb/          # Main database server
│   ├── replay_check/    # WAL replay verification tool
│   └── testclient/     # Integration test client
├── internal/
│   ├── bloom/           # Bloom filter implementation
│   ├── config/          # YAML configuration loader
│   ├── index/           # Trace index (segment locator)
│   ├── manifest/        # SST manifest manager
│   ├── memtable/       # In-memory write buffer
│   ├── segment/        # Segmented block store
│   ├── server/         # gRPC + HTTP query layer
│   ├── store/          # Unified storage orchestrator
│   └── wal/            # Write-ahead Log
├── pkg/
│   └── otelutil/       # OpenTelemetry helpers
├── tests/
│   └── integration/    # End-to-end integration tests
└── docs/               # Architecture docs & roadmap
```

---

## Author

Yen-Onn — Senior Platform Engineer, SAP Singapore  
Expertise: Go, Kubernetes, OpenTelemetry, Kafka, mTLS, distributed systems  
Related work: Octoroute (mTLS cert-based Kubernetes routing operator)

---

## Related Documentation

| Document | Location |
|----------|----------|
| Architecture Decisions | [`docs/architecture/ADR.md`](docs/architecture/ADR.md) |
| Ideas & Open Questions | [`docs/planning/IDEAS.md`](docs/planning/IDEAS.md) |
| Roadmap | [`docs/planning/ROADMAP.md`](docs/planning/ROADMAP.md) |
| Write-Path Review | [`docs/reviews/WRITE_PATH_REVIEW.md`](docs/reviews/WRITE_PATH_REVIEW.md) |
