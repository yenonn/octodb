# OctoDB — Architecture Decision Records (ADR)

A running log of key architectural decisions, the context behind them, and the alternatives considered.

---

## ADR-001: OTLP as Native Storage Schema

**Date:** 2026-04  
**Status:** Accepted

### Context
Every existing observability backend translates OTLP into an internal format at ingestion. This causes semantic loss — exemplar links break, trace context flattens, resource attributes get denormalized inconsistently.

### Decision
OctoDB stores the OTel data model directly. Resources, Scopes, Spans, MetricDataPoints, LogRecords are stored as structural entities, not translated into a foreign schema.

### Consequences
- No translation layer — ingestion is faster and lossless
- Query model must be designed around OTel semantics, not SQL tables
- Tighter coupling to OTel spec evolution — must track opentelemetry-proto changes
- Unique differentiator — no other database does this

### Alternatives Considered
- ClickHouse schema (columnar, fast, but OTel is a translation target not native)
- Parquet-backed (Tempo approach, good for traces, poor for unified signal model)

---

## ADR-002: Eliminate External Routing Layer

**Date:** 2026-04  
**Status:** Accepted

### Context
Standard OTel pipelines use Kafka or NATS as a routing/buffering layer between collectors and backends. This adds operational complexity, latency, and a system that doesn't understand OTel semantics.

### Decision
OctoDB exposes native OTLP gRPC ingestion and handles routing internally. A built-in WAL provides durability and buffering. No external message broker required.

### Consequences
- Simpler deployment topology — one system instead of three
- WAL design is now a critical internal component
- Must handle backpressure natively
- Reduces operational surface area significantly for the end user

### Alternatives Considered
- **Kafka** — high throughput ceiling but disproportionate operational weight, partition rigidity, latency mismatch
- **NATS JetStream** — better fit than Kafka (subject-based routing maps to OTel signal types naturally, lower latency, simpler ops) but still an external dependency. NATS remains a valid alternative for users who prefer it.

### NATS vs Internal WAL Trade-offs

| | NATS JetStream | OctoDB Internal WAL |
|---|---|---|
| Operational complexity | Low | Zero (built-in) |
| Throughput | High | Sufficient for most |
| Routing flexibility | Subject-based | Resource attribute-based |
| OTel semantic awareness | None | Native |
| Replay capability | Yes | Yes |

---

## ADR-003: Dynamic Tenant Resolution from Resource Attributes

**Date:** 2026-04  
**Status:** Accepted

### Context
Multi-tenant observability systems typically require explicit tenant identification via API keys or HTTP headers — extra instrumentation burden on the application side.

### Decision
Tenant identity is resolved dynamically from OTel Resource attributes at ingestion time. A configurable resolution policy maps resource attributes to tenant identities.

```yaml
tenant_resolution:
  strategy: attribute_based
  primary: k8s.namespace.name
  fallback: service.namespace
  default: unassigned
```

### Consequences
- Zero additional instrumentation required from application teams
- New services automatically land in the correct tenant partition
- Resolution policy must be configurable and well-documented
- Edge cases: services with missing or ambiguous attributes need a defined fallback

### Alternatives Considered
- Static API key per tenant (simpler but requires manual registration per service)
- HTTP header injection at collector level (works but requires collector config per tenant)

---

## ADR-008: Per-Tenant Signal Bundles with Lifecycle Management

**Date:** 2026-04-29  
**Status:** Accepted

### Context
Current architecture shares a single WAL + memtable + segment directory for all tenants. While tenant resolution (ADR-003) groups data at ingestion, the storage layer still mixes tenants in the same sorted stream. This creates segment-level pruning inefficiency and weakens crash isolation.

### Decision  
Each tenant gets an independent `signalBundle` per signal type (traces, logs, metrics). Bundles transition through Active / Parked / Cold lifecycle states to constrain memory and file descriptors.

### Consequences
- True physical isolation — drop tenant = `rm -rf dataDir/tenant/{id}/`
- Segment pruning is perfect: a segment contains exactly one tenant
- Bounded resources: only active tenant bundles hold open files + memtables
- Per-tenant backup, restore, and migration are simple directory operations
- Crash of one tenant's bundle doesn't affect others

### Full Spec
See [`docs/superpowers/specs/2026-04-29-per-tenant-segment-isolation.md`](../superpowers/specs/2026-04-29-per-tenant-segment-isolation.md)

---

### Context
Existing observability backends either have no ACL, coarse tenant isolation, or require mapping OTel attributes to a foreign permission model. None express permissions in OTel's own vocabulary.

### Decision
OctoDB ACL is expressed using OTel resource attribute predicates. Permissions are scoped to signal type and resource attribute filters.

```yaml
principal: grafana-payments
can_read:
  traces:
    where: service.name in ["payment-service", "checkout-service"]
  metrics:
    where:
      - k8s.namespace.name == "production"
      - deployment.environment == "prod"
```

### Consequences
- Platform teams can express fine-grained access without learning a foreign model
- ACL evaluation happens at query time against OTel attribute values
- Performance consideration: attribute predicate evaluation must be fast — index resource attributes used in ACL rules
- Cross-tenant read must be an explicit, audited permission

### Alternatives Considered
- RBAC (role-based) — simpler but too coarse for OTel's rich attribute model
- External policy engine (OPA/Cedar) — flexible but adds external dependency; may integrate later

---

## ADR-005: mTLS as Primary Ingestion Auth

**Date:** 2026-04  
**Status:** Accepted

### Context
OctoDB's sister project Octoroute already uses mTLS cert-based routing for network-layer identity. Consistent zero-trust posture should extend to the data layer.

### Decision
mTLS is the primary authentication mechanism at the OTLP gRPC ingestion boundary. Certificate identity (Subject DN or SPIFFE SVID) maps to tenant identity and ingestion permissions. Token-based auth supported as secondary mechanism for environments where mTLS is not feasible.

### Consequences
- Zero-trust by default — consistent with Octoroute's philosophy
- Certificate lifecycle management is the operator's responsibility
- Natural fit with cert-manager and SPIFFE/SPIRE in Kubernetes environments
- Reuses Octoroute PKI infrastructure directly

### Alternatives Considered
- Token-only auth (simpler but weaker security posture)
- OAuth2/OIDC (flexible but complex for machine-to-machine ingestion)

---

## ADR-006: Go Prototype First, Rust Core Later

**Date:** 2026-04  
**Status:** Accepted

### Context
Primary author is fluent in Go, learning Rust. The OTel data model and ACL design need validation before committing to a storage engine implementation language.

### Decision
Phase 1 prototype in Go. Validate data model, tenant routing, ACL, and query API design. Use Postgres as the backing store for the prototype — chosen over SQLite for JSONB attribute support, concurrent writes, CTEs, and built-in multi-tenancy primitives (row-level security). Still behind a clean `Store` interface — replaceable if/when Rust core is needed. Rewrite the storage core in Rust in Phase 3, using the Go prototype as a correctness reference.

### Consequences
- Faster time to a working system
- Rust learning curve tackled after domain is well-understood — only one hard thing at a time
- Go prototype may attract early contributors before Rust rewrite
- Risk: prototype decisions may inadvertently constrain Rust design — document prototype limitations explicitly

### Alternatives Considered
- **SQLite** — simpler (single file, zero config, pure Go driver) but single-writer lock contention, no JSONB, no row-level security, weak query capabilities for span trees and time-series
- **DuckDB** — better for columnar metrics but adds CGo dependency, complicates builds
- Start directly in Rust (correct long-term but blocks validation behind a steep learning curve)
- Stay in Go permanently (viable but limits performance ceiling and enterprise credibility)

### Risk
Postgres may be capable enough that the Rust rewrite loses urgency. This is acceptable — many production systems run on Postgres. Revisit the Rust decision based on real performance data from Phase 1.

---

## ADR-007: Unified Signal Query Model

**Date:** 2026-04  
**Status:** Under Discussion

### Context
Current observability ecosystem has three separate query languages: PromQL (metrics), TraceQL (traces), LogQL (logs). Requiring users to learn three languages for one backend is poor UX. Designing a unified query language is research-level hard.

### Options Under Consideration

**Option A: Support all three natively**
Implement PromQL, TraceQL, and LogQL compatibility. Familiar to existing users, maximizes migration ease. High implementation cost.

**Option B: Single OTel-native query language**
Design a new query language around OTel's data model — signal-agnostic, attribute-predicate-based. Novel and differentiated but requires user re-learning.

**Option C: SQL with OTel extensions**
SQL is universally known. Extend with OTel-specific functions (trace_tree(), exemplar_link(), resource_filter()). Pragmatic, accessible, less novel.

**Option D: Start with HTTP/REST API, defer query language**
Expose a structured JSON query API first. Add language layer later once query patterns are understood from real usage.

### Current Leaning
Option D for Phase 1 prototype, evaluate Option C or A for Phase 3 based on user feedback.

### Status
Decision deferred pending Phase 1 prototype learnings.

---

## Related Documentation

| Document | Location |
|----------|----------|
| Ideas & Open Questions | [`../planning/IDEAS.md`](../planning/IDEAS.md) |
| Roadmap | [`../planning/ROADMAP.md`](../planning/ROADMAP.md) |
| Write-Path Review | [`../reviews/WRITE_PATH_REVIEW.md`](../reviews/WRITE_PATH_REVIEW.md) |
| Project README | [`../../README.md`](../../README.md) |
