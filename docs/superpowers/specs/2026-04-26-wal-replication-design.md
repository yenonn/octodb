# WAL Replication Design — Multi-Raft Consensus

**Date:** 2026-04-26
**Status:** Draft
**Parent Spec:** WAL Rotation Design (2026-04-26)

---

## Problem

Current WAL design provides:
- ✅ CRC32 corruption detection on read
- ✅ Periodic fsync (configurable 1ms-100ms)
- ✅ Tombstone rotation for cleanup
- ❌ No replication — single node only

Single-node WAL risks:
- Machine crash = data unavailable
- Disk failure = data loss
- No HA for read or write

## Goals

1. **High Availability** — Survive machine/disk failures
2. **Consistency** — No data loss or duplication  
3. **Read Scaling** — Distribute reads across replicas
4. **Auto Failover** — Leader election without manual intervention

## Non-Goals

1. **Geographic distribution** — Datacenter-level (future)
2. **Strong consistency** — Eventual consistency acceptable for observability

## Design

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     WAL Replication                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  [Producer] ── Write ──→ [Leader] ──→ [Follower A]          │
│                           │          ──→ [Follower B]          │
│                           │                                 │
│                    ┌──────┴──────┐                       │
│                    │  Raft Group  │                       │
│                    │   (3 nodes)  │                       │
│                    └─────────────┘                       │
└─────────────────────────────────────────────────────────────┘
```

### Replication Protocol

1. **Leader Election** — Raft consensus (leader = write leader)
2. **Log Replication** — Raft log entries replicated to quorum
3. **Async Replication** — Followers accept writes asynchronously

### WAL-Specific Design

#### State Machine

```
┌────────────┐    ┌────────────┐    ┌────────────┐
│  Leader     │    │  Follower   │    │  Follower   │
│  (write)    │    │  (async)   │    │  (async)   │
├────────────┤    ├────────────┤    ├────────────┤
│ WAL Append │───→│ WAL Append │───→│ WAL Append │
│ fsync      │    │ no fsync   │    │ no fsync   │
│ Commit     │    │ Commit     │    │ Commit     │
└────────────┘    └────────────┘    └────────────┘
```

#### Replication Flow

```
Producer.write(traces)
       │
       ▼
┌──────────────────┐
│  Leader.process  │ (add to local WAL)
│  - Append to WAL │
│  - fsync WAL     │ (for durability)
│  - Add to Raft   │
└──────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  Raft.Replicate(entry)              │
│  - Append to Raft log               │
│  - Send to followers                │
│  - Wait for quorum (2/3)             │
└─────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  Follower.process                    │
│  - Append to WAL (async)             │
│  - Reply ACK                         │
└─────────────────────────────────────┘
```

### Configuration

```go
type ReplicationConfig struct {
    NodeID        string        // Unique node identifier
    RaftAddr      string        // Raft port for consensus
    WALAddr       string        // WAL port for replication
    Peers         []string     // Other node addresses
    QuorumSize    int          // Required replicas (default: 3)
    SyncInterval  time.Duration // WAL sync interval
    
    // NEW: Replication mode
    SyncMode      SyncMode     // async, sync, quorum
}

type SyncMode int
const (
    SyncModeAsync   SyncMode = iota  // No sync, max throughput (default for observability)
    SyncModeOne               = 1   // Sync with 1 follower
    SyncModeQuorum             = 2   // Sync with quorum
)
```

### State Machines

| State | Leader | Follower |
|-------|--------|---------|
| **Receives writes** | ✅ Yes | ❌ No |
| **fsync to disk** | ✅ Yes | ❌ No |
| **Accepts reads** | ✅ Yes | ✅ Yes (local) |
| **Triggers flush** | ✅ Yes | ❌ No |
| **Committed to Raft** | Yes | Yes |

### Failure Handling

| Failure | Detection | Recovery |
|----------|-----------|----------|
| Leader crash | No heartbeat | Followers elect new leader |
| Follower crash | No ACK | Remove from quorum, continue |
| Network partition | No heartbeat | Majority wins |

### Crash Safety

**Crash before replicate:**
- WAL on leader is fsynced
- On restart: read from local WAL + replay from checkpoint

**Crash after replicate:**
- Raft ensures quorum received
- New leader replays from Raft log

**Corruption:**
- CRC32 on WAL protects
- Raft log protects state

## Performance Impact

Replication significantly impacts write throughput. This is a fundamental trade-off.

### Single Node vs Replicated

| Configuration | Latency | Throughput | Max Data Loss |
|---------------|--------|-----------|--------------|
| **Single node** | 2.5 µs | 400,000/sec | 10ms |
| **+1 follower (sync)** | ~500 µs | ~2,000/sec | 0ms |
| **+2 followers (sync)** | ~1000 µs | ~1,000/sec | 0ms |

### Why It's Slower

```
Single node write:
  Write → [WAL] → [fsync] → return
         (2.5 µs)

Replicated write:
  Write → [WAL] → [fsync] → [network] → [follower] → [fsync] → [ACK]
         (2.5 µs)                                (0.5ms)
                              Total: ~500µs
```

### Replication Modes (Trade-offs)

| Mode | Throughput | Latency | Data Safety | Use Case |
|------|------------|---------|-------------|----------|
| **Async** | Same as single (~400K/s) | 2.5 µs | 1 RTT loss | Maximum throughput |
| **1-to-1 sync** | Half (~200K/s) | 500 µs | Immediate | Balanced |
| **Quorum (2/3)** | 1/3 (~130K/s) | 1ms | Immediate | Maximum safety |

### Network Assumptions

- **Same datacenter**: 0.5ms RTT between nodes
- **Cross-region**: 5-10ms RTT (not recommended for WAL)

### Benchmark Recommendations

```go
// Test replication overhead
func BenchmarkReplicatedWrites(b *testing.B) {
    // 2 nodes: ~2000/sec
    // 3 nodes: ~1000/sec
}
```

## Implementation

### Phase 1: Raft Integration

1. Add RAFT library (e.g., hashicorp/raft)
2. Wrap WAL in Raft FSM
3. Add leader election

### Phase 2: WAL Integration

1. Modify WriteTraces to go through Raft
2. Add follower WAL replication
3. Add config for sync vs async

### Phase 3: Auto Failover

1. Add health checks
2. Add leader election
3. Add client failover logic

## Files Changed

| File | Change |
|------|--------|
| `internal/store/store.go` | Add ReplicationConfig, wrap store |
| `internal/store/replication.go` | New: Raft FSM, leader election |
| `internal/store/block2.go` | Modify writes to go through replication |
| `internal/config/config.go` | Add replication config |

## Testing

1. **TestLeaderElection** — Verify leader election on startup
2. **TestReplication** — Verify writes replicate to followers
3. **TestFailover** — Verify client fails over on leader crash
4. **TestRecover** — Verify state recovers after restart

## Read Scaling (Bonus)

Replication enables read scaling across followers!

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Read Distribution                       │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  [Client] ──→ [Leader]  → [Follower A] → [Follower B]   │
│     │              │           │             │              │
│  Reads scale    │        ┌────┴────┐      ┌────┴────┐      │
│              (local)  │  local   │      │  local   │      │
│                       │  reads   │      │  reads   │      │
│                       └─────────┘      └─────────┘      │
└─────────────────────────────────────────────────────────┘
```

### Read Options

| Read From | Latency | Consistency | Scaling |
|----------|---------|-------------|----------|
| **Leader** | 2.5 µs | Strong | ❌ No |
| **Follower (local)** | ~0.5ms | Eventual | ✅ Yes |
| **Closest replica** | ~0.1ms | Eventual | ✅ Yes |

### Use Cases

| Use Case | Read From | Why |
|---------|---------|-----|
| **Dashboard** | Follower | Stale OK, scale reads |
| **Debug trace ID** | Leader | Strong consistency |
| **Time-range query** | Follower | Stale OK, large result |
| **Real-time alert** | Leader | Must see all data |

### Implementation

```go
type ReadRequest struct {
    TenantID string
    Service string
    
    // Read options
    PreferLeader bool      // Force leader read
    AllowStale bool       // Allow follower read
    MaxStaleness time.Duration // How stale is OK
}

func (s *Store) ReadTraces(ctx context.Context, req ReadRequest) ([]*tracepb.ResourceSpans, error) {
    // Read from leader for consistency
    if req.PreferLeader || s.isLeader() {
        return s.readFromLeader(ctx, req)
    }
    
    // Read from follower for scale
    if req.AllowStale {
        return s.readFromFollower(ctx, req)
    }
    
    // Default: leader
    return s.readFromLeader(ctx, req)
}
```

### Benchmark Potential

With 3 replication nodes:
- **Leader reads**: ~400K/sec (local)
- **Follower reads**: ~400K/sec each
- **Total**: ~1.2M/sec across all nodes

This provides **3x read scaling** without additional complexity!

### Comparison with Single Node

| Metric | Single Node | 3 Replicas |
|--------|------------|------------|
| Write throughput | 130K/sec | 130K/sec |
| Read throughput | 400K/sec | 1.2M/sec |
| Availability | 1 node | 3 nodes |
| Data durability | 10ms loss | 0ms loss |

## When NOT to Use Replication

For observability (traces, logs, metrics), consider these alternatives:

1. **Separate write path** — Kafka, Pulsar for durable ingestion
2. **Sidecar replication** — Write to local + async to queue
3. **Tiered storage** — Hot (no replication) → Cold (backed by object storage)

### Comparison

| Approach | Durability | Complexity | Latency |
|----------|------------|------------|---------|
| OctoDB replication | Medium | High | High |
| Kafka/Pulsar | High | Medium | Medium |
| Object storage (S3) | High | Low | High |
| No replication | Low | Low | Low |

**Recommendation for OctoDB:** Use single-node with async WAL sync for max throughput, rely on upstream Kafka/Pulsar for durability.

### Read Replicas vs Full Replication

Full replication enables read scaling — but you can also add read-only replicas without write capability:

| Configuration | Write | Read | Notes |
|---------------|-------|------|-------|
| Full replication | Leader only | All | Current spec |
| Read replicas | Leader only | Read-only | Simpler, same read scale |

**Recommendation:** Add read-only replicas (without Raft) for read scaling — simpler than full replication.

## Trade-offs

| Option | Durability | Throughput | Complexity |
|--------|------------|------------|-----------|
| Sync (current) | ✅ Best | ❌ Low | Low |
| Raft 2/3 quorum | ✅ Good | ⚠️ Medium | High |
| Raft 1/2 quorum | ⚠️ Medium | ✅ Good | Medium |
| Async | ❌ Low | ✅ Best | Low |

Default: **Raft 2/3 quorum** (for consistency)