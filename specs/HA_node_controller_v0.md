# HA Node-Controller v0 (Active–Passive, Epoch-Fenced)

Status: DRAFT (v0)
Scope: High-availability wrapper for DSPU controller using active–passive leadership with fencing.

## 0. Goals

- Exactly one controller is allowed to perform **mutating** operations at any time.
- Standby controller stays hot for **reads** and may optionally run a normal worker agent ("moonlighting").
- Split-brain is prevented by:
  - an **exclusive leader lock**
  - a monotonically increasing **leader_epoch** used as a fencing token
  - hard-guards on mutating endpoints

Non-goals (v0):
- Active-active scheduling
- Multi-region consensus
- Fairness/quotas/policy scheduling
- Gossip-based arbitration

## 1. Definitions

- **node-controller**: a supervisor process that manages two child processes:
  1) `controller` (API + scheduler)
  2) `agent` (worker, v1-only)
- **role**: `LEADER` or `STANDBY` (optional `DRAINING`)
- **leader lock**: an exclusive lock such that at most one node can hold it at a time.
- **leader_epoch**: a monotonically increasing integer that changes on each leadership acquisition.
- **job_epoch**: monotonically increasing integer per job used to reject stale leases/results.

## 2. Invariants (MUST)

### 2.1 Single-writer rule
Only the node in role `LEADER` may execute mutating operations.

### 2.2 Fencing token rule
Every mutating write MUST be associated with the current `leader_epoch`.
If a process is not the current leader, it MUST NOT be able to write state that will be accepted as current.

### 2.3 Mutual exclusion: leader vs worker
On any physical node:
- If role == `LEADER`, the local worker agent MUST be OFF.
- If role == `STANDBY`, the local worker agent MAY be ON (configurable).

Rationale: prevent a single node from acting as scheduler and worker simultaneously in v0.

### 2.4 Standby is hot but read-only
Standby controller:
- MUST serve read endpoints.
- MUST reject mutating endpoints with `NOT_LEADER`.

## 3. Node-Controller State Machine

### 3.1 States
- `STANDBY`: controller read-only, agent optional ON
- `LEADER`: controller writable, agent OFF
- `DRAINING` (optional): leader stepping down; no new leases; accepts in-flight commits for a short grace window

### 3.2 Transitions

#### STANDBY → LEADER (Acquire leadership)
1) Acquire leader lock (exclusive).
2) Increment `leader_epoch` atomically (monotonic).
3) Start/enable controller in writable mode with that `leader_epoch`.
4) Stop/disable local worker agent.

#### LEADER → DRAINING (optional)
1) Stop issuing new leases.
2) Continue accepting results for leases issued under this leader term for a grace window.
3) Proceed to step down.

#### LEADER/DRAINING → STANDBY (Step down)
1) Disable controller mutating endpoints (read-only).
2) Release leader lock.
3) Start/enable local worker agent (optional).

### 3.3 Crash behavior
If leader crashes, standby may acquire the lock and become leader, incrementing `leader_epoch`.
A restarted old leader MUST be fenced (see Section 4 & 5).

## 4. Controller Endpoint Roles

### 4.1 Read endpoints (allowed on both LEADER and STANDBY)
Examples (non-exhaustive):
- `GET /healthz`
- `GET /v1/agents`
- `GET /v1/jobs`
- `GET /v1/jobs/{id}`
- `GET /v1/events` (SSE)

These MUST include role + epoch metadata (see Section 6).

### 4.2 Mutating endpoints (LEADER only)
Examples (non-exhaustive):
- `POST /v1/jobs`
- `POST /v1/leases`
- `POST /v1/results`
- `DELETE /v1/agents/{agent_name}`

If called on STANDBY, MUST return `409 NOT_LEADER` (see Section 5).

## 5. NOT_LEADER Response Contract (MUST)

When a mutating endpoint is called on a non-leader controller, respond:

- HTTP status: `409 Conflict`
- JSON body:
  - `error`: `"NOT_LEADER"`
  - `leader_id`: string | null
  - `leader_url`: string | null
  - `leader_epoch`: integer | null
  - `node_id`: string
  - `role`: `"STANDBY"`

Notes:
- `leader_*` fields MAY be null if unknown.
- This response MUST be stable for agents/clients to redirect.

## 6. Role/Epoch Introspection (MUST)

Controller MUST provide a read endpoint (or include headers) that exposes:
- `node_id`
- `role` (`LEADER` | `STANDBY` | `DRAINING`)
- `leader_epoch` (current, if known)
- `leader_id` (current, if known)

Minimum required endpoint:
- `GET /role` returning JSON with the above.

## 7. Fencing Rules (MUST)

### 7.1 leader_epoch stamping
All mutations that create or modify durable state MUST record `leader_epoch`:
- job creation/update
- lease creation/update
- result commit
- agent delete/tombstone

### 7.2 Reject stale leader writes
If a request includes `leader_epoch` and it does not match the controller's active leader term, reject:
- HTTP status: `409 Conflict`
- JSON: `error: "STALE_EPOCH"` (alias `"STALE_LEASE"` permitted)

### 7.3 job_epoch correctness
Leases and results MUST carry `job_epoch`.
If a result/lease references a stale `job_epoch`, reject with `STALE_EPOCH`.

## 8. Standby Moonlighting Worker (Optional)

If enabled:
- Standby node-controller starts a normal v1 agent process.
- That agent uses the stable controller endpoint and naturally receives NOT_LEADER if it hits standby.

Recommendation:
- Name the standby worker agent distinctly (e.g., `{node_id}-worker`) to aid observability.

## 9. Conformance Tests (MUST for v0)

The repository MUST include tests that assert:

1) Standby rejects mutating endpoints with 409 NOT_LEADER.
2) Only one node can hold leader lock at a time.
3) leader_epoch increments on leadership acquisition.
4) Old leader cannot commit results after losing leadership (STALE_EPOCH).
5) job_epoch rejects stale work (STALE_EPOCH).

These tests define v0 "done".

## 10. Implementation Notes (Non-normative)

- Lock backend options: Postgres advisory lock, etcd, Consul.
- Prefer simple active–passive first; HA correctness comes from epochs + hard guards.
- Node-controller should supervise child processes and restart on role changes.
