# Webhook Ingestion Service (`webhook-ingest`)

A robust, highly concurrent, and idempotent Go microservice designed to ingest telephony call-completion webhooks, persist raw events, track per-account call aggregates, and asynchronously process call audio recordings with graceful lifecycle management.

---

## 🌟 Architectural & Engineering Highlights

1. **Strict Idempotency & Zero Double-Counting**
   - Telephony providers deliver webhooks with **at-least-once** semantics.
   - Enforces database-level uniqueness on `events.event_id` paired with atomic `INSERT ... ON CONFLICT (event_id) DO NOTHING`.
   - Eliminates check-then-insert race conditions on concurrent retries, guaranteeing that duplicates are safely ignored without inflating `account_stats`.

2. **Thread-Safe Hot-Path In-Memory Analytics**
   - In-memory cache (`stats.Cache`) synchronized via read-write mutexes (`sync.RWMutex`) to guarantee safe concurrent reads and writes without data races.
   - Synchronously backed by PostgreSQL (`account_stats`) as the durable source of truth.

3. **Resilient Asynchronous Background Workers**
   - Asynchronous call recording pipeline (`processRecording`) decoupled from the incoming HTTP request.
   - Uses `context.WithoutCancel(ctx)` to ensure that background workers survive the completion and termination of the HTTP request lifecycle.
   - Structured error logging with `log/slog` for operational observability.

4. **Zero-Downtime Graceful Shutdown Drain**
   - Coordinated process termination using `sync.WaitGroup` and `Service.Shutdown(ctx)`.
   - Listens for `SIGINT` / `SIGTERM` to allow in-flight recording downloads and database operations to finish before application exit.

---

## 🏗️ System Architecture & Data Flow

```
[ Telephony Provider ]
         │
         ▼  (POST /webhooks/calls)
┌─────────────────────────────────────────────────────────┐
│ HTTP API Router & Handler (internal/httpapi)           │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│ Ingestion Service (internal/ingest)                     │
│  - Atomic Postgres Dedup (events)                       │
│  - Call Record Upsert (calls)                           │
│  - Durable Aggregate Increment (account_stats)          │
│  - Thread-Safe Memory Cache (internal/stats)            │
│  - Asynchronous Background Worker (sync.WaitGroup)      │
└──────────────┬──────────────────────────┬───────────────┘
               │                          │
               ▼ (Durable Storage)        ▼ (Detached Goroutine)
     ┌──────────────────┐       ┌───────────────────────────┐
     │ PostgreSQL (v16) │       │ Recording Worker (~50ms)  │
     │ - events         │       │ - MarkRecordingProcessed  │
     │ - calls          │       └───────────────────────────┘
     │ - account_stats  │
     └──────────────────┘
```

---

## 🚀 Getting Started

### Prerequisites
- **Go**: 1.25+
- **Docker & Docker Compose**

### Running with Docker Compose
```bash
# Start Postgres, Redis, and the service
docker compose up -d --build

# Verify health status
curl http://localhost:8080/healthz
# Output: ok
```

### Running Test Suite & Race Detector
```bash
# Run all tests with race detector enabled
go test -race -v ./...
```

### Teardown & Reset
```bash
# Wipe containers, volumes, and reapply migrations from clean state
make reset
```

---

## 📡 API Reference

### 1. Ingest Call Webhook
- **Endpoint**: `POST /webhooks/calls`
- **Headers**: `Content-Type: application/json`

**Request Payload:**
```json
{
  "event_id": "evt_01H8XK2M9P",
  "call_id": "call_9f2ab31c",
  "account_id": "acc_123",
  "status": "completed",
  "duration_sec": 143,
  "recording_url": "https://recordings.example.com/9f2ab31c.wav",
  "occurred_at": "2026-08-13T09:12:00Z"
}
```
*Note: `status` must be one of `completed`, `failed`, or `no_answer`.*

**Responses:**
- `200 OK`: `{"status":"accepted"}`
- `400 Bad Request`: `{"error":"<validation error message>"}`
- `500 Internal Server Error`: `{"error":"ingest failed"}`

---

### 2. Get Account Call Statistics
- **Endpoint**: `GET /accounts/{account_id}/stats`

**Response (`200 OK`):**
```json
{
  "account_id": "acc_123",
  "call_count": 1,
  "total_duration_sec": 143
}
```

---

### 3. Service Health Check
- **Endpoint**: `GET /healthz`

**Response (`200 OK`):**
```
ok
```

---

## 📂 Project Structure

```
├── cmd/
│   └── server/          # Application entrypoint, graceful shutdown & wiring
├── internal/
│   ├── config/          # Environment configuration loader
│   ├── httpapi/         # HTTP routes, request decoding & responses
│   ├── ingest/          # Webhook ingestion service & async worker pipeline
│   ├── redisclient/     # Redis client connection setup
│   ├── stats/           # Thread-safe in-memory cache with sync.RWMutex
│   ├── store/           # PostgreSQL repository (pgxpool) with atomic queries
│   └── testutil/        # Test harnesses and isolated per-test database fixtures
├── migrations/          # SQL migrations (001_init.sql, 002_unique_event_id.sql)
├── Dockerfile           # Multi-stage production container build
├── docker-compose.yml   # Multi-service stack (App, PostgreSQL 16, Redis 7)
├── Makefile             # Automation targets (up, down, reset, test, logs)
└── SOLUTION.md          # Engineering documentation & 10k/sec scaling roadmap
```
