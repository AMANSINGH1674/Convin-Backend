# SOLUTION

## What was broken, and why

**1. Race condition in `Cache.Record()` (stats drift)**
`Record()` read and wrote the shared map without acquiring the mutex, while `Get()` properly used `RLock`. Under concurrent webhook deliveries this caused data races — corrupting in-memory call counts and making them drift higher than reality.

**Fix:** Added `c.mu.Lock()` / `defer c.mu.Unlock()` to `Record()`.

**2. Recording processing used the HTTP request context (recordings never marked processed)**
`Ingest()` launched `processRecording` in a goroutine but passed `ctx` — the HTTP request's context. The handler returned 200 immediately, cancelling that context. The `MarkRecordingProcessed` DB call then failed with "context canceled", and the error was silently swallowed (`// TODO: handle`).

**Fix:** Used `context.WithoutCancel(ctx)` so the goroutine survives the request lifecycle, and log errors instead of swallowing them.

**3. No WaitGroup for in-flight goroutines (data lost on deploy)**
The fire-and-forget `go func()` for recording processing had no tracking. On `SIGTERM` (deploy), `srv.Shutdown()` drained HTTP connections but the recording goroutines were killed mid-flight.

**Fix:** Added a `sync.WaitGroup` to `Service` and a `Shutdown(ctx)` method that drains in-flight goroutines before the process exits.

**4. Check-then-insert race on deduplication (duplicate events + double-counted stats)**
`EventExists()` and `InsertEvent()` were separate non-atomic calls. Two concurrent redeliveries of the same `event_id` could both pass the existence check, both insert, and both increment `account_stats`. The `events` table only had a non-unique index, so Postgres didn't enforce uniqueness either.

**Fix:** Added a `UNIQUE` constraint on `events.event_id` and changed `InsertEvent` to `INSERT ... ON CONFLICT (event_id) DO NOTHING`, returning whether the row was actually inserted. The separate `EventExists` check is kept for reads but the write path is now atomic.

## Why Postgres for deduplication (over Redis)

I considered three approaches:

| Approach | Pros | Cons |
|---|---|---|
| **Postgres UNIQUE + ON CONFLICT** ✅ | Atomic, durable, single round-trip, zero race window, no TTL expiry risk | Slightly slower than Redis for pure lookups |
| Redis SET NX with TTL | Fast, simple | Not durable (data lost on restart), TTL must be tuned, adds a second system of record |
| Redis + Postgres (belt-and-suspenders) | Fast check + durable fallback | Complexity, two systems to reason about, Redis miss still needs Postgres fallback |

I chose Postgres because the `events` table already exists as the durable record of deliveries. `INSERT ... ON CONFLICT` is a single atomic operation — no race window, no TTL to tune, no warm-up after restart, and no second source of truth to keep in sync.

## What I would change at 10,000 webhooks/second

- **Batch inserts**: Buffer incoming events and flush in batches via `COPY` or multi-row `INSERT` to reduce per-event round-trip overhead.
- **Redis fast-path dedup**: Use `SET NX` with a short TTL (e.g. 1 hour) as a fast in-memory filter before hitting Postgres. The UNIQUE constraint remains the durable guarantee.
- **Partitioned events table**: Partition `events` by time (e.g. daily) so the unique index stays small and old data can be cheaply dropped.
- **Connection pooling**: Use PgBouncer or increase pool size; at 10k/s the default 20 connections would saturate.
- **Async stats update**: Decouple `IncrementAccountStats` from the hot path — publish to a queue (Redis Streams / Kafka) and aggregate asynchronously.
- **Horizontal scaling**: Multiple service instances behind a load balancer; idempotency is already safe because the UNIQUE constraint is in the database.