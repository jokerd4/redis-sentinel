# Redis Sentinel HA Cluster & NestJS Backend

High-availability Redis Sentinel cluster (1 Master, 2 Replicas, 3 Sentinels, Quorum 2) paired with a NestJS backend (Bun runtime) for failover and load testing.

---

## Architecture

- **Topology:** 1 Master, 2 Replicas, 3 Sentinels (`quorum 2`, `down-after-milliseconds 3000`, `failover-timeout 5000`).
- **Persistence:** RDB snapshots + AOF (`appendfsync everysec`, RDB preamble), data in Docker volumes.
- **Client:** `ioredis` via Sentinel:
  - `failoverDetector` drops the old master on `+switch-master`;
  - commands are queued while there is no master and resent after reconnect;
  - commands rejected with `READONLY` are resent;
  - reconnect backoff is 100–500 ms;
  - the client recovers from the ioredis `end` state by itself (a `+switch-master` during a reconnect could leave it closed for good);
  - writes (`SET`) are repeated after a lost connection instead of returning an error;
  - optional `WAIT`: a write is acknowledged only after replicas confirmed it (`REDIS_WAIT_REPLICAS`).
- **Graceful switchover:** `scripts/switchover.ts` changes the master with zero data loss.

```
├── docker-compose.yml                # 1 Master + 2 Replicas + 3 Sentinels + NestJS App
├── Dockerfile                        # Multi-stage Bun build
├── .env                              # Cluster & application configuration
├── scripts/
│   ├── stress/http-load.ts           # HTTP load generator + data verification
│   ├── switchover.ts                 # Graceful switchover (no data loss)
│   ├── seed-redis.ts                 # Fill Redis with test data
│   └── test-requests.ts              # Simple load on GET /time
└── src/
    ├── main.ts                       # Application bootstrap (port 3000)
    ├── infra/redis/
    │   ├── config/redis.conf         # Redis config (replication, persistence, memory)
    │   ├── config/sentinel.conf      # Sentinel config (quorum, timeouts)
    │   ├── redis.service.ts          # Redis facade (commands & connection status)
    │   ├── redis-options.factory.ts  # Client options, retry & failover handling
    │   └── sentinel-events.service.ts# Sentinel Pub/Sub event logger
    ├── time/                         # GET /time
    └── stress/                       # Stress test endpoints
```

---

## API

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/time` | Write current timestamp to Redis and read it back |
| `POST` | `/stress/:runId/:id` | Write key `stress:<runId>:<id>` (no TTL) |
| `GET` | `/stress/:runId/keys` | List stored ids of a run |
| `DELETE` | `/stress/:runId` | Delete keys of a run (`*` = all runs) |

---

## Configuration (`.env`)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `APP_PORT` | `3000` | Application HTTP port |
| `APP_HOST` | `0.0.0.0` | Application bind address |
| `APP_CONTAINER_NAME` | `nest-sentinel-app` | Application container name |
| `REDIS_IMAGE` | `redis:7.2-alpine` | Redis / Sentinel image |
| `REDIS_PORT` | `6379` | Redis port (published for the initial master) |
| `REDIS_MASTER_NAME` | `mymaster` | Sentinel master group name |
| `REDIS_MASTER_HOST` | `redis-master` | Initial master host |
| `SENTINEL_HOST_1..3` | `sentinel-1..3` | Sentinel hosts for the client |
| `SENTINEL_PORT` | `26379` | Sentinel port inside the network |
| `SENTINEL_PORT_1..3` | `26379..26381` | Sentinel ports published on the host |
| `REDIS_COMMAND_TIMEOUT_MS` | `8000` | Command timeout, also the retry window for writes. Must exceed the failover pause: ~5–7 s normally, up to ~15 s after a split Sentinel vote. Tested with `15000` |
| `REDIS_CONNECT_TIMEOUT_MS` | `5000` | Connection timeout |
| `REDIS_MAX_RETRIES` | `null` | Max retries per command (`null` = wait until reconnect) |
| `REDIS_WAIT_REPLICAS` | `0` | Acknowledge a write only after N replicas confirmed it (`WAIT`). `0` = off, async replication |
| `REDIS_WAIT_TIMEOUT_MS` | `1000` | `WAIT` timeout; fewer confirmations within it return an error |
| `REDIS_PASSWORD` / `REDIS_SENTINEL_PASSWORD` | empty | Auth passwords |

Sentinel parameters (quorum, down-after, failover-timeout, parallel-syncs) are set in `src/infra/redis/config/sentinel.conf`.

---

## Quick Start

```bash
docker compose up --build -d        # start cluster
docker compose logs -f nest-app     # application logs
curl http://localhost:3000/time     # check the app
```

---

## Testing Failover

Current master:

```bash
docker exec sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
```

### Master crash

```bash
docker kill <current-master>
```

Expected log sequence:
```text
[RedisService]          [MASTER DOWN] Connection error: read ECONNRESET to redis-master:6379 at ...
[SentinelEventsService] [SENTINEL EVENT: SDOWN] MASTER redis-master:6379 is UNREACHABLE
[SentinelEventsService] [SENTINEL EVENT: ODOWN] Quorum confirmed: Master redis-master:6379 is objectively DOWN
[SentinelEventsService] [SENTINEL EVENT: SWITCH MASTER] Master migrated: redis-master:6379 => redis-replica-1:6379
[RedisService]          [MASTER UP] Reconnected to Master: redis-replica-1:6379 | Total downtime: 5.0s
```

`docker kill` is not restarted by `restart: unless-stopped`, so bring the node back manually. Sentinel then demotes it to replica:

```bash
docker start <old-master>
docker exec <old-master> redis-cli role   # slave
```

### Graceful switchover

```bash
bun run switchover
```

The script pauses writes on the master (`CLIENT PAUSE WRITE`), waits for the replicas to catch up, then runs `SENTINEL FAILOVER`. After that it demotes the old master and unpauses. Do not use plain `SENTINEL FAILOVER` under load: it loses writes.

### Stress test

```bash
bun run stress:http --rate=1000 --duration=60 --label=my-run
```

The script sends `POST /stress/...` at the given rate and checks every acknowledged id in Redis on the master and both replicas. The report is saved to `reports/`. Trigger a failover while it runs (see above) to test it under load.

---

## Stress Test Results

### Throughput (no failover)

| Rate | Errors | Lost | p99 |
|---|---|---|---|
| 1k/s | 0 | 0 | 1.4 ms |
| 2k/s | 0 | 0 | 2.5 ms |
| 5k/s | 0 | 0 | 13.7 ms |
| 10k/s | 0 | 0 | 676 ms |

### Failover under load

| Scenario | Rate | Errors | Lost | Pause |
|---|---|---|---|---|
| Switchover (`bun run switchover`) | 1k / 5k/s | 0 | 0 | 1.8 / 2.4 s |
| Master crash (`docker kill`) | 1k / 5k/s | 0 | 0 | 5.0 / 5.3 s |
| Master hang (`docker pause` 10 s) | 1k / 5k/s | 0 | 0 | 7.1 / 6.5 s |

Plain `SENTINEL FAILOVER` at 1k/s lost 10 927 acknowledged writes without a single error: the old master keeps accepting
writes until Sentinel demotes it. `bun run switchover` closes this window.

### Network faults, `WAIT` on and off

All runs at 1k/s. "Lost" = acknowledged (`200 OK`) writes missing in Redis after the run.

| Scenario | Without `WAIT`: lost / errors | `WAIT 1`: lost / errors |
|---|---|---|
| Master crash, 500 ms replication lag | **346–443** / 0 (3 runs) | **0** / 0–1 316 (6 runs) |
| Network partition 10 s: master cut off from replicas and sentinels, app still reaches it | **4 697–4 855** / 0 (2 runs) | **0** / 257 |
| Master crash, app 50 ms away from Redis | 0 / 0\* | 0 / 0 |
| Switchover, app 50 ms away from Redis | 0 / 0 | 0 / 0 |

- Asynchronous replication loses the writes that the master acknowledged but had not sent to a replica yet.
  The client gets no error. With a fast network this window is under 1 ms, so a crash test usually shows 0 lost, by chance.
- A partition is the worst case: Sentinel promotes a replica while the isolated master keeps acknowledging writes,
  and they are wiped when the network heals.
- `WAIT 1` turns a silent loss into an error: an isolated master cannot get a replica confirmation.
  The errors in the 500 ms lag runs came from the HTTP layer under thousands of concurrent connections and from the ioredis `end` race (fixed since).
- Delay between the app and Redis does not cause loss, only latency.

\* Measured before the ioredis `end` fix: 1 024 errors from that race, not rerun after the fix.

Write latency (p50):

| Network | Without `WAIT` | `WAIT 1` |
|---|---|---|
| Docker network | 0.6 ms | 0.7 ms |
| App 50 ms away from Redis | 51 ms | 127 ms |
| 500 ms replication lag | ~1 ms | ~1.3 s |

`WAIT` blocks the connection, so concurrent writes share one `WAIT`. A write waits for the current `WAIT` and then for its own:
up to two replication round trips.
