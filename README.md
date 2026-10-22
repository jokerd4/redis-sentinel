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
  - reconnect backoff is 100–500 ms.
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
| `REDIS_COMMAND_TIMEOUT_MS` | `8000` | Command timeout; must exceed the failover pause (~7 s), tested with `15000` |
| `REDIS_CONNECT_TIMEOUT_MS` | `5000` | Connection timeout |
| `REDIS_MAX_RETRIES` | `null` | Max retries per command (`null` = wait until reconnect) |
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
