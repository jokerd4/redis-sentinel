# Redis Sentinel HA Cluster & NestJS Backend

High-availability Redis Sentinel cluster (1 Master, 2 Replicas, 3 Sentinels, Quorum 2) paired with a NestJS backend (Bun runtime) generating continuous traffic with automated failover monitoring.

---

## Architecture

- **Topology:** 1 Master, 2 Replicas, 3 Sentinels (`quorum 2`).
- **Workload:** Background worker executes 10 requests/sec (5 writes + 5 reads + cleanup).
- **Failover Handling:** Non-breaking failover via `ioredis` with Sentinel Pub/Sub lifecycle tracking and downtime measurement.

```
├── docker-compose.yml             # 1 Master + 2 Replicas + 3 Sentinels + NestJS App
├── Dockerfile                     # Multi-stage Bun build
├── .env                           # Cluster & application configuration
└── src/
    ├── main.ts                   # Application bootstrap (port 3000)
    ├── app.controller.ts         # GET /health endpoint
    ├── infra/redis/
    │   ├── redis.service.ts          # Core Redis facade (commands & status)
    │   ├── redis-options.factory.ts  # Client options & retry backoff strategy
    │   └── sentinel-events.service.ts# Sentinel Pub/Sub event subscriber
    └── worker/
        └── worker.service.ts     # Traffic generator (10 req/s)
```

---

## Configuration (`.env`)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `APP_PORT` | `3000` | Application HTTP port |
| `WORKER_INTERVAL_MS` | `1000` | Batch execution interval (ms) |
| `REDIS_PORT` | `6379` | Redis server port |
| `REDIS_MASTER_NAME` | `mymaster` | Sentinel master group name |
| `SENTINEL_PORT` | `26379` | Sentinel internal port |
| `SENTINEL_QUORUM` | `2` | Quorum required for failover |
| `SENTINEL_DOWN_AFTER_MS` | `3000` | Inactivity threshold before SDOWN (ms) |
| `SENTINEL_FAILOVER_TIMEOUT_MS` | `10000` | Failover execution timeout (ms) |
| `REDIS_COMMAND_TIMEOUT_MS` | `8000` | Command timeout during failover (ms, 0 = disabled) |
| `REDIS_MAX_RETRIES` | `null` | Max command retries (null = unlimited until reconnect) |

---

## Quick Start

### 1. Start Cluster

```bash
docker compose up --build -d
```

### 2. View Application Logs

```bash
docker compose logs -f nest-app
```

### 3. Check Health Endpoint

```bash
curl http://localhost:3000/health
```

---

## Testing Failover

### Step 1: Stop Master

```bash
docker stop redis-master
```

Expected log sequence:
```text
[RedisService]          [MASTER DOWN] Connection lost to redis-master:6379 at ...
[SentinelEventsService] [SENTINEL EVENT: SDOWN] MASTER redis-master:6379 is UNREACHABLE
[SentinelEventsService] [SENTINEL EVENT: ODOWN] Quorum confirmed: Master redis-master:6379 is objectively DOWN
[SentinelEventsService] [SENTINEL EVENT: SWITCH MASTER] Master migrated: redis-master:6379 => redis-replica-1:6379
[RedisService]          [MASTER UP] Reconnected to Master: redis-replica-1:6379 | Total downtime: 6.50s
[WorkerService]         [BATCH #36] [OK] 5 writes + 5 reads + cleanup | Latency: 1226.6ms | Master: redis-replica-1:6379
```

### Step 2: Verify Active Master via Sentinel

```bash
docker compose exec sentinel-1 redis-cli -p 26379 sentinel get-master-addr-by-name mymaster
```

### Step 3: Recover Old Master

```bash
docker start redis-master
```

Verify demotion to replica:
```bash
docker compose exec redis-master redis-cli role
```
Expected role: `slave` (replicated from the active master).
