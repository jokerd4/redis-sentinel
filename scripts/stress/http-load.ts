import Redis from 'ioredis';
import { mkdirSync, writeFileSync } from 'node:fs';

/**
 * HTTP stress test for the Redis Sentinel write path (POST /stress/:runId/:id).
 *
 * Every request writes a unique key. After the run the script compares
 * acknowledged ids with the keys that actually exist in Redis.
 *
 * Usage:
 *   bun scripts/stress/http-load.ts --rate=1000 --duration=30 --label=baseline-1k
 *   bun scripts/stress/http-load.ts --rate=1000 --duration=0 --label=switchover   # until Ctrl+C
 *
 * Options:
 *   --rate=N            target requests per second (default 1000)
 *   --duration=S        seconds to run, 0 = until Ctrl+C (default 30)
 *   --label=NAME        scenario name, used in runId and report file name
 *   --url=URL           app base URL (default http://localhost:3000)
 *   --max-inflight=N    max concurrent requests; ticks over the cap are skipped (default 10000)
 *   --timeout=MS        per-request timeout (default 60000)
 *   --connections=N     max open HTTP connections, 1..65535 (Bun default 256)
 *   --sentinel=HOST:PORT  sentinel for event timeline (default localhost:26379)
 */

// Bun's fetch caps concurrent requests (= open connections) at 256 unless
// BUN_CONFIG_MAX_HTTP_REQUESTS is set at startup, so --connections re-execs with it.
// Requests over the cap wait inside the generator, like behind a load balancer pool.
const connectionsArg = process.argv.find((a) => a.startsWith('--connections='))?.split('=')[1];
if (connectionsArg && process.env.BUN_CONFIG_MAX_HTTP_REQUESTS !== connectionsArg) {
  const child = Bun.spawn([process.execPath, ...process.argv.slice(1)], {
    env: { ...process.env, BUN_CONFIG_MAX_HTTP_REQUESTS: connectionsArg },
    stdio: ['inherit', 'inherit', 'inherit'],
  });
  process.on('SIGINT', () => child.kill('SIGINT'));
  process.on('SIGTERM', () => child.kill('SIGTERM'));
  process.exit(await child.exited);
}

const arg = (name: string, fallback: string): string =>
  process.argv.find((a) => a.startsWith(`--${name}=`))?.split('=').slice(1).join('=') ?? fallback;

const rate = Math.max(1, Number(arg('rate', '1000')));
const durationSec = Math.max(0, Number(arg('duration', '30')));
const label = arg('label', 'run').replace(/[^A-Za-z0-9_-]/g, '-');
const baseUrl = arg('url', 'http://localhost:3000').replace(/\/$/, '');
const maxInflight = Math.max(1, Number(arg('max-inflight', '10000')));
const timeoutMs = Math.max(1, Number(arg('timeout', '60000')));
const connections = Number(process.env.BUN_CONFIG_MAX_HTTP_REQUESTS || 256);
const [sentinelHost, sentinelPort] = arg('sentinel', 'localhost:26379').split(':');

const startedAtIso = new Date().toISOString();
const runId = `${label}-${startedAtIso.replace(/[-:]/g, '').replace('T', '-').slice(0, 15)}`;
const REDIS_CONTAINERS = ['redis-master', 'redis-replica-1', 'redis-replica-2'];
const APP_CONTAINER = process.env.APP_CONTAINER_NAME || 'nest-sentinel-app';

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
interface SecondBucket {
  sent: number;
  skipped: number;
  ok: number;
  err: number;
  latencies: number[];
  maxInflight: number;
  genCpuPct: number;
}

interface ErrorGroup {
  count: number;
  firstAtSec: number;
  lastAtSec: number;
}

let t0 = performance.now();
const nowSec = (): number => (performance.now() - t0) / 1000;

const buckets: SecondBucket[] = [];
const bucket = (sec: number): SecondBucket => {
  const i = Math.floor(sec);
  while (buckets.length <= i) {
    buckets.push({ sent: 0, skipped: 0, ok: 0, err: 0, latencies: [], maxInflight: 0, genCpuPct: 0 });
  }
  return buckets[i];
};

const ackedIds: number[] = [];
const failedIds: number[] = [];
const okCompletions: number[] = []; // completion time (s) of successful requests
const allLatencies: number[] = [];
const errorGroups = new Map<string, ErrorGroup>();
const sentinelEvents: { atSec: number; channel: string; message: string }[] = [];
const dockerSamples: { atSec: number; name: string; cpuPct: number; memMiB: number }[] = [];

let nextId = 0;
let sent = 0;
let skipped = 0;
let inflight = 0;
let stopping = false;

// ---------------------------------------------------------------------------
// Load generation
// ---------------------------------------------------------------------------
function recordError(key: string, atSec: number): void {
  const group = errorGroups.get(key);
  if (group) {
    group.count++;
    group.lastAtSec = atSec;
  } else {
    errorGroups.set(key, { count: 1, firstAtSec: atSec, lastAtSec: atSec });
  }
}

async function fire(id: number): Promise<void> {
  const start = performance.now();
  inflight++;
  const b = bucket(nowSec());
  b.sent++;
  b.maxInflight = Math.max(b.maxInflight, inflight);

  let errorKey: string | null = null;
  try {
    const res = await fetch(`${baseUrl}/stress/${runId}/${id}`, {
      method: 'POST',
      signal: AbortSignal.timeout(timeoutMs),
    });
    const body = await res.text();
    if (!res.ok) errorKey = `HTTP ${res.status}: ${body.slice(0, 200)}`;
  } catch (error) {
    const e = error as Error;
    errorKey = `NETWORK ${e.name}: ${e.message}`.slice(0, 200);
  } finally {
    inflight--;
  }

  const end = performance.now();
  const latency = end - start;
  const endSec = (end - t0) / 1000;
  const done = bucket(endSec);
  done.latencies.push(latency);
  allLatencies.push(latency);

  if (errorKey) {
    done.err++;
    failedIds.push(id);
    recordError(errorKey, endSec);
  } else {
    done.ok++;
    ackedIds.push(id);
    okCompletions.push(endSec);
  }
}

function tick(): void {
  if (stopping) return;
  const elapsed = nowSec();
  if (durationSec > 0 && elapsed >= durationSec) {
    void stop();
    return;
  }
  const target = Math.floor(rate * elapsed);
  while (sent < target) {
    sent++;
    if (inflight >= maxInflight) {
      skipped++;
      bucket(elapsed).skipped++;
      continue;
    }
    void fire(nextId++);
  }
}

// ---------------------------------------------------------------------------
// Observers: sentinel events, docker stats, generator CPU
// ---------------------------------------------------------------------------
const SENTINEL_CHANNELS = new Set([
  '+sdown', '-sdown', '+odown', '-odown', '+new-epoch', '+try-failover', '+elected-leader',
  '+failover-state-select-slave', '+selected-slave', '+failover-state-send-slaveof-noone',
  '+failover-state-wait-promotion', '+promoted-slave', '+failover-state-reconf-slaves',
  '+slave-reconf-sent', '+slave-reconf-inprog', '+slave-reconf-done', '+failover-end',
  '+switch-master', '+convert-to-slave', '+reboot', '-failover-abort-not-elected',
  '-failover-abort-no-good-slave', '+failover-detected',
]);

const sentinel = new Redis({
  host: sentinelHost,
  port: Number(sentinelPort),
  lazyConnect: true,
  maxRetriesPerRequest: 1,
  retryStrategy: (times) => Math.min(times * 500, 3000),
});
sentinel.on('error', () => {});
sentinel.on('pmessage', (_pattern: string, channel: string, message: string) => {
  if (!SENTINEL_CHANNELS.has(channel)) return;
  const atSec = nowSec();
  sentinelEvents.push({ atSec, channel, message });
  console.log(`  >> [${atSec.toFixed(2)}s] SENTINEL ${channel} ${message}`);
});

const stripAnsi = (s: string): string => s.replace(/\x1b\[[0-9;]*[A-Za-z]/g, '');
const parseMem = (s: string): number => {
  const m = /([\d.]+)\s*([KMG]i?B|B)/.exec(s);
  if (!m) return 0;
  const k = { B: 1 / 1048576, KiB: 1 / 1024, KB: 1 / 1024, MiB: 1, MB: 1, GiB: 1024, GB: 1024 }[m[2]] ?? 1;
  return Number(m[1]) * k;
};

let dockerStats: ReturnType<typeof Bun.spawn> | null = null;
async function startDockerStats(): Promise<void> {
  try {
    dockerStats = Bun.spawn(['docker', 'stats', '--format', '{{json .}}', ...REDIS_CONTAINERS, APP_CONTAINER], {
      stdout: 'pipe',
      stderr: 'ignore',
    });
    const decoder = new TextDecoder();
    let buf = '';
    for await (const chunk of dockerStats.stdout as ReadableStream<Uint8Array>) {
      buf += decoder.decode(chunk);
      const lines = buf.split('\n');
      buf = lines.pop() ?? '';
      for (const raw of lines) {
        const line = stripAnsi(raw).trim();
        const start = line.indexOf('{');
        if (start < 0) continue;
        try {
          const s = JSON.parse(line.slice(start));
          dockerSamples.push({
            atSec: nowSec(),
            name: s.Name,
            cpuPct: Number.parseFloat(s.CPUPerc) || 0,
            memMiB: parseMem(s.MemUsage ?? ''),
          });
        } catch {
          // partial line
        }
      }
    }
  } catch {
    // docker is optional
  }
}

let lastCpu = process.cpuUsage();
let lastCpuAt = performance.now();
let lastPrinted = 0;
function sampleAndPrint(): void {
  const now = performance.now();
  const cpu = process.cpuUsage(lastCpu);
  const pct = ((cpu.user + cpu.system) / 1000 / (now - lastCpuAt)) * 100;
  lastCpu = process.cpuUsage();
  lastCpuAt = now;
  const sec = Math.floor(nowSec());
  bucket(sec).genCpuPct = pct;

  const prev = sec - 1;
  if (prev < 0 || prev <= lastPrinted - 1) return;
  lastPrinted = sec;
  const b = bucket(prev);
  const p = percentiles(b.latencies);
  const app = latestDocker(APP_CONTAINER);
  console.log(
    `[${String(prev).padStart(4)}s] sent=${b.sent} ok=${b.ok} err=${b.err}` +
      (b.skipped ? ` SKIPPED=${b.skipped}` : '') +
      ` p50=${p.p50.toFixed(0)}ms p99=${p.p99.toFixed(0)}ms max=${p.max.toFixed(0)}ms` +
      ` inflight=${inflight} gen-cpu=${pct.toFixed(0)}%` +
      (app ? ` app-cpu=${app.cpuPct.toFixed(0)}%` : ''),
  );
}

function latestDocker(name: string): { cpuPct: number } | undefined {
  for (let i = dockerSamples.length - 1; i >= 0; i--) {
    if (dockerSamples[i].name === name) return dockerSamples[i];
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Stats helpers
// ---------------------------------------------------------------------------
function percentiles(values: number[]): { p50: number; p90: number; p99: number; max: number } {
  if (values.length === 0) return { p50: 0, p90: 0, p99: 0, max: 0 };
  const s = Float64Array.from(values).sort();
  const at = (q: number): number => s[Math.min(s.length - 1, Math.floor(q * s.length))];
  return { p50: at(0.5), p90: at(0.9), p99: at(0.99), max: s[s.length - 1] };
}

function longestOkGap(): { gapSec: number; fromSec: number; toSec: number } {
  const s = Float64Array.from(okCompletions).sort();
  let best = { gapSec: 0, fromSec: 0, toSec: 0 };
  for (let i = 1; i < s.length; i++) {
    const gap = s[i] - s[i - 1];
    if (gap > best.gapSec) best = { gapSec: gap, fromSec: s[i - 1], toSec: s[i] };
  }
  return best;
}

function dockerSummary(): Record<string, { avgCpuPct: number; maxCpuPct: number; maxMemMiB: number }> {
  const out: Record<string, { avgCpuPct: number; maxCpuPct: number; maxMemMiB: number; n: number }> = {};
  for (const s of dockerSamples) {
    const o = (out[s.name] ??= { avgCpuPct: 0, maxCpuPct: 0, maxMemMiB: 0, n: 0 });
    o.avgCpuPct += s.cpuPct;
    o.n++;
    o.maxCpuPct = Math.max(o.maxCpuPct, s.cpuPct);
    o.maxMemMiB = Math.max(o.maxMemMiB, s.memMiB);
  }
  return Object.fromEntries(
    Object.entries(out).map(([k, v]) => [
      k,
      { avgCpuPct: +(v.avgCpuPct / v.n).toFixed(1), maxCpuPct: v.maxCpuPct, maxMemMiB: +v.maxMemMiB.toFixed(1) },
    ]),
  );
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------
async function fetchStoredIds(): Promise<number[] | null> {
  for (let attempt = 1; attempt <= 30; attempt++) {
    try {
      const res = await fetch(`${baseUrl}/stress/${runId}/keys`, { signal: AbortSignal.timeout(60000) });
      if (res.ok) return ((await res.json()) as { ids: number[] }).ids;
      console.log(`  verify attempt ${attempt}: HTTP ${res.status}`);
    } catch (e) {
      console.log(`  verify attempt ${attempt}: ${(e as Error).message}`);
    }
    await Bun.sleep(2000);
  }
  return null;
}

async function sh(cmd: string[]): Promise<string | null> {
  try {
    const p = Bun.spawn(cmd, { stdout: 'pipe', stderr: 'ignore' });
    const out = await new Response(p.stdout).text();
    return (await p.exited) === 0 ? out.trim() : null;
  } catch {
    return null;
  }
}

async function perNodeCounts(): Promise<Record<string, { role: string | null; keys: number | null }>> {
  const out: Record<string, { role: string | null; keys: number | null }> = {};
  for (const name of REDIS_CONTAINERS) {
    const role = await sh(['docker', 'exec', name, 'redis-cli', 'role']);
    const count = await sh([
      'docker', 'exec', name, 'sh', '-c', `redis-cli --scan --count 1000 --pattern 'stress:${runId}:*' | wc -l`,
    ]);
    out[name] = { role: role?.split('\n')[0] ?? null, keys: count === null ? null : Number(count) };
  }
  return out;
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------
let timer: ReturnType<typeof setInterval>;
let printer: ReturnType<typeof setInterval>;

async function stop(): Promise<void> {
  if (stopping) return;
  stopping = true;
  clearInterval(timer);
  const loadEndSec = nowSec();
  console.log(`\nLoad stopped at ${loadEndSec.toFixed(1)}s. Waiting for ${inflight} in-flight requests...`);
  const drainDeadline = performance.now() + timeoutMs + 5000;
  while (inflight > 0 && performance.now() < drainDeadline) await Bun.sleep(100);
  clearInterval(printer);
  dockerStats?.kill();
  sentinel.disconnect();

  console.log('Waiting 3s for replication, then verifying...');
  await Bun.sleep(3000);
  const storedIds = await fetchStoredIds();
  const nodes = await perNodeCounts();
  const appLog = await sh(['sh', '-c', `docker logs --since ${startedAtIso} ${APP_CONTAINER} 2>&1`]);

  const stored = new Set(storedIds ?? []);
  const failedSet = new Set(failedIds);
  const ackedSet = new Set(ackedIds);
  const lost = storedIds ? ackedIds.filter((id) => !stored.has(id)).sort((a, b) => a - b) : [];
  const writtenDespiteError = storedIds ? failedIds.filter((id) => stored.has(id)) : [];
  const unknown = storedIds ? [...stored].filter((id) => !failedSet.has(id) && !ackedSet.has(id)) : [];

  const lat = percentiles(allLatencies);
  const gap = longestOkGap();
  const totalCompleted = ackedIds.length + failedIds.length;

  const summary = {
    runId,
    label,
    targetRate: rate,
    loadDurationSec: +loadEndSec.toFixed(2),
    sent: nextId,
    skippedByGenerator: skipped,
    achievedSendRate: +(nextId / loadEndSec).toFixed(1),
    completed: totalCompleted,
    acked: ackedIds.length,
    failed: failedIds.length,
    neverCompleted: nextId - totalCompleted,
    latencyMs: Object.fromEntries(Object.entries(lat).map(([k, v]) => [k, +v.toFixed(1)])),
    longestGapWithoutSuccessSec: +gap.gapSec.toFixed(2),
    longestGapWindowSec: [+gap.fromSec.toFixed(2), +gap.toSec.toFixed(2)],
    verification: storedIds
      ? {
          storedKeys: storedIds.length,
          lostAcked: lost.length,
          writtenDespiteError: writtenDespiteError.length,
          storedButNeverSent: unknown.length,
        }
      : 'FAILED: could not read keys back',
  };

  const report = {
    summary,
    config: { rate, durationSec, baseUrl, maxInflight, timeoutMs, connections, startedAt: startedAtIso },
    errorGroups: [...errorGroups.entries()]
      .map(([error, g]) => ({ error, ...g }))
      .sort((a, b) => b.count - a.count),
    sentinelEvents,
    nodes,
    docker: dockerSummary(),
    timeline: buckets.map((b, sec) => {
      const p = percentiles(b.latencies);
      return {
        sec,
        sent: b.sent,
        skipped: b.skipped,
        ok: b.ok,
        err: b.err,
        p50: +p.p50.toFixed(1),
        p99: +p.p99.toFixed(1),
        max: +p.max.toFixed(1),
        maxInflight: b.maxInflight,
        genCpuPct: +b.genCpuPct.toFixed(0),
      };
    }),
    lostIdsSample: lost.slice(0, 1000),
    failedIdsSample: failedIds.slice(0, 1000),
    dockerSamples,
  };

  mkdirSync('reports', { recursive: true });
  const reportPath = `reports/${runId}.json`;
  writeFileSync(reportPath, JSON.stringify(report, null, 2));
  if (appLog !== null) writeFileSync(`reports/${runId}.app.log`, appLog);

  console.log('\n=== SUMMARY ===');
  console.log(JSON.stringify(summary, null, 2));
  if (report.errorGroups.length) {
    console.log('\n=== ERRORS ===');
    for (const g of report.errorGroups.slice(0, 10)) {
      console.log(`  ${g.count}x [${g.firstAtSec.toFixed(1)}s..${g.lastAtSec.toFixed(1)}s] ${g.error}`);
    }
  }
  console.log('\n=== NODES ===');
  console.log(JSON.stringify(nodes));
  console.log('\n=== DOCKER (avg/max CPU %) ===');
  console.log(JSON.stringify(report.docker));
  console.log(`\nReport: ${reportPath}`);
  process.exit(0);
}

async function main(): Promise<void> {
  console.log(`=== HTTP stress: runId=${runId} ===`);
  console.log(
    `target=${rate} req/s duration=${durationSec || 'until Ctrl+C'}s url=${baseUrl}/stress/${runId}/:id max-inflight=${maxInflight} connections=${connections}`,
  );

  try {
    await sentinel.connect();
    await sentinel.psubscribe('*');
    const query = new Redis({ host: sentinelHost, port: Number(sentinelPort), maxRetriesPerRequest: 1 });
    const master = await query.call('SENTINEL', 'get-master-addr-by-name', 'mymaster').catch(() => null);
    query.disconnect();
    console.log(`Sentinel ${sentinelHost}:${sentinelPort} subscribed. Current master: ${JSON.stringify(master)}`);
  } catch (e) {
    console.log(`Sentinel events unavailable: ${(e as Error).message}`);
  }
  void startDockerStats();

  process.on('SIGINT', () => void stop());
  process.on('SIGTERM', () => void stop());
  t0 = performance.now();
  timer = setInterval(tick, 2);
  printer = setInterval(sampleAndPrint, 1000);
}

void main();
