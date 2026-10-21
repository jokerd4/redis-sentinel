import Redis from 'ioredis';

/**
 * Graceful Sentinel switchover: no acknowledged write may be lost.
 *
 * Plain `SENTINEL FAILOVER` promotes a replica while the old master keeps
 * accepting writes until Sentinel demotes it seconds later; those writes are
 * then wiped by resync. This script closes that window:
 *
 *   1. CLIENT PAUSE <ms> WRITE on the master   - writes wait instead of landing
 *   2. wait until replicas reach the master's replication offset
 *   3. SENTINEL FAILOVER <name>
 *   4. wait for +switch-master                 - clients (failoverDetector) drop the old master
 *   5. REPLICAOF <new> + CLIENT KILL on the old master - paused commands are never executed there,
 *                                                       ioredis resends them to the new master
 *   6. CLIENT UNPAUSE
 *
 * Usage:
 *   bun scripts/switchover.ts
 *   bun scripts/switchover.ts --pause-ms=10000 --sync-timeout-ms=5000
 *
 * Redis nodes are reached via `docker exec <container> redis-cli`, sentinel host names
 * are the container names. Sentinels are reached via their published host ports.
 */

const arg = (name: string, fallback: string): string =>
  process.argv.find((a) => a.startsWith(`--${name}=`))?.split('=').slice(1).join('=') ?? fallback;

const masterName = arg('master-name', process.env.REDIS_MASTER_NAME || 'mymaster');
const pauseMs = Number(arg('pause-ms', '10000'));
const syncTimeoutMs = Number(arg('sync-timeout-ms', '5000'));
const switchTimeoutMs = Number(arg('switch-timeout-ms', String(pauseMs - 1000)));
const sentinelHost = arg('sentinel-host', 'localhost');
const sentinelPorts = arg('sentinel-ports', '26379,26380,26381').split(',').map(Number);

const t0 = performance.now();
const log = (msg: string): void => console.log(`[+${((performance.now() - t0) / 1000).toFixed(3)}s] ${msg}`);

class SwitchoverError extends Error {}

async function redisCli(container: string, ...args: string[]): Promise<string> {
  const proc = Bun.spawn(['docker', 'exec', container, 'redis-cli', ...args], { stdout: 'pipe', stderr: 'pipe' });
  const [out, err, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  if (code !== 0) throw new SwitchoverError(`redis-cli ${args.join(' ')} on ${container} failed: ${err.trim()}`);
  const result = out.trim();
  if (result.startsWith('ERR') || result.startsWith('(error)')) {
    throw new SwitchoverError(`redis-cli ${args.join(' ')} on ${container}: ${result}`);
  }
  return result;
}

function parseInfo(info: string): Record<string, string> {
  return Object.fromEntries(
    info
      .split(/\r?\n/)
      .filter((line) => line.includes(':'))
      .map((line) => [line.slice(0, line.indexOf(':')), line.slice(line.indexOf(':') + 1)]),
  );
}

/** Replica entries from INFO replication: slaveN:ip=...,port=...,state=online,offset=...,lag=... */
function parseReplicas(info: Record<string, string>): { id: string; state: string; offset: number }[] {
  return Object.entries(info)
    .filter(([k]) => /^slave\d+$/.test(k))
    .map(([, v]) => {
      const fields = Object.fromEntries(v.split(',').map((kv) => kv.split('=')));
      return { id: `${fields.ip}:${fields.port}`, state: fields.state, offset: Number(fields.offset) };
    });
}

async function waitForReplicaSync(master: string): Promise<void> {
  const deadline = performance.now() + syncTimeoutMs;
  for (;;) {
    const info = parseInfo(await redisCli(master, 'INFO', 'replication'));
    const masterOffset = Number(info.master_repl_offset);
    const replicas = parseReplicas(info).filter((r) => r.state === 'online');
    const behind = replicas.filter((r) => r.offset < masterOffset);
    if (replicas.length > 0 && behind.length === 0) {
      log(`replicas in sync at offset ${masterOffset}: ${replicas.map((r) => r.id).join(', ')}`);
      return;
    }
    if (performance.now() > deadline) {
      throw new SwitchoverError(
        `replicas not in sync after ${syncTimeoutMs}ms (master offset ${masterOffset}, ` +
          `replicas ${JSON.stringify(replicas)})`,
      );
    }
    await Bun.sleep(20);
  }
}

async function main(): Promise<void> {
  const sentinel = new Redis({ host: sentinelHost, port: sentinelPorts[0], maxRetriesPerRequest: 1 });
  const subscribers = sentinelPorts.map(
    (port) => new Redis({ host: sentinelHost, port, maxRetriesPerRequest: 1, lazyConnect: true }),
  );
  let paused: string | null = null;

  try {
    // --- Pre-checks -------------------------------------------------------
    const [oldHost, oldPort] = (await sentinel.call('SENTINEL', 'get-master-addr-by-name', masterName)) as string[];
    log(`current master: ${oldHost}:${oldPort}`);
    log(`quorum: ${await sentinel.call('SENTINEL', 'ckquorum', masterName)}`);
    const role = parseInfo(await redisCli(oldHost, 'INFO', 'replication'));
    if (role.role !== 'master') throw new SwitchoverError(`${oldHost} reports role=${role.role}`);
    if (parseReplicas(role).filter((r) => r.state === 'online').length === 0) {
      throw new SwitchoverError('no online replicas to promote');
    }

    // Subscribe before anything changes so +switch-master cannot be missed.
    const switched = new Promise<[string, string]>((resolve) => {
      for (const sub of subscribers) {
        sub.on('error', () => {});
        sub.on('message', (_channel: string, message: string) => {
          const [name, , , newHost, newPort] = message.split(' ');
          if (name === masterName) resolve([newHost, newPort]);
        });
      }
    });
    await Promise.all(subscribers.map((s) => s.connect().then(() => s.subscribe('+switch-master'))));

    // --- 1. Freeze writes -------------------------------------------------
    await redisCli(oldHost, 'CLIENT', 'PAUSE', String(pauseMs), 'WRITE');
    paused = oldHost;
    const pausedAt = performance.now();
    log(`CLIENT PAUSE ${pauseMs} WRITE on ${oldHost}`);

    // --- 2. Wait until replicas caught up -----------------------------------
    await waitForReplicaSync(oldHost);

    // --- 3. Failover ------------------------------------------------------
    log(`SENTINEL FAILOVER ${masterName}: ${await sentinel.call('SENTINEL', 'FAILOVER', masterName)}`);

    // --- 4. Wait for the new master ---------------------------------------
    const [newHost, newPort] = await Promise.race([
      switched,
      Bun.sleep(switchTimeoutMs).then(() => {
        throw new SwitchoverError(
          `no +switch-master within ${switchTimeoutMs}ms; pause expires at ${pauseMs}ms, check cluster manually`,
        );
      }),
    ]);
    log(`+switch-master: ${oldHost}:${oldPort} -> ${newHost}:${newPort}`);

    // --- 5. Demote the old master right away ---------------------------------
    log(`REPLICAOF on ${oldHost}: ${await redisCli(oldHost, 'REPLICAOF', newHost, newPort)}`);
    log(`CLIENT KILL TYPE normal on ${oldHost}: ${await redisCli(oldHost, 'CLIENT', 'KILL', 'TYPE', 'normal')} killed`);

    // --- 6. Unpause -------------------------------------------------------
    await redisCli(oldHost, 'CLIENT', 'UNPAUSE');
    paused = null;
    log(`CLIENT UNPAUSE on ${oldHost}; writes were paused ${((performance.now() - pausedAt) / 1000).toFixed(3)}s`);

    const now = (await sentinel.call('SENTINEL', 'get-master-addr-by-name', masterName)) as string[];
    const oldRole = (await redisCli(oldHost, 'ROLE')).split('\n')[0];
    log(`done: sentinel master=${now.join(':')}, ${oldHost} role=${oldRole}`);
  } catch (error) {
    if (paused) {
      await redisCli(paused, 'CLIENT', 'UNPAUSE').catch(() => {});
      log(`CLIENT UNPAUSE on ${paused} (rollback)`);
    }
    log(`SWITCHOVER FAILED: ${(error as Error).message}`);
    process.exitCode = 1;
  } finally {
    sentinel.disconnect();
    subscribers.forEach((s) => s.disconnect());
  }
}

void main();
