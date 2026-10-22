import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { RedisOptionsFactory } from './redis-options.factory';
import { RedisNode } from './redis.interface';
import { SentinelEventsService } from './sentinel-events.service';

interface PendingAck {
  epoch: number;
  resolve: () => void;
  reject: (err: Error) => void;
}

// Thrown when the write and its WAIT ended up on different connections
class ConnectionChangedError extends Error {
  constructor() {
    super('Connection changed before the write was confirmed by replicas');
  }
} 

interface SocketStreamInfo {
  remoteAddress?: string;
  remotePort?: number;
}

@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private client: Redis;

  private currentMaster: RedisNode | null = null;
  private lastKnownMaster: RedisNode | null = null;
  private downAt: Date | null = null;
  private isConnected = false;
  private isShuttingDown = false;
  private endRecoveries = 0;
  private endRecoveryTimer: ReturnType<typeof setTimeout> | null = null;

  // WAIT: a write is acknowledged only after this many replicas confirmed it (0 = off)
  private waitReplicas = 0;
  private waitTimeoutMs = 1000;
  // Incremented on every lost connection: WAIT only covers writes made on the same connection
  private connectionEpoch = 0;
  private pendingAcks: PendingAck[] = [];
  private waitInFlight = false;

  // How long a write may keep retrying after a lost connection
  private retryWindowMs = 15000;
  private nextReady: { promise: Promise<void>; resolve: () => void } | null = null;

  constructor(
    private readonly configService: ConfigService,
    private readonly sentinelEvents: SentinelEventsService,
  ) {}

  onModuleInit(): void {
    const defaultMasterHost = this.configService.get<string>('REDIS_MASTER_HOST', 'redis-master');
    const defaultMasterPort = Number.parseInt(this.configService.get<string>('REDIS_PORT', '6379'), 10) || 6379;
    this.currentMaster = { host: defaultMasterHost, port: defaultMasterPort };
    this.lastKnownMaster = { ...this.currentMaster };

    this.waitReplicas = Math.max(0, Number(this.configService.get<string>('REDIS_WAIT_REPLICAS', '0')) || 0);
    this.waitTimeoutMs = Math.max(1, Number(this.configService.get<string>('REDIS_WAIT_TIMEOUT_MS', '1000')) || 1000);
    if (this.waitReplicas > 0) {
      this.logger.log(`Writes wait for ${this.waitReplicas} replica(s), timeout ${this.waitTimeoutMs}ms`);
    }
    this.retryWindowMs = Number(this.configService.get<string>('REDIS_COMMAND_TIMEOUT_MS')) || this.retryWindowMs;

    const options = RedisOptionsFactory.createClientOptions(this.configService);
    this.logger.log(`Connecting to Redis Sentinel: group="${options.name}"`);

    this.client = new Redis(options);
    this.registerClientEvents();

    this.sentinelEvents.onMasterSwitch((newMaster: RedisNode) => {
      this.currentMaster = newMaster;
    });
  }

  private registerClientEvents(): void {
    this.client.on('ready', () => {
      this.isConnected = true;
      const node = this.currentMaster || this.extractMasterNode();
      const nodeDisplay = `${node.host}:${node.port}`;

      if (this.downAt) {
        const downtime = ((Date.now() - this.downAt.getTime()) / 1000).toFixed(2);
        this.logger.warn(`[MASTER UP] Reconnected to Master: ${nodeDisplay} | Total downtime: ${downtime}s`);
        this.downAt = null;
      } else {
        this.logger.log(`[CONNECTED] Active Master: ${nodeDisplay} | Time: ${new Date().toISOString()}`);
      }

      this.currentMaster = node;
      this.lastKnownMaster = node;
      this.endRecoveries = 0;
      this.nextReady?.resolve();
      this.nextReady = null;
    });

    this.client.on('close', () => {
      this.connectionEpoch++;
      this.handleDisconnect('Connection lost');
    });

    this.client.on('error', (err: Error) => {
      this.handleDisconnect(`Connection error: ${err.message}`);
    });

    // ioredis never reconnects from 'end'. With failoverDetector it can land there on its own:
    // +switch-master from the previous connection's detector aborts an in-flight reconnect,
    // connect() rejects and the client is closed for good. Bring it back unless we are shutting down.
    this.client.on('end', () => {
      this.handleDisconnect('Client ended');
      if (this.isShuttingDown || this.endRecoveryTimer) return;

      const delayMs = Math.min(100 * 2 ** this.endRecoveries, 1000);
      this.endRecoveries++;
      this.logger.error(`[CLIENT END] Redis client closed unexpectedly, reconnecting in ${delayMs}ms`);

      this.endRecoveryTimer = setTimeout(() => {
        this.endRecoveryTimer = null;
        if (this.isShuttingDown || this.client.status !== 'end') return;
        // A failed attempt emits 'end' again and schedules the next one
        this.client.connect().catch((err: Error) => {
          this.logger.error(`[CLIENT END] Reconnect attempt failed: ${err.message}`);
        });
      }, delayMs);
    });
  }

  private handleDisconnect(reason: string): void {
    if (!this.isConnected) return;
    this.isConnected = false;
    this.currentMaster = null;

    if (!this.downAt) {
      this.downAt = new Date();
      const last = this.lastKnownMaster ? `${this.lastKnownMaster.host}:${this.lastKnownMaster.port}` : 'unknown';
      this.logger.error(`[MASTER DOWN] ${reason} to ${last} at ${this.downAt.toISOString()}`);
    }
  }

  private extractMasterNode(): RedisNode {
    try {
      const stream = (this.client as unknown as { stream?: SocketStreamInfo })?.stream;
      if (stream?.remoteAddress) {
        return { host: stream.remoteAddress, port: stream.remotePort || 6379 };
      }
    } catch {
      // fallback
    }
    return this.currentMaster || { host: 'unknown', port: 6379 };
  }

  public getActiveMaster(): RedisNode | null {
    return this.currentMaster;
  }

  public getActiveMasterString(): string {
    if (!this.isConnected || !this.currentMaster) {
      if (this.lastKnownMaster) {
        return `reconnecting... (was ${this.lastKnownMaster.host}:${this.lastKnownMaster.port})`;
      }
      return 'reconnecting...';
    }
    return `${this.currentMaster.host}:${this.currentMaster.port}`;
  }

  public isReady(): boolean {
    return this.isConnected && this.client?.status === 'ready';
  }

  public async set(key: string, value: string, ttlSeconds = 10): Promise<'OK' | null> {
    return this.idempotentWrite(() => this.client.set(key, value, 'EX', ttlSeconds));
  }

  public async setPersistent(key: string, value: string): Promise<'OK' | null> {
    return this.idempotentWrite(() => this.client.set(key, value));
  }

  /**
   * Runs a write (plus WAIT when enabled) and repeats it if the connection was lost under it:
   * - 'Connection is closed.': the client hit 'end' and dropped its queue, the command never reached Redis;
   * - ConnectionChangedError: the write may have landed on the old master but is unconfirmed.
   * Only for idempotent commands: a repeated SET leaves the same value.
   * WAIT shortfall and 'Command timed out' are not retried: no replicas, or the command is still queued.
   */
  private async idempotentWrite<T>(write: () => Promise<T>): Promise<T> {
    const deadline = Date.now() + this.retryWindowMs;
    for (;;) {
      try {
        const result = await write();
        if (this.waitReplicas > 0) await this.confirmByReplicas();
        return result;
      } catch (err) {
        const retryable = err instanceof ConnectionChangedError || (err as Error).message === 'Connection is closed.';
        if (!retryable || this.isShuttingDown || !(await this.waitUntilReady(deadline))) throw err;
      }
    }
  }

  private async waitUntilReady(deadline: number): Promise<boolean> {
    if (this.client.status === 'ready') return true;
    const remaining = deadline - Date.now();
    if (remaining <= 0) return false;

    if (!this.nextReady) {
      let resolve!: () => void;
      const promise = new Promise<void>((r) => (resolve = r));
      this.nextReady = { promise, resolve };
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<boolean>((r) => (timer = setTimeout(() => r(false), remaining)));
    const ready = await Promise.race([this.nextReady.promise.then(() => true), timedOut]);
    clearTimeout(timer);
    return ready;
  }

  /**
   * Resolves only after WAIT confirmed the caller's last write on `waitReplicas` replicas, so an acknowledged
   * write survives a master crash (Sentinel promotes the replica with the highest offset).
   * Must be called right after the write resolved, before the connection could change.
   * WAIT blocks the connection, so concurrent writes share one WAIT instead of queueing behind each other.
   */
  private confirmByReplicas(): Promise<void> {
    const epoch = this.connectionEpoch;
    return new Promise<void>((resolve, reject) => {
      this.pendingAcks.push({ epoch, resolve, reject });
      void this.flushWait();
    });
  }

  private async flushWait(): Promise<void> {
    if (this.waitInFlight || this.pendingAcks.length === 0) return;
    this.waitInFlight = true;
    const batch = this.pendingAcks;
    this.pendingAcks = [];
    const epoch = this.connectionEpoch;

    try {
      const acked = await this.client.wait(this.waitReplicas, this.waitTimeoutMs);
      // Reconnected meanwhile: WAIT ran on a new connection and says nothing about our writes
      const sameConnection = epoch === this.connectionEpoch;
      for (const p of batch) {
        if (!sameConnection || p.epoch !== epoch) {
          p.reject(new ConnectionChangedError());
        } else if (acked < this.waitReplicas) {
          p.reject(new Error(`Write confirmed by ${acked}/${this.waitReplicas} replicas within ${this.waitTimeoutMs}ms`));
        } else {
          p.resolve();
        }
      }
    } catch (err) {
      for (const p of batch) p.reject(err as Error);
    } finally {
      this.waitInFlight = false;
      void this.flushWait();
    }
  }

  public async scanKeys(pattern: string): Promise<string[]> {
    const keys: string[] = [];
    const stream = this.client.scanStream({ match: pattern, count: 1000 });
    for await (const batch of stream) {
      keys.push(...(batch as string[]));
    }
    return keys;
  }

  public async deleteByPattern(pattern: string): Promise<number> {
    const keys = await this.scanKeys(pattern);
    let deleted = 0;
    for (let i = 0; i < keys.length; i += 1000) {
      deleted += await this.client.unlink(...keys.slice(i, i + 1000));
    }
    return deleted;
  }

  public async get(key: string): Promise<string | null> {
    return this.client.get(key);
  }

  public async del(key: string): Promise<number> {
    return this.client.del(key);
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.log('Closing Redis client...');
    this.isShuttingDown = true;
    if (this.endRecoveryTimer) clearTimeout(this.endRecoveryTimer);
    if (this.client) {
      await this.client.quit().catch(() => this.client.disconnect());
    }
  }
}
