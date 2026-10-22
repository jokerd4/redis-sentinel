import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { RedisOptionsFactory } from './redis-options.factory';
import { ConnectionChangedError, ReplicaAckBatcher } from './replica-ack-batcher';

@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private client: Redis;

  private isConnected = false;
  private downAt: Date | null = null;
  private isShuttingDown = false;
  private endRecoveries = 0;
  private endRecoveryTimer: ReturnType<typeof setTimeout> | null = null;

  // WAIT confirmation of writes, null = off (async replication)
  private replicaAcks: ReplicaAckBatcher | null = null;
  // How long a write may keep retrying after a lost connection
  private retryWindowMs = 15000;
  private nextReady: { promise: Promise<void>; resolve: () => void } | null = null;

  constructor(private readonly configService: ConfigService) {}

  onModuleInit(): void {
    const options = RedisOptionsFactory.createClientOptions(this.configService);
    this.logger.log(`Connecting to Redis Sentinel: group="${options.name}"`);
    this.retryWindowMs = options.commandTimeout || this.retryWindowMs;

    this.client = new Redis(options);
    this.registerClientEvents();

    const waitReplicas = Math.max(0, Number(this.configService.get<string>('REDIS_WAIT_REPLICAS', '0')) || 0);
    const waitTimeoutMs = Math.max(1, Number(this.configService.get<string>('REDIS_WAIT_TIMEOUT_MS', '1000')) || 1000);
    if (waitReplicas > 0) {
      this.replicaAcks = new ReplicaAckBatcher(this.client, waitReplicas, waitTimeoutMs);
      this.logger.log(`Writes wait for ${waitReplicas} replica(s), timeout ${waitTimeoutMs}ms`);
    }
  }

  // The master's name is logged by SentinelEventsService on +switch-master
  private registerClientEvents(): void {
    this.client.on('ready', () => {
      this.isConnected = true;
      if (this.downAt) {
        const downtime = ((Date.now() - this.downAt.getTime()) / 1000).toFixed(2);
        this.logger.warn(`[MASTER UP] Reconnected to master | Total downtime: ${downtime}s`);
        this.downAt = null;
      } else {
        this.logger.log(`[CONNECTED] Connected to master at ${new Date().toISOString()}`);
      }

      this.endRecoveries = 0;
      this.nextReady?.resolve();
      this.nextReady = null;
    });

    this.client.on('close', () => {
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
    this.downAt = new Date();
    this.logger.error(`[MASTER DOWN] ${reason} at ${this.downAt.toISOString()}`);
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
        await this.replicaAcks?.confirm();
        return result;
      } catch (err) {
        const retryable = err instanceof ConnectionChangedError || (err as Error).message === 'Connection is closed.';
        if (!retryable || this.isShuttingDown || !(await this.waitUntilReady(deadline))) throw err;
      }
    }
  }

  // All waiting writes share one promise instead of each adding a 'ready' listener to the client
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

  async onModuleDestroy(): Promise<void> {
    this.logger.log('Closing Redis client...');
    this.isShuttingDown = true;
    if (this.endRecoveryTimer) clearTimeout(this.endRecoveryTimer);
    if (this.client) {
      await this.client.quit().catch(() => this.client.disconnect());
    }
  }
}
