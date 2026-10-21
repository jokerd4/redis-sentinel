import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { RedisOptionsFactory } from './redis-options.factory';
import { RedisNode } from './redis.interface';
import { SentinelEventsService } from './sentinel-events.service';

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

  constructor(
    private readonly configService: ConfigService,
    private readonly sentinelEvents: SentinelEventsService,
  ) {}

  onModuleInit(): void {
    const defaultMasterHost = this.configService.get<string>('REDIS_MASTER_HOST', 'redis-master');
    const defaultMasterPort = Number.parseInt(this.configService.get<string>('REDIS_PORT', '6379'), 10) || 6379;
    this.currentMaster = { host: defaultMasterHost, port: defaultMasterPort };
    this.lastKnownMaster = { ...this.currentMaster };

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
    });

    this.client.on('close', () => {
      this.handleDisconnect('Connection lost');
    });

    this.client.on('error', (err: Error) => {
      this.handleDisconnect(`Connection error: ${err.message}`);
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
    return this.client.set(key, value, 'EX', ttlSeconds);
  }

  public async setPersistent(key: string, value: string): Promise<'OK' | null> {
    return this.client.set(key, value);
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
    if (this.client) {
      await this.client.quit().catch(() => this.client.disconnect());
    }
  }
}
