import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { RedisOptionsFactory } from './redis-options.factory';
import { RedisNode } from './redis.interface';

@Injectable()
export class SentinelEventsService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SentinelEventsService.name);
  private subscriber: Redis | null = null;
  private onMasterSwitchCallback?: (newMaster: RedisNode) => void;

  constructor(private readonly configService: ConfigService) {}

  onModuleInit(): void {
    this.initSubscriber();
  }

  public onMasterSwitch(callback: (newMaster: RedisNode) => void): void {
    this.onMasterSwitchCallback = callback;
  }

  private initSubscriber(): void {
    const sentinels = RedisOptionsFactory.getSentinelNodes(this.configService);
    const primary = sentinels[0];
    const password = this.configService.get<string>('REDIS_SENTINEL_PASSWORD') || undefined;

    try {
      this.subscriber = new Redis({
        host: primary.host,
        port: primary.port,
        password,
        lazyConnect: false,
        retryStrategy: (times: number): number => Math.min(times * 500, 3000),
      });

      this.subscriber.psubscribe('*', (err: Error | null) => {
        if (err) {
          this.logger.warn(`Could not subscribe to Sentinel events on ${primary.host}:${primary.port}: ${err.message}`);
        } else {
          this.logger.log(`Subscribed to Sentinel Pub/Sub events on ${primary.host}:${primary.port}`);
        }
      });

      this.subscriber.on('pmessage', (_pattern: string, channel: string, message: string) => {
        this.handleMessage(channel, message);
      });

      this.subscriber.on('error', (err: Error) => {
        this.logger.debug(`Sentinel subscriber note: ${err.message}`);
      });
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : String(e);
      this.logger.warn(`Failed to initialize Sentinel subscriber: ${message}`);
    }
  }

  private handleMessage(channel: string, message: string): void {
    const now = new Date().toISOString();
    const parts = message.split(' ');

    switch (channel) {
      case '+switch-master': {
        const [, oldHost, oldPort, newHost, newPort] = parts;
        this.logger.warn(`[SENTINEL EVENT: SWITCH MASTER] Master migrated: ${oldHost}:${oldPort} => ${newHost}:${newPort} at ${now}`);
        this.onMasterSwitchCallback?.({
          host: newHost,
          port: Number.parseInt(newPort, 10) || 6379,
        });
        break;
      }
      case '+convert-to-slave': {
        const node = `${parts[2] || parts[1]}:${parts[3] || '6379'}`;
        const master = `${parts[6] || parts[parts.length - 2]}:${parts[7] || '6379'}`;
        this.logger.warn(`[SENTINEL EVENT: CONVERT TO REPLICA] Node ${node} reconfigured as REPLICA of ${master} at ${now}`);
        break;
      }
      case '-sdown': {
        const node = `${parts[2] || parts[1]}:${parts[3] || '6379'}`;
        this.logger.log(`[SENTINEL EVENT: NODE RECOVERED] ${parts[0].toUpperCase()} ${node} came back ONLINE at ${now}`);
        break;
      }
      case '+sdown': {
        const node = `${parts[2] || parts[1]}:${parts[3] || '6379'}`;
        this.logger.warn(`[SENTINEL EVENT: SDOWN] ${parts[0].toUpperCase()} ${node} is UNREACHABLE at ${now}`);
        break;
      }
      case '+odown': {
        const node = `${parts[2] || parts[1]}:${parts[3] || '6379'}`;
        this.logger.error(`[SENTINEL EVENT: ODOWN] Quorum confirmed: Master ${node} is objectively DOWN at ${now}`);
        break;
      }
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.subscriber) {
      await this.subscriber.quit().catch(() => this.subscriber?.disconnect());
    }
  }
}
