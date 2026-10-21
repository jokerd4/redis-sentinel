import { ConfigService } from '@nestjs/config';
import { RedisOptions } from 'ioredis';
import { SentinelNode } from './redis.interface';

export class RedisOptionsFactory {
  static getSentinelNodes(config: ConfigService): SentinelNode[] {
    const port = Number(config.get<string>('SENTINEL_PORT', '26379'));
    return [
      { host: config.get<string>('SENTINEL_HOST_1', 'sentinel-1'), port },
      { host: config.get<string>('SENTINEL_HOST_2', 'sentinel-2'), port },
      { host: config.get<string>('SENTINEL_HOST_3', 'sentinel-3'), port },
    ];
  }

  static createClientOptions(config: ConfigService): RedisOptions {
    const masterName = config.get<string>('REDIS_MASTER_NAME', 'mymaster');
    const sentinels = this.getSentinelNodes(config);
    const commandTimeout = this.parsePositiveNumber(config.get<string>('REDIS_COMMAND_TIMEOUT_MS'));
    const connectTimeout = this.parsePositiveNumber(config.get<string>('REDIS_CONNECT_TIMEOUT_MS')) ?? 5000;
    const maxRetries = this.parseMaxRetries(config.get<string>('REDIS_MAX_RETRIES'));

    return {
      name: masterName,
      sentinels,
      role: 'master',
      password: config.get<string>('REDIS_PASSWORD') || undefined,
      sentinelPassword: config.get<string>('REDIS_SENTINEL_PASSWORD') || undefined,
      commandTimeout,
      connectTimeout,
      enableOfflineQueue: true,
      maxRetriesPerRequest: maxRetries,
      autoResendUnfulfilledCommands: true,
      // Drop the connection as soon as any sentinel publishes +switch-master,
      // otherwise the client keeps writing to the demoted master.
      failoverDetector: true,

      sentinelRetryStrategy: (times: number): number => {
        return Math.min(times * 200, 2000);
      },

      // Each attempt asks sentinels for the current master, so retrying fast
      // shortens the pause after a failover without hammering a dead node.
      retryStrategy: (times: number): number => {
        return Math.min(times * 100, 500);
      },

      // 2 = reconnect and resend the failed command instead of rejecting it
      reconnectOnError: (err: Error): 2 | false => {
        const msg = err.message || '';
        const shouldResend = msg.includes('READONLY') || msg.includes('ETIMEDOUT') || msg.includes('ECONNREFUSED');
        return shouldResend ? 2 : false;
      },
    };
  }

  private static parsePositiveNumber(value?: string): number | undefined {
    if (!value) return undefined;
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
  }

  private static parseMaxRetries(value?: string): number | null {
    if (!value || value === 'null') return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
  }
}
