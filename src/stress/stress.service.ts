import { Injectable } from '@nestjs/common';
import { RedisService } from '../infra/redis/redis.service';

@Injectable()
export class StressService {
  private readonly keyPrefix = 'stress';

  constructor(private readonly redisService: RedisService) {}

  async write(runId: string, id: number): Promise<{ id: number }> {
    await this.redisService.setPersistent(this.buildKey(runId, id), id.toString());
    return { id };
  }

  async listIds(runId: string): Promise<{ runId: string; count: number; ids: number[] }> {
    const keys = await this.redisService.scanKeys(`${this.keyPrefix}:${runId}:*`);
    const prefixLength = `${this.keyPrefix}:${runId}:`.length;
    const ids = keys.map((key) => Number(key.slice(prefixLength)));
    return { runId, count: ids.length, ids };
  }

  async clear(runId: string): Promise<{ deleted: number }> {
    const pattern = runId === '*' ? `${this.keyPrefix}:*` : `${this.keyPrefix}:${runId}:*`;
    return { deleted: await this.redisService.deleteByPattern(pattern) };
  }

  private buildKey(runId: string, id: number): string {
    return `${this.keyPrefix}:${runId}:${id}`;
  }
}
