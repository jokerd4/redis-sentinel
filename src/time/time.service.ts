import { Injectable, InternalServerErrorException } from '@nestjs/common';
import { RedisService } from '../infra/redis/redis.service';

@Injectable()
export class TimeService {
  private readonly redisKey = 'timestamp';

  constructor(private readonly redisService: RedisService) {}

  async recordAndGetTimestamp(): Promise<{ timestamp: number }> {
    const unixNow = Date.now();
    await this.redisService.set(this.redisKey, unixNow.toString());
    const val = await this.redisService.get(this.redisKey);

    if (!val) {
      throw new InternalServerErrorException('Timestamp was not found in Redis');
    }

    return {
      timestamp: Number(val),
    };
  }
}
