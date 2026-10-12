import { Controller, Get } from '@nestjs/common';
import { RedisService } from './infra/redis/redis.service';

export interface HealthResponse {
  status: 'ok' | 'degraded';
  master: string;
  timestamp: string;
}

@Controller()
export class AppController {
  constructor(private readonly redisService: RedisService) {}

  @Get('health')
  getHealth(): HealthResponse {
    return {
      status: this.redisService.isReady() ? 'ok' : 'degraded',
      master: this.redisService.getActiveMasterString(),
      timestamp: new Date().toISOString(),
    };
  }
}
