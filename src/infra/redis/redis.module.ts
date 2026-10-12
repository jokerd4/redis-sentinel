import { Global, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { RedisService } from './redis.service';
import { SentinelEventsService } from './sentinel-events.service';

@Global()
@Module({
  imports: [ConfigModule],
  providers: [SentinelEventsService, RedisService],
  exports: [RedisService],
})
export class RedisModule {}
