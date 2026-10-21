import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { RedisModule } from './infra/redis/redis.module';
import { StressModule } from './stress/stress.module';
import { TimeModule } from './time/time.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    RedisModule,
    TimeModule,
    StressModule,
  ],
})
export class AppModule {}
