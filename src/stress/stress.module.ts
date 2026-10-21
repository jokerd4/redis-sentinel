import { Module } from '@nestjs/common';
import { StressController } from './stress.controller';
import { StressService } from './stress.service';

@Module({
  controllers: [StressController],
  providers: [StressService],
})
export class StressModule {}
