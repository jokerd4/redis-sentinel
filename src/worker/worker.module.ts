import { Module } from '@nestjs/common';
import { WorkerService } from './worker.service';

@Module({
  providers: [WorkerService],
  exports: [WorkerService],
})
export class WorkerModule {}
