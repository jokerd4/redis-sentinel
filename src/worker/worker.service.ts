import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RedisService } from '../infra/redis/redis.service';
import { BatchExecutionResult } from './worker.interfaces';

@Injectable()
export class WorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WorkerService.name);
  private timerHandle: NodeJS.Timeout | null = null;
  private batchCounter = 0;

  constructor(
    private readonly redisService: RedisService,
    private readonly configService: ConfigService,
  ) {}

  onModuleInit(): void {
    const delayStartMs = 1500;
    this.logger.log(`Starting worker in ${delayStartMs}ms (10 requests/sec: 5 writes + 5 reads + cleanup)...`);

    setTimeout(() => {
      this.startTrafficLoop();
    }, delayStartMs);
  }

  private startTrafficLoop(): void {
    const intervalMs = Number.parseInt(this.configService.get<string>('WORKER_INTERVAL_MS', '1000'), 10) || 1000;

    this.timerHandle = setInterval(async () => {
      await this.executeBatch();
    }, intervalMs);
  }

  private async executeBatch(): Promise<BatchExecutionResult> {
    const batchId = ++this.batchCounter;
    const start = process.hrtime.bigint();
    const activeMaster = this.redisService.getActiveMasterString();

    const writeCount = 5;
    const readCount = 5;

    try {
      // 1. Пять операций записи (TTL 10 секунд на случай сбоя до удаления)
      for (let i = 1; i <= writeCount; i++) {
        const key = `test:item:${i}`;
        const value = `val-${batchId}-${i}-${Date.now()}`;
        await this.redisService.set(key, value, 10);
      }

      // 2. Пять операций чтения
      for (let i = 1; i <= readCount; i++) {
        const key = `test:item:${i}`;
        await this.redisService.get(key);
      }

      // 3. Очистка ключей (удаление, чтобы не засорять память Redis)
      for (let i = 1; i <= writeCount; i++) {
        const key = `test:item:${i}`;
        await this.redisService.del(key);
      }

      const end = process.hrtime.bigint();
      const durationMs = Number(end - start) / 1_000_000;
      const currentMaster = this.redisService.getActiveMasterString();

      this.logger.log(
        `[BATCH #${batchId}] [OK] 5 writes + 5 reads + cleanup | Latency: ${durationMs.toFixed(1)}ms | Master: ${currentMaster}`
      );

      return {
        batchId,
        writesCount: writeCount,
        readsCount: readCount,
        deletesCount: writeCount,
        durationMs,
        master: currentMaster,
        success: true,
      };
    } catch (err: unknown) {
      const end = process.hrtime.bigint();
      const durationMs = Number(end - start) / 1_000_000;
      const errorMsg = err instanceof Error ? err.message : 'Redis error during failover';
      const currentMaster = this.redisService.getActiveMasterString();

      this.logger.error(
        `[BATCH #${batchId}] [FAILOVER / ERROR] ${errorMsg} | Latency: ${durationMs.toFixed(1)}ms | Master: ${currentMaster}`
      );

      return {
        batchId,
        writesCount: writeCount,
        readsCount: readCount,
        deletesCount: writeCount,
        durationMs,
        master: activeMaster,
        success: false,
        error: errorMsg,
      };
    }
  }

  onModuleDestroy(): void {
    if (this.timerHandle) {
      clearInterval(this.timerHandle);
      this.timerHandle = null;
    }
    this.logger.log('Worker stopped.');
  }
}
