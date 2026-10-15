import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const logger = new Logger('Bootstrap');
  const app = await NestFactory.create(AppModule);

  app.enableShutdownHooks();

  const port = Number(process.env.APP_PORT);
  const host = process.env.APP_HOST;
  await app.listen(port, host);

  logger.log(`NestJS Redis Sentinel application is running on ${host}:${port}`);
}

bootstrap();
