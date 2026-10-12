import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const logger = new Logger('Bootstrap');
  const app = await NestFactory.create(AppModule);

  app.enableShutdownHooks();

  const port = process.env.APP_PORT || process.env.PORT || 3000;
  await app.listen(port);

  logger.log(`NestJS Redis Sentinel application is running on port ${port}`);
}

bootstrap();
