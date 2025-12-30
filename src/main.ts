import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('Bootstrap');
  logger.log('✅ Process started - bootstrapping Nest app...');

  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: ['log', 'error', 'warn', 'debug', 'verbose'],
  });

  logger.log('✅ Nest application context created');

  // keep the process alive (so the pod doesn't exit)
  setInterval(() => logger.debug('alive'), 60_000);

  process.on('SIGTERM', async () => {
    logger.warn('SIGTERM received, shutting down...');
    await app.close();
    process.exit(0);
  });
  process.on('SIGINT', async () => {
    logger.warn('SIGINT received, shutting down...');
    await app.close();
    process.exit(0);
  });
}

bootstrap().catch((e) => {
  // if bootstrap fails, you MUST print it or logs will look empty in some cases
  // eslint-disable-next-line no-console
  console.error('❌ Fatal bootstrap error:', e);
  process.exit(1);
});
