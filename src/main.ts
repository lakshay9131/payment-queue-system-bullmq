import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { join } from 'node:path';
import { PaymentModule } from './payment.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestExpressApplication>(PaymentModule);

  app.enableCors();
  app.useStaticAssets(join(__dirname, '..', 'src', 'public'));

  await app.listen(Number(process.env.PORT ?? 3000));
}

void bootstrap();