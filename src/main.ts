/* istanbul ignore file */
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { natsStreamConfig } from './utils/config.nats';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {cors: true});
    app.connectMicroservice(natsStreamConfig);
    app.startAllMicroservices();
    await app.listen(3001);
}
bootstrap();