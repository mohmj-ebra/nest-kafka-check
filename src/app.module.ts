import { Module } from '@nestjs/common';
import { KafkaHealthService } from './kafka.service';

@Module({
  providers: [KafkaHealthService],
})
export class AppModule {}
